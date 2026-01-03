use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};

#[derive(Debug)]
pub struct GzipMember {
    pub offset: u64,
    pub length: u64,
}

impl GzipMember {
    pub fn new(offset: u64) -> Self {
        Self {
            offset,
            length: 0,
        }
    }
}

pub struct GzipOffsetReader<R> {
    reader: R,
    gzip_header: [u8; 10],
}

impl<R: Read + Seek> GzipOffsetReader<R> {
    pub fn new(mut reader: R) -> std::io::Result<Self> {
        // We are taking first 10 bytes from a gzip file which
        // contains some gzip members (warc.gz file for example),
        // and use this header to find next gzip members.
        let mut gzip_header = [0u8; 10];
        reader.read_exact(&mut gzip_header)?;

        // Validate if this is really a gzip header
        //if gzip_header[0] == 0x1f && gzip_header[1] == 0x8b {
        if gzip_header[0..2] == [0x1f, 0x8b] {
            reader.seek(SeekFrom::Start(0))?;
            Ok(Self { reader, gzip_header })
        } else {
            Err(std::io::ErrorKind::InvalidData)?
        }
    }

    pub fn iter_members(self) -> GzipMemberIterator<R> {
        GzipMemberIterator::new(self.reader, self.gzip_header)
    }
    
    pub fn get_members(self) -> Vec<GzipMember> {
        Vec::from_iter(self.iter_members())
    }
}

pub struct GzipMemberIterator<R> {
    reader: R,
    gzip_header: [u8; 10],
    positions: VecDeque<u64>,
    previous_block: Option<GzipMember>,
    last_member_entered: bool,
}

impl<R: Read + Seek> GzipMemberIterator<R> {
    pub fn new(reader: R, gzip_header: [u8; 10]) -> Self {
        Self {
            reader,
            gzip_header,
            positions: VecDeque::new(),
            previous_block: None,
            last_member_entered: false,
        }
    }

    fn find_members(&mut self) -> std::io::Result<Option<bool>> {
        if self.last_member_entered {
            return Ok(None);
        }

        let mut buffer = [0u8; 4096];
        let pos = self.reader.seek(SeekFrom::Current(0))?;

        self.positions.clear();

        let bytes_read = self.reader.read(&mut buffer)?;
        if bytes_read == 0 {
            self.positions.push_back(pos);
            self.last_member_entered = true;
        }

        for (i, &byte) in buffer[..bytes_read].iter().enumerate() {
            if byte == self.gzip_header[0] {
                if i + self.gzip_header.len() <= bytes_read {
                    if buffer[i..i + self.gzip_header.len()] == self.gzip_header {
                        self.positions.push_back(pos + i as u64);
                    }
                } else {
                    //println!("Needed bytes count {}", needed_bytes_count);
                    let first_bytes = buffer[i..].to_vec();

                    if first_bytes != self.gzip_header[..first_bytes.len()] {
                        continue;
                    }

                    let needed_bytes_count = self.gzip_header.len() - first_bytes.len();
                    assert!(needed_bytes_count < self.gzip_header.len());
                    let mut next_bytes = vec![0u8; needed_bytes_count];
                    self.reader.read_exact(&mut next_bytes)?;


                    if next_bytes == self.gzip_header[first_bytes.len()..] {
                        self.positions.push_back(pos + i as u64);
                    } else {
                        self.reader.seek(SeekFrom::Current(0 - needed_bytes_count as i64))?;
                    }
                    break;
                }
            }
        }

        Ok(Some(!self.positions.is_empty()))
    }
}

impl<R: Read + Seek> Iterator for GzipMemberIterator<R> {
    type Item = GzipMember;

    fn next(&mut self) -> Option<Self::Item> {
        if self.positions.is_empty() {
            loop {
                match self.find_members() {
                    Ok(Some(anything)) => if anything { break; },
                    _ => return None,
                }
            }
        }

        // We are at first record, prepare block for next length
        if let None = self.previous_block
                && let Some(offset) = self.positions.pop_front() {
            self.previous_block = Some(GzipMember::new(offset));
        }

        // Get prepared block to set length, and return it
        if let Some(offset) = self.positions.pop_front()
                && let Some(mut block) = self.previous_block.take() {
            block.length = offset - block.offset;
            self.previous_block = Some(GzipMember::new(offset));

            return Some(block);
        }

        None
    }
}