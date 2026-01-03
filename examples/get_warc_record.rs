use std::fs::File;
use std::io::{BufReader, Read, Seek};

/// This example seeks warc.gz file to specific offset and the length,
/// decompress it and print decompressed warc record.
/// You can get offset and length by GzipOffsetReader
fn main() {
    let file = File::open("/Users/johannes/Downloads/WIDE-20110225183219005-04371-13730~crawl301.us.archive.org~9443.warc.gz").unwrap();
    let mut reader = BufReader::new(file);
    let offset = 191091750;
    let length = 583;

    reader.seek(std::io::SeekFrom::Start(offset)).unwrap();

    let mut buffer = vec![0; length];
    reader.read_exact(&mut buffer).unwrap();

    let mut decoder = libflate::gzip::Decoder::new(&buffer[..]).unwrap();

    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();

    print!("{}", String::from_utf8_lossy(&decompressed));
}