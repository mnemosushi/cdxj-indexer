use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Seek, Write};
use std::ops::Index;
use crate::gz::GzipMember;
use serde::Serialize;
use url::Url;
use warc::{RecordType, WarcHeader};
use warc::WarcReader;
use crate::IndexFormat;

pub struct Warc {
}


#[derive(Serialize)]
struct CdxData {
    url: String,
    digest: String,
    redirect: String,
    mime: String,
    offset: u64,
    length: u64,
    status: u32,
    file_name: String,
}

impl CdxData {
    pub fn new(file_name: String) -> Self {
        Self {
            file_name,
            status: 0,
            length: 0,
            url: String::from("-"),
            offset: 0,
            mime: String::from("-"),
            digest: String::from("-"),
            redirect: String::from("-"),
        }
    }

    pub fn to_cdxj(&self) -> Option<String> {
        match serde_json::to_string(self) {
            Ok(json) => Some(json),
            Err(_) => None,
        }
    }

    pub fn to_cdx11(&self) -> Option<String> {
        // CDX N b a m s k r M S V g
        // N: massaged url
        // b: date
        // a: original url
        // m: mime type
        // s: response code
        // k: new style checksum (sha)
        // r: redirect
        // M: meta tags (AIF)
        // S: compressed record size
        // V: compressed file offset
        // g: file name
        Some(format!("{} {} {} {} {} - {} {} {}", self.url, self.mime, self.status,
                     self.digest, self.redirect, self.length, self.offset, self.file_name))
    }
}

pub struct CdxRecord {
    searchable_url: String,
    timestamp: String,
    data: CdxData,
}

pub trait CdxRecordFormatter {
    fn write<T: Write>(writer: &mut T, record: &CdxRecord) -> std::io::Result<()>;
}

impl CdxRecord {
    pub fn new(file_name: String) -> Self {
        Self {
            searchable_url: "".to_string(),
            timestamp: "".to_string(),
            data: CdxData::new(file_name)
        }
    }
}

pub struct Cdx11Writer {}
pub struct CdxjWriter {}

impl CdxRecordFormatter for CdxjWriter {
    fn write<T: Write>(writer: &mut T, record: &CdxRecord) -> std::io::Result<()> {
        write!(writer, "{} {} {}\n", record.searchable_url, record.timestamp,
            record.data.to_cdxj().ok_or(std::io::ErrorKind::InvalidData)?)?;
        Ok(())
    }
}

impl CdxRecordFormatter for Cdx11Writer {
    fn write<T: Write>(writer: &mut T, record: &CdxRecord) -> std::io::Result<()> {
        write!(writer, "{} {} {}\n", record.searchable_url, record.timestamp,
               record.data.to_cdx11().ok_or(std::io::ErrorKind::InvalidData)?)?;
        Ok(())
    }
}

impl Warc {
    pub fn index(input: &str, output: &str, format: IndexFormat, gzip_members: Vec<GzipMember>) -> Result<(), std::io::Error> {
        let file = WarcReader::from_path_gzip(&input)?;
        let output = File::create(output)?;
        let mut writer = BufWriter::new(output);

        // We are adding space before header, to make sure sort does not move this header position
        if let IndexFormat::CDX = format {
            writer.write(b" CDX N b a m s k r M S V g\n")?;
        }

        let mut count = 0;
        for record in file.iter_records() {
            let member = gzip_members.index(count);
            count += 1;
            match record {
                Err(err) => println!("ERROR: {}\r\n", err),
                Ok(record) => {
                    if record.warc_type() != &RecordType::Response {
                        // Skip non-response warc records
                        continue;
                    }

                    if let Some(content_type) = record.header(WarcHeader::ContentType) {
                        if content_type != "application/http; msgtype=response" {
                            // Skip non-http response
                            continue;
                        }
                    }

                    let mut cdx_record = CdxRecord::new(input.to_string());
                    cdx_record.timestamp = record.date().format("%Y%m%d%H%M%S").to_string();

                    if let Some(target_uri) = record.header(WarcHeader::TargetURI)
                            && let Ok(uri) = Url::parse(&target_uri) {
                        let mut domain = uri
                            .host_str()
                            .unwrap()
                            .split('.')
                            .rev()
                            .collect::<Vec<&str>>()
                            .join(",");
                        if let Some(stripped) = domain.strip_suffix(",www") {
                            domain = stripped.to_owned();
                        }
                        let mut path = uri.path().to_lowercase();
                        if path.len() > 1 && path.ends_with('/') {
                            let removed = path.remove(path.len() - 1);
                            if removed != '/' {
                                panic!("What?!");
                            }
                        }

                        if let Some(query) = uri.query() {
                            let query_string = if query.contains('&') {
                                let query_to_sort = query.to_lowercase();
                                let mut sorted: Vec<&str> = query_to_sort
                                    .split('&')
                                    .collect();

                                sorted.sort();
                                sorted.join("&").to_lowercase()
                            } else {
                                query.to_lowercase()
                            };
                            cdx_record.searchable_url = format!("{domain}){path}?{}", query_string);
                        } else {
                            cdx_record.searchable_url = format!("{domain}){path}");
                        }

                        cdx_record.data.url = uri.to_string();
                    } else {
                        eprintln!("URI is invalid, skipping!");
                        continue;
                    }

                    cdx_record.data.length = member.length;
                    cdx_record.data.offset = member.offset;

                    if let Some(digest) = record.header(WarcHeader::PayloadDigest) {
                        cdx_record.data.digest = digest
                            .strip_prefix("sha1:")
                            .unwrap_or(digest.as_ref())
                            .to_string();
                    }

                    // Extract HTTP headers from body
                    let mut first_line = String::new();
                    let mut body = BufReader::new(record.body());

                    // HTTP Status
                    if let Ok(_) = body.read_line(&mut first_line) {
                        let mut http_status = first_line.split_whitespace();
                        if http_status.next().is_some() &&
                            let Some(status) = http_status.next() &&
                            let Ok(status) = status.parse::<u32>() {
                            cdx_record.data.status = status;
                        }
                    }

                    for line in body.lines().into_iter() {
                        if let Ok(line) = line {
                            if line == "" {
                                break;
                            }

                            if let Some((key, value)) = line.split_once(':') {
                                //println!("{key}: {value}");
                                match key.to_lowercase().as_str() {
                                    "content-type" => {
                                        cdx_record.data.mime = value
                                            .split_once(';')
                                            .unwrap_or((value, ""))
                                            .0
                                            .trim()
                                            .to_owned();
                                    },
                                    "location" =>
                                        cdx_record.data.redirect = value.trim().to_owned(),
                                    _ => (),
                                }
                            }
                        }
                    }

                    // Write record
                    match format {
                        IndexFormat::CDX => Cdx11Writer::write(writer.by_ref(), &cdx_record)?,
                        IndexFormat::CDXJ => CdxjWriter::write(writer.by_ref(), &cdx_record)?,
                    }
                    // if let Some(cdxj) = cdx_record.to_cdxj() {
                    //     writer.write(cdxj.as_bytes())?;
                    // }
                }
            }
        }

        Ok(())
    }
}