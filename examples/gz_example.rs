use cdxj_indexer::gz::GzipOffsetReader;
use std::fs::File;
use std::io::BufReader;

/// This example read warc.gz file to get gz members,
/// prints offset and length of each warc record
fn main() -> std::io::Result<()> {
    let file = File::open(
        "/Users/johannes/Downloads/WIDE-20110225183219005-04371-13730~crawl301.us.archive.org~9443.warc.gz",
    )?;
    let reader = BufReader::new(file);
    let iterator = GzipOffsetReader::new(reader);

    for member in iterator?.iter_members() {
        println!("{} {}", member.offset, member.length);
    }
    Ok(())
}
