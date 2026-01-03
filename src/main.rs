mod warc;
mod gz;

use std::fs::File;
use std::io::BufReader;
use clap::{Parser,ValueEnum};
use gz::GzipOffsetReader;
use crate::warc::Warc;

#[derive(Debug, Clone, ValueEnum)]
pub enum IndexFormat {
    CDX,
    CDXJ,
}

#[derive(Parser, Debug)]
#[clap(version = "1.0", about = "Index WARC file into CDXJ format")]
struct Options {
    #[arg(short, value_name="WARC FILE", help="Gzipped warc file as input file")]
    input: String,
    #[arg(short, value_name="CDXJ FILE", help="Write to CDXJ file")]
    output: String,
    #[arg(short, value_enum, default_value="cdxj")]
    format: IndexFormat,
    #[arg(short,long)]
    verbose: bool,
}

fn main() -> Result<(), std::io::Error> {
    let args = Options::parse();

    if args.verbose {
        println!("Use warc file: {}", args.input);
        println!("Gain gzip members...");
    }

    let file = File::open(&args.input)?;
    let reader = GzipOffsetReader::new(BufReader::new(file));
    let members = reader?.get_members();

    if args.verbose {
        println!("Ready, now create {:?} index file from warc... this may take a while... please wait...", args.format);
    }

    Warc::index(&args.input, &args.output, args.format, members)?;

    if args.verbose {
        println!("Done");
    }

    Ok(())
}
