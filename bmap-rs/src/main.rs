use anyhow::{anyhow, bail, ensure, Context, Result};
use bmap_parser::{AsyncDiscarder, Bmap, Discarder, SeekForward, CopyError};
use clap::{arg, command, Arg, ArgAction, Command};
use futures::TryStreamExt;
use nix::unistd::ftruncate;
use std::ffi::OsStr;
use std::fmt::Write;
use std::fs::File;
use std::io::Read;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use tokio_util::compat::TokioAsyncReadCompatExt;
use thiserror::Error;

use std::io::Cursor;

use gpt::{header, disk, partition};
use gpt::partition::Partition;
use std::collections::BTreeMap;

#[cfg(feature = "remote")]
use reqwest::{Response, Url};

#[cfg(feature = "remote")]
use async_compression::futures::bufread::GzipDecoder;

#[cfg(feature = "progress")]
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

#[cfg(feature = "lz4")]
use lz4::Decoder as Lz4Decoder;

#[cfg(feature = "gz")]
use flate2::read::GzDecoder;

#[cfg(feature = "xz")]
use xz2::read::XzDecoder;

#[cfg(feature = "tar")]
use tar::Archive;

#[derive(Debug, Error)]
pub enum FeatureError {
    #[error("XZ is not supported by this build")]
    XZNotSupported,
    #[error("LZ4 is not supported by this build")]
    LZ4NotSupported,
    #[error("GZ is not supported by this build")]
    GZNotSupported,
    #[error("Tar is not supported by this build")]
    TarNotSupported,
    #[error("Tar detected but no --tar-inner-image given")]
    TarButNoTarInnerImageArg,
}

#[derive(Debug)]
enum Image {
    Path(PathBuf),
    #[cfg(feature = "remote")]
    Url(Url),
}

#[derive(Debug)]
struct Copy {
    image: Image,
    dest: PathBuf,
    nobmap: bool,
}

#[derive(Debug)]
struct CopyPart {
    partnumber: usize,
    image: Image,
    dest: PathBuf,
    tar_inner_image: PathBuf,
    nobmap: bool,
}

#[derive(Debug)]

enum Subcommand {
    Copy(Copy),
    CopyPart(CopyPart)
}

#[derive(Debug)]
struct Opts {
    command: Subcommand,
}

impl Opts {
    fn parser() -> Opts {
        let matches = command!()
            .propagate_version(true)
            .subcommand_required(true)
            .arg_required_else_help(true)
            .subcommand(
                Command::new("copy")
                    .about("Copy image to block device or file")
                    .arg(arg!([IMAGE]).required(true))
                    .arg(arg!([DESTINATION]).required(true))
                    .arg(
                        Arg::new("nobmap")
                            .short('n')
                            .long("nobmap")
                            .action(ArgAction::SetTrue),
                    ),
             )
             .subcommand(
                 Command::new("copy-part")
                    .about("Copy image partition (GPT only) to block device or file")
                    .arg(arg!([PARTNUMBER]).required(true))
                    .arg(arg!([IMAGE]).required(true))
                    .arg(arg!([DESTINATION]).required(true))
                    .arg(
                        Arg::new("nobmap")
                            .short('n')
                            .long("nobmap")
                            .action(ArgAction::SetTrue)
                     )
                    .arg(
                        Arg::new("tar-inner-image")
                           .short('i')
                           .long("tar-inner-image")
                           .default_value("")
                    ),

            )
            .get_matches();
        match matches.subcommand() {
            Some(("copy", sub_matches)) => Opts {
                command: Subcommand::Copy({
                    #[cfg(feature = "remote")]
                    {
                        Copy {
                            image: match Url::parse(sub_matches.get_one::<String>("IMAGE").unwrap()) {
                                Ok(url) => Image::Url(url),
                                Err(_) => Image::Path(PathBuf::from(
                                    sub_matches.get_one::<String>("IMAGE").unwrap(),
                                )),
                            },
                            dest: PathBuf::from(sub_matches.get_one::<String>("DESTINATION").unwrap()),
                            nobmap: sub_matches.get_flag("nobmap"),
                        }
                    }
                    #[cfg(not(feature = "remote"))]
                    {
                        Copy {
                            image: Image::Path(PathBuf::from(
                                sub_matches.get_one::<String>("IMAGE").unwrap(),
                            )),
                            dest: PathBuf::from(sub_matches.get_one::<String>("DESTINATION").unwrap()),
                            nobmap: sub_matches.get_flag("nobmap"),
                        }
                    }
                }),
            },
            Some(("copy-part", sub_matches)) => Opts {
                command: Subcommand::CopyPart({
                    CopyPart {
                        partnumber: sub_matches.get_one::<String>("PARTNUMBER").unwrap().parse::<usize>().unwrap(),
                        image: Image::Path(PathBuf::from(
                                sub_matches.get_one::<String>("IMAGE").unwrap(),
                            )),
                        dest: PathBuf::from(sub_matches.get_one::<String>("DESTINATION").unwrap()),
                        tar_inner_image: PathBuf::from(sub_matches.get_one::<String>("tar-inner-image").unwrap()),
                        nobmap: sub_matches.get_flag("nobmap"),
                    }
                }),
            },
            _ => unreachable!(
                "Exhausted list of subcommands and subcommand_required prevents `None`"
            ),
        }
    }
}

fn append(path: PathBuf) -> PathBuf {
    let mut p = path.into_os_string();
    p.push(".bmap");
    p.into()
}

fn find_bmap(img: &Path) -> Option<PathBuf> {
    let mut bmap = img.to_path_buf();
    loop {
        bmap = append(bmap);
        if bmap.exists() {
            return Some(bmap);
        }

        // Drop .bmap
        bmap.set_extension("");
        bmap.extension()?;
        // Drop existing orignal extension part
        bmap.set_extension("");
    }
}

#[cfg(feature = "remote")]
fn find_remote_bmap(mut url: Url) -> Result<Url> {
    let mut path = PathBuf::from(url.path());
    path.set_extension("bmap");
    url.set_path(path.to_str().unwrap());
    Ok(url)
}

fn get_partitions(input: &mut Decoder) -> BTreeMap<u32, Partition>
{
    let mut v = Vec::new();

    v.resize(4096 * 10, 0); // a LBA is 4096 bytes, partition table is usually at LBA 3, let's load 10 LBAs to be safe
    let mut buf = v.as_mut_slice();

    let r = input
                .read(&mut buf)
                .map_err(CopyError::ReadError);//?;

    /*
    if r == 0 {
        return Err(CopyError::UnexpectedEof);
    }
    */

    let mut c = Cursor::new(buf);

    let lb_size = disk::DEFAULT_SECTOR_SIZE;

    let hdr = header::read_header_from_arbitrary_device(&mut c, lb_size).unwrap();
    //println!("{:#?}", hdr);

    let partitions = partition::file_read_partitions(&mut c, &hdr, lb_size).unwrap();//;.map_err(CopyError::GPTReadError);

    return partitions;
}

trait ReadSeekForward: SeekForward + Read {}
impl<T: Read + SeekForward> ReadSeekForward for T {}

struct Decoder {
    inner: Box<dyn ReadSeekForward>,
}

impl Decoder {
    fn new<T: ReadSeekForward + 'static>(inner: T) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl Read for Decoder {
    fn read(&mut self, data: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(data)
    }
}

impl SeekForward for Decoder {
    fn seek_forward(&mut self, forward: u64) -> std::io::Result<()> {
        self.inner.seek_forward(forward)
    }
}

fn setup_local_input(path: &Path) -> Result<Decoder> {
    let f = File::open(path)?;
    setup_decoder(f, path)
}

fn setup_decoder(file: File, path: &Path) -> Result<Decoder>
{
    match path.extension().and_then(OsStr::to_str) {
        Some("gz") => {
            #[cfg(feature = "gz")]
            {
                let gz = GzDecoder::new(file);
                Ok(Decoder::new(Discarder::new(gz)))
            }

            #[cfg(not(feature = "gz"))]
            {
                Err(FeatureError::GZNotSupported)?
            }
        },
        Some("xz") => {
            #[cfg(feature = "xz")]
            {
                let xz = XzDecoder::new(file);
                Ok(Decoder::new(Discarder::new(xz)))
            }

            #[cfg(not(feature = "xz"))]
            {
                Ok(Decoder::new(file))
            }
        },
        Some("lz4") => {
            #[cfg(feature = "lz4")]
            {
                let lz4 = Lz4Decoder::new(file).unwrap();
                Ok(Decoder::new(Discarder::new(lz4)))
            }

            #[cfg(not(feature = "lz4"))]
            {
                Err(FeatureError::LZ4NotSupported)?
            }
        },
        _ => Ok(Decoder::new(file)),
    }
}

#[cfg(feature = "remote")]
async fn setup_remote_input(url: Url) -> Result<Response> {
    match PathBuf::from(url.path())
        .extension()
        .and_then(OsStr::to_str)
    {
        Some("gz") => reqwest::get(url).await.map_err(anyhow::Error::new),
        None => bail!("No file extension found"),
        _ => bail!("Image file format not implemented"),
    }
}

#[cfg(feature = "progress")]
fn setup_progress_bar(bmap: &Bmap) -> ProgressBar {
    let pb = ProgressBar::new(bmap.total_mapped_size());
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));
    pb
}

#[cfg(feature = "progress")]
fn setup_spinner() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
    pb
}

fn setup_output<T: AsRawFd>(output: &T, bmap: &Bmap, metadata: std::fs::Metadata) -> Result<()> {
    if metadata.is_file() {
        ftruncate(output.as_raw_fd(), bmap.image_size() as i64)
            .context("Failed to truncate file")?;
    }
    Ok(())
}

async fn copy(c: Copy) -> Result<()> {
    if c.nobmap {
        return match c.image {
            Image::Path(path) => copy_local_input_nobmap(path, c.dest),
            #[cfg(feature = "remote")]
            Image::Url(url) => copy_remote_input_nobmap(url, c.dest).await,
        };
    }
    match c.image {
        Image::Path(path) => copy_local_input(path, c.dest),
        #[cfg(feature = "remote")]
        Image::Url(url) => copy_remote_input(url, c.dest).await,
    }
}

async fn copypart(c: CopyPart) -> Result<()> {
    /*
    if c.nobmap {
        return match c.image {
            Image::Path(path) => copy_local_part_nobmap(path, c.dest, c.partnumber),
            Image::Url(url) => copy_remote_part_nobmap(url, c.dest, c.partnumber).await,
        };
    }
    */

    match c.image {
        Image::Path(path) => {
            match path.extension().and_then(OsStr::to_str) {
                Some("tar") => {
                    #[cfg(feature = "tar")]
                    {
                        copy_local_part_from_tar(path, c.tar_inner_image, c.dest, c.partnumber)
                    }
                    #[cfg(not(feature = "tar"))]
                    {
                        Err(FeatureError::TarNotSupported)?
                    }
                },
                _ => copy_local_part(path, c.dest, c.partnumber)
            }
        },
        #[cfg(feature = "remote")]
        Image::Url(url) => copy_remote_part(url, c.dest, c.partnumber).await
    }

}

fn copy_local_input(source: PathBuf, destination: PathBuf) -> Result<()> {
    ensure!(source.exists(), "Image file doesn't exist");
    let bmap = find_bmap(&source).ok_or_else(|| anyhow!("Couldn't find bmap file"))?;
    println!("Found bmap file: {}", bmap.display());

    let mut b = File::open(&bmap).context("Failed to open bmap file")?;
    let mut xml = String::new();
    b.read_to_string(&mut xml)?;

    let bmap = Bmap::from_xml(&xml)?;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(destination)?;

    setup_output(&output, &bmap, output.metadata()?)?;

    let mut input = setup_local_input(&source)?;

    #[cfg(feature = "progress")]
    {
        let pb = setup_progress_bar(&bmap);
        bmap_parser::copy(&mut input, &mut pb.wrap_write(&output), &bmap)?;
        pb.finish_and_clear();
    }

    #[cfg(not(feature = "progress"))]
    {
        bmap_parser::copy(&mut input, &mut output, &bmap)?;
    }

    println!("Done: Syncing...");
    output.sync_all()?;

    Ok(())
}

fn copy_local_part(source: PathBuf, destination: PathBuf, partnumber: usize) -> Result<()> {
    ensure!(source.exists(), "Image file doesn't exist");
    let bmap = find_bmap(&source).ok_or_else(|| anyhow!("Couldn't find bmap file"))?;
    println!("Found bmap file: {}", bmap.display());

    let mut b = File::open(&bmap).context("Failed to open bmap file")?;
    let mut xml = String::new();
    b.read_to_string(&mut xml)?;

    let bmap = Bmap::from_xml(&xml)?;

    let mut src_input = setup_local_input(&source)?;
    let src_parts = get_partitions(&mut src_input);
    drop(src_input);
    let mut dest_input = setup_local_input(&destination)?;
    let dest_parts = get_partitions(&mut dest_input);
    drop(dest_input);

    let partnumber_i = partnumber as u32;


    if (src_parts.len() < partnumber) || (dest_parts.len() < partnumber) {
        return Err(CopyError::PartitionDoesntExistError)?;
    } else if partnumber == 0 {
        return Err(CopyError::PartitionNumberError)?;
    } else if (src_parts[&partnumber_i].last_lba - src_parts[&partnumber_i].first_lba) > (dest_parts[&partnumber_i].last_lba - dest_parts[&partnumber_i].first_lba)  {
        return Err(CopyError::DestinationPartitionTooSmall)?;
    }

    let output = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(destination)?;

    setup_output(&output, &bmap, output.metadata()?)?;

    let mut input = setup_local_input(&source)?;
    let srcpart = src_parts.get(&partnumber_i).unwrap();
    let destpart = dest_parts.get(&partnumber_i).unwrap();

    #[cfg(feature = "progress")]
    {
        let pb = setup_progress_bar(&bmap);
        bmap_parser::copypart(&mut input, &mut pb.wrap_write(&output), &bmap, &srcpart, &destpart)?;
        pb.finish_and_clear();
    }

    #[cfg(not(feature = "progress"))]
    {
        bmap_parser::copypart(&mut input, &mut &output, &bmap, &srcpart, &destpart)?;
    }

    println!("Done: Syncing...");
    output.sync_all()?;

    Ok(())
}

#[cfg(feature = "tar")]
fn copy_local_part_from_tar(source_tar: PathBuf, source: PathBuf, destination: PathBuf, partnumber: usize) -> Result<()> {

    ensure!(source_tar.exists(), "Tar file doesn't exist");

    // look for fixed bmap name for the moment
    let mut bmap_candidate_1 = source.to_path_buf();
    bmap_candidate_1.set_extension("bmap");
    println!("candidate 1 {:?}", bmap_candidate_1);

    // look for bmap
    let bmaparchivefile = File::open(source_tar.clone()).unwrap();
    let mut a = Archive::new(bmaparchivefile);
    let mut xml = String::new();
    for file in a.entries().unwrap() {
        // Make sure there wasn't an I/O error
        let mut file = file.unwrap();

        if file.header().path().unwrap() == bmap_candidate_1 {
            println!("Found bmap file in tar: {}", bmap_candidate_1.display());
            file.read_to_string(&mut xml)?;
            break;
        }

    }
    let bmap = Bmap::from_xml(&xml)?;

    // look for image
    let source_tar_const: PathBuf = source_tar.clone();
    let imgarchivefile: std::fs::File = File::open(source_tar_const).unwrap();
    let ar = Box::leak(Box::new(Archive::new(imgarchivefile)));

    for file in ar.entries().unwrap() {
        // Make sure there wasn't an I/O error
        let file = file.unwrap();

        if file.header().path().unwrap() == source {
            println!("Found image file in tar");

            //let mut src_input = setup_decoder(file., &source);
            let lz4 = Lz4Decoder::new(file).unwrap();
            let mut dec = Decoder::new(Discarder::new(lz4));
            let src_parts = get_partitions(&mut dec);

            let mut dest_input = setup_local_input(&destination)?;
            let dest_parts = get_partitions(&mut dest_input);
            drop(dest_input);

            let partnumber_i = partnumber as u32;


            if (src_parts.len() < partnumber) || (dest_parts.len() < partnumber) {
                return Err(CopyError::PartitionDoesntExistError)?;
            } else if partnumber == 0 {
                return Err(CopyError::PartitionNumberError)?;
            } else if (src_parts[&partnumber_i].last_lba - src_parts[&partnumber_i].first_lba) > (dest_parts[&partnumber_i].last_lba - dest_parts[&partnumber_i].first_lba)  {
                return Err(CopyError::DestinationPartitionTooSmall)?;
            }

            let output = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(destination)?;

            setup_output(&output, &bmap, output.metadata()?)?;

            let source_tar_const2: PathBuf = source_tar.clone();
            let imgarchivefile2: std::fs::File = File::open(source_tar_const2).unwrap();
            let ar2 = Box::leak(Box::new(Archive::new(imgarchivefile2)));

            for file2 in ar2.entries().unwrap() {
                // Make sure there wasn't an I/O error
                let file2 = file2.unwrap();
                if file2.header().path().unwrap() == source {

                    let lz4_2 = Lz4Decoder::new(file2).unwrap();
                    let mut dec2 = Decoder::new(Discarder::new(lz4_2));

                    let srcpart = src_parts.get(&partnumber_i).unwrap();
                    let destpart = dest_parts.get(&partnumber_i).unwrap();

                    #[cfg(feature = "progress")]
                    {
                        let pb = setup_progress_bar(&bmap);
                        bmap_parser::copypart(&mut dec2, &mut pb.wrap_write(&output), &bmap, &srcpart, &destpart)?;
                        pb.finish_and_clear();
                    }

                    #[cfg(not(feature = "progress"))]
                    {
                        bmap_parser::copypart(&mut dec2, &mut &output, &bmap, &srcpart, &destpart)?;
                    }
                }
            }

            //let mut input = setup_local_input(&source)?;

            println!("Done: Syncing...");
            output.sync_all()?;

            return Ok(());
        }
    }

    Err(anyhow!("Could not find image and / or bmap in tar"))
}


#[cfg(feature = "remote")]
async fn copy_remote_input(source: Url, destination: PathBuf) -> Result<()> {
    let bmap_url = find_remote_bmap(source.clone())?;

    let xml = reqwest::get(bmap_url.clone()).await?.text().await?;
    println!("Found bmap file: {}", bmap_url);

    let bmap = Bmap::from_xml(&xml)?;
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(destination)
        .await?;

    setup_output(&output, &bmap, output.metadata().await?)?;

    let res = setup_remote_input(source).await?;
    let stream = res
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read();
    let reader = GzipDecoder::new(stream);
    let mut input = AsyncDiscarder::new(reader);

    #[cfg(feature = "progress")]
    {
        let pb = setup_progress_bar(&bmap);
        bmap_parser::copy_async(
            &mut input,
            &mut pb.wrap_async_write(&mut output).compat(),
            &bmap,
        )
        .await?;
        pb.finish_and_clear();

        println!("Done: Syncing...");
        output.sync_all().await?;
    }

    #[cfg(not(feature = "progress"))]
    {
        bmap_parser::copy_async(
            &mut input,
            &mut output.compat(),
            &bmap,
        )
        .await?;

        println!("Done: Syncing...");
        //output.sync_all().await?; // TODO: does not work
    }

    Ok(())
}

#[cfg(feature = "remote")]
async fn copy_remote_part(source: Url, destination: PathBuf, partnumber: usize) -> Result<()> {
    let bmap_url = find_remote_bmap(source.clone())?;

    let xml = reqwest::get(bmap_url.clone()).await?.text().await?;
    println!("Found bmap file: {}", bmap_url);

    let bmap = Bmap::from_xml(&xml)?;
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(destination)
        .await?;

    setup_output(&output, &bmap, output.metadata().await?)?;

    let res = setup_remote_input(source).await?;
    let stream = res
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read();
    let reader = GzipDecoder::new(stream);
    let mut input = AsyncDiscarder::new(reader);

    #[cfg(feature = "progress")]
    {
        let pb = setup_progress_bar(&bmap);
        bmap_parser::copypart_async(
            &mut input,
            &mut pb.wrap_async_write(&mut output).compat(),
            &bmap,
        )
        .await?;
        pb.finish_and_clear();

        println!("Done: Syncing...");
        output.sync_all().await?;
    }

    #[cfg(not(feature = "progress"))]
    {
        bmap_parser::copypart_async(
            &mut input,
            &mut output.compat(),
            &bmap,
        )
        .await?;

        println!("Done: Syncing...");

        //output.sync_all().await?; // TODO: does not work
    }

    Ok(())
}

fn copy_local_input_nobmap(source: PathBuf, destination: PathBuf) -> Result<()> {
    ensure!(source.exists(), "Image file doesn't exist");

    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(destination)?;

    let mut input = setup_local_input(&source)?;

    #[cfg(feature = "progress")]
    {
        let pb = setup_spinner();
        bmap_parser::copy_nobmap(&mut input, &mut pb.wrap_write(&output))?;
        pb.finish_and_clear();
    }

    #[cfg(not(feature = "progress"))]
    {
        bmap_parser::copy_nobmap(&mut input, &mut output)?;
    }

    println!("Done: Syncing...");
    output.sync_all().expect("Sync failure");

    Ok(())
}

#[cfg(feature = "remote")]
async fn copy_remote_input_nobmap(source: Url, destination: PathBuf) -> Result<()> {
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(destination)
        .await?;

    let res = setup_remote_input(source).await?;
    let stream = res
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read();
    let reader = GzipDecoder::new(stream);
    let mut input = AsyncDiscarder::new(reader);

    #[cfg(feature = "progress")]
    {
        let pb = setup_spinner();
        bmap_parser::copy_async_nobmap(&mut input, &mut pb.wrap_async_write(&mut output).compat())
            .await?;
        pb.finish_and_clear();

        println!("Done: Syncing...");
        output.sync_all().await?;
    }

    #[cfg(not(feature = "progress"))]
    {
        bmap_parser::copy_async_nobmap(&mut input, &mut output.compat())
            .await?;

        println!("Done: Syncing...");
        //output.sync_all().await?; // TODO: does not work
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parser();

    match opts.command {
        Subcommand::Copy(c) => copy(c).await,
        Subcommand::CopyPart(c) => copypart(c).await,
    }
}
