mod bmap;
pub use crate::bmap::*;
mod discarder;
pub use crate::discarder::*;
use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use futures::TryFutureExt;
use sha2::{Digest, Sha256};
use thiserror::Error;

use std::io::Cursor;
use std::io::Result as IOResult;
use std::io::{Read, Seek, SeekFrom, Write};

use gpt::{header, disk, partition};
use gpt::partition::Partition;
use std::collections::BTreeMap;

/// Trait that can only seek further forwards
pub trait SeekForward {
    fn seek_forward(&mut self, offset: u64) -> IOResult<()>;
}

impl<T: Seek> SeekForward for T {
    fn seek_forward(&mut self, forward: u64) -> IOResult<()> {
        self.seek(SeekFrom::Current(forward as i64))?;
        Ok(())
    }
}

#[async_trait(?Send)]
pub trait AsyncSeekForward {
    async fn async_seek_forward(&mut self, offset: u64) -> IOResult<()>;
}

#[async_trait(?Send)]
impl<T: AsyncSeek + Unpin + Send> AsyncSeekForward for T {
    async fn async_seek_forward(&mut self, forward: u64) -> IOResult<()> {
        self.seek(SeekFrom::Current(forward as i64)).await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CopyError {
    #[error("Failed to Read: {0}")]
    ReadError(std::io::Error),
    #[error("Failed to Write: {0}")]
    WriteError(std::io::Error),
    #[error("Checksum error")]
    ChecksumError,
    #[error("Unexpected EOF on input")]
    UnexpectedEof,
    #[error("Failed to read GPT partition table")]
    GPTReadError,
    #[error("GPT partition number is invalid")]
    PartitionNumberError,
    #[error("GPT partition number does not exist")]
    PartitionDoesntExistError,
    #[error("Destination partition is smaller than source partition")]
    DestinationPartitionTooSmall,
}

pub fn copy<I, O>(input: &mut I, output: &mut O, map: &Bmap) -> Result<(), CopyError>
where
    I: Read + SeekForward,
    O: Write + SeekForward,
{
    let mut hasher = match map.checksum_type() {
        HashType::Sha256 => Sha256::new(),
    };

    let mut v = Vec::new();
    // TODO benchmark a reasonable size for this
    v.resize(8 * 1024 * 1024, 0);

    let buf = v.as_mut_slice();
    let mut position = 0;
    for range in map.block_map() {
        let forward = range.offset() - position;
        input.seek_forward(forward).map_err(CopyError::ReadError)?;
        output
            .seek_forward(forward)
            .map_err(CopyError::WriteError)?;

        let mut left = range.length() as usize;
        while left > 0 {
            let toread = left.min(buf.len());
            let r = input
                .read(&mut buf[0..toread])
                .map_err(CopyError::ReadError)?;
            if r == 0 {
                return Err(CopyError::UnexpectedEof);
            }
            hasher.update(&buf[0..r]);
            output
                .write_all(&buf[0..r])
                .map_err(CopyError::WriteError)?;
            left -= r;
        }
        let digest = hasher.finalize_reset();
        if range.checksum().as_slice() != digest.as_slice() {
            return Err(CopyError::ChecksumError);
        }

        position = range.offset() + range.length();
    }

    Ok(())
}

pub async fn copy_async<I, O>(input: &mut I, output: &mut O, map: &Bmap) -> Result<(), CopyError>
where
    I: AsyncRead + AsyncSeekForward + Unpin,
    O: AsyncWrite + AsyncSeekForward + Unpin,
{
    let mut hasher = match map.checksum_type() {
        HashType::Sha256 => Sha256::new(),
    };
    let mut v = Vec::new();
    // TODO benchmark a reasonable size for this
    v.resize(8 * 1024 * 1024, 0);

    let buf = v.as_mut_slice();
    let mut position = 0;
    for range in map.block_map() {
        let forward = range.offset() - position;
        input
            .async_seek_forward(forward)
            .map_err(CopyError::ReadError)
            .await?;
        output.flush().map_err(CopyError::WriteError).await?;
        output
            .async_seek_forward(forward)
            .map_err(CopyError::WriteError)
            .await?;

        let mut left = range.length() as usize;
        while left > 0 {
            let toread = left.min(buf.len());
            let r = input
                .read(&mut buf[0..toread])
                .map_err(CopyError::ReadError)
                .await?;
            if r == 0 {
                return Err(CopyError::UnexpectedEof);
            }
            hasher.update(&buf[0..r]);
            output
                .write_all(&buf[0..r])
                .await
                .map_err(CopyError::WriteError)?;
            left -= r;
        }
        let digest = hasher.finalize_reset();
        if range.checksum().as_slice() != digest.as_slice() {
            return Err(CopyError::ChecksumError);
        }

        position = range.offset() + range.length();
    }
    Ok(())
}

pub fn copy_nobmap<I, O>(input: &mut I, output: &mut O) -> Result<(), CopyError>
where
    I: Read,
    O: Write,
{
    std::io::copy(input, output).map_err(CopyError::WriteError)?;
    Ok(())
}

pub async fn copy_async_nobmap<I, O>(input: &mut I, output: &mut O) -> Result<(), CopyError>
where
    I: AsyncRead + AsyncSeekForward + Unpin,
    O: AsyncWrite + AsyncSeekForward + Unpin,
{
    futures::io::copy(input, output)
        .map_err(CopyError::WriteError)
        .await?;
    Ok(())
}

pub fn copypart<I, O>(input: &mut I, output: &mut O, map: &Bmap, srcpart: &Partition, destpart: &Partition) -> Result<(), CopyError>
where
    I: Read + SeekForward,
    O: Read + Write + SeekForward,
{

    if (srcpart.last_lba - srcpart.first_lba) > (destpart.last_lba - destpart.first_lba)  {
        return Err(CopyError::DestinationPartitionTooSmall);
    }

    let sector_size = u64::from(gpt::disk::DEFAULT_SECTOR_SIZE);

    let low_byte = (srcpart.first_lba) * sector_size;
    let high_byte = (srcpart.last_lba) * sector_size;
    let byte_offset = (destpart.first_lba - srcpart.first_lba) * sector_size;

    let part_range = low_byte..high_byte;

    /*
    println!("low byte: {:#?}", part_range.start);
    println!("high byte: {:#?}", part_range.end);
    println!("byte offset: {:#?}", byte_offset);
    println!("sector size: {:#?}", sector_size);
    */

    let mut hasher = match map.checksum_type() {
        HashType::Sha256 => Sha256::new(),
    };

    let mut v = Vec::new();
    // TODO benchmark a reasonable size for this
    v.resize(8 * 1024 * 1024, 0);


    let buf = v.as_mut_slice();
    let mut position = 0;
    for range in map.block_map() {
        let source_range = range.offset()..(range.offset() + range.length());

        if (source_range.start < part_range.start) && (source_range.end < part_range.start) {
            //println!("Block out of range!! Start: {:#?} End: {:#?}", source_range.start, source_range.end);
            continue;
        }

        if source_range.start > part_range.end {
            //println!("Block out of range!! Start: {:#?} End: {:#?}", source_range.start, source_range.end);
            continue;
        }


        if (source_range.start >= part_range.start) && (source_range.end <= part_range.end) {
            //println!("Block fully inside destination partition. Start: {:#?} End: {:#?}", source_range.start, source_range.end);
        }

        let mut forward = range.offset() - position;
        let mut left_delta = 0;
        if part_range.start > source_range.start {
            println!("Range start earlier than partition. Shifting copy start to +{:#?} ({:#?}). Start: {:#?} End: {:#?}", part_range.start - source_range.start, part_range.start, range.offset(), range.offset() + range.length());
            forward = part_range.start - position;
            left_delta += part_range.start - source_range.start;
        }

        if part_range.end < source_range.end {
            println!("Range finish later than partition. Shifting copy end to -{:#?} ({:#?}). Start: {:#?} End: {:#?}", source_range.end - part_range.end, part_range.end, range.offset(), range.offset() + range.length());
            left_delta += source_range.end - part_range.end;
        }

        let mut left = (range.length() - left_delta) as usize;

        input.seek_forward(forward).map_err(CopyError::ReadError)?;
        output
            .seek_forward(forward + byte_offset)
            .map_err(CopyError::WriteError)?;

        while left > 0 {
            let toread = left.min(buf.len());
            let r = input
                .read(&mut buf[0..toread])
                .map_err(CopyError::ReadError)?;
            if r == 0 {
                return Err(CopyError::UnexpectedEof);
            }
            hasher.update(&buf[0..r]);
            output
                .write_all(&buf[0..r])
                .map_err(CopyError::WriteError)?;
            left -= r;
        }
        let digest = hasher.finalize_reset();
        if range.checksum().as_slice() != digest.as_slice() {
            return Err(CopyError::ChecksumError);
        }

        position = range.offset() + range.length();
    }

    Ok(())
}

pub async fn copypart_async<I, O>(input: &mut I, output: &mut O, map: &Bmap) -> Result<(), CopyError>
where
    I: AsyncRead + AsyncSeekForward + Unpin,
    O: AsyncWrite + AsyncSeekForward + Unpin,
{
    let mut hasher = match map.checksum_type() {
        HashType::Sha256 => Sha256::new(),
    };
    let mut v = Vec::new();
    // TODO benchmark a reasonable size for this
    v.resize(8 * 1024 * 1024, 0);

    let buf = v.as_mut_slice();
    let mut position = 0;
    for range in map.block_map() {
        let forward = range.offset() - position;
        input
            .async_seek_forward(forward)
            .map_err(CopyError::ReadError)
            .await?;
        output.flush().map_err(CopyError::WriteError).await?;
        output
            .async_seek_forward(forward)
            .map_err(CopyError::WriteError)
            .await?;

        let mut left = range.length() as usize;
        while left > 0 {
            let toread = left.min(buf.len());
            let r = input
                .read(&mut buf[0..toread])
                .map_err(CopyError::ReadError)
                .await?;
            if r == 0 {
                return Err(CopyError::UnexpectedEof);
            }
            hasher.update(&buf[0..r]);
            output
                .write_all(&buf[0..r])
                .await
                .map_err(CopyError::WriteError)?;
            left -= r;
        }
        let digest = hasher.finalize_reset();
        if range.checksum().as_slice() != digest.as_slice() {
            return Err(CopyError::ChecksumError);
        }

        position = range.offset() + range.length();
    }
    Ok(())
}
