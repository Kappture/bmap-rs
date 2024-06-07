mod bmap;
pub use crate::bmap::*;
mod discarder;
pub use crate::discarder::*;
use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use futures::TryFutureExt;
use sha2::{Digest, Sha256};
use thiserror::Error;
use crc::{Crc};

use std::io::Result as IOResult;
use std::io::{Read, Seek, SeekFrom, Write};

use gpt::partition::Partition;
use std::collections::VecDeque;
use std::ops::Range;

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
    #[error("Error during GPT table copy")]
    GPTCopyError,

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
            //println!("Range start earlier than partition. Shifting copy start to +{:#?} ({:#?}). Start: {:#?} End: {:#?}", part_range.start - source_range.start, part_range.start, range.offset(), range.offset() + range.length());
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

pub fn copygpt<I, O>(input: &mut I, output: &mut O, map: &Bmap, src_hdr: &gpt::header::Header) -> Result<(), CopyError>
where
    I: Read + SeekForward,
    O: Read + Write + SeekForward,
{
    let sector_size = u64::from(gpt::disk::DEFAULT_SECTOR_SIZE);

    // main read buffer
    let mut v = Vec::new();
    v.resize(8 * 1024 * 1024, 0);
    let buf = v.as_mut_slice();

    // intermediary data holder (to be able to reconstitute backup gpt header)
    let mut mbr_v: Vec<u8> = Vec::new();
    let mut header_v: Vec<u8> = Vec::new();
    let mut parts_v: Vec<u8> = Vec::new();

    // holder for ranges to copy
    let mut ranges: VecDeque<(&mut Vec<u8>, Range<u64>)> = VecDeque::new();

    // copy protective MBR LBA
    ranges.push_back((&mut mbr_v, 0..(1*sector_size)));

    // copy gpt header
    ranges.push_back((&mut header_v, (src_hdr.current_lba*sector_size)..((src_hdr.current_lba+1)*sector_size)));

    // copy gpt partitions table (_usable + 1) LBA to backup header LBA (inclusive, so +1)
    ranges.push_back((&mut parts_v, ((src_hdr.part_start)*sector_size)..((src_hdr.first_usable)*sector_size)));

    //println!("Ranges to copy: {:#?}", ranges);

    let mut position = 0;
    for range in map.block_map() {
        let source_range = range.offset()..(range.offset() + range.length());

        //println!("New source range {:#?}", source_range);

        // move to start of source block
        let mut forward = source_range.start - position;
        if forward > 0 {
            input.seek_forward(forward).map_err(CopyError::ReadError)?;
            output
                .seek_forward(forward)
                .map_err(CopyError::WriteError)?;
            position += forward;
            //println!("Moving forward to {:#?} (+{:#?})", position, forward);
        }

        if ranges.len() == 0 {
            //println!("No more GPT blocks, we're done");
            break;
        }

        let mut pair = ranges.pop_front().unwrap();
        let mut r = pair.1;

        loop {
            //println!("ranges {:#?}", ranges);
            //println!("{:#?}..{:#?} ->  {:#?}", source_range.start, source_range.end, r);

            // this one should never happen as blocks are in order
            if source_range.start > r.end {
                //println!("Source block went past GPT block!!");
                return Err(CopyError::GPTCopyError);
            }

            if (source_range.start < r.start) && (source_range.end < r.start) {
                //println!("Block out of range!!");
                ranges.push_front((pair.0, r));
                break; // next source block
            }

            /*
            if (source_range.start >= r.start) && (source_range.end <= r.end) {
                println!("Block fully inside destination partition.");
            }
            */

            let mut left_delta = 0;
            if r.start > position {
                //println!("Position is before GPT block. Shifting copy start to +{:#?} ({:#?}).", r.start - position, r.start);
                forward = r.start - position;
                left_delta += r.start - source_range.start;
            }

            if r.end < source_range.end {
                //println!("Block finish later than GPT block. Shifting copy end to -{:#?} ({:#?}).", source_range.end - r.end, r.end);
                left_delta += source_range.end - r.end;
            }

            let mut left = ((source_range.end - position) - left_delta) as usize;

            input.seek_forward(forward).map_err(CopyError::ReadError)?;
            output
                .seek_forward(forward)
                .map_err(CopyError::WriteError)?;
            position += forward;

            while left > 0 {
                let toread = left.min(buf.len());
                let r = input
                    .read(&mut buf[0..toread])
                    .map_err(CopyError::ReadError)?;
                if r == 0 {
                    return Err(CopyError::UnexpectedEof);
                }
                output
                    .write_all(&buf[0..r])
                    .map_err(CopyError::WriteError)?;
                left -= r;

                // also store for later
                pair.0.extend_from_slice(&buf[0..r]);

                position += r as u64;
            }

            if r.end > source_range.end {
                //println!("GPT block finish later than block. Inserting new GPT block at beginning");
                ranges.push_front((pair.0, (source_range.end)..(r.end)));
                break;
            }

            if source_range.end > r.end {
                if ranges.len() == 0 {
                    //println!("No more GPT blocks, we're done");
                    break;
                }

                //println!("Data left in source block, move to next GPT block");
                pair = ranges.pop_front().unwrap();
                r = pair.1;
                continue;
            }

            break;
        }
    }

    /*
     * Starting from this point, GPT main header + parts have been copied, now we need to
     * reconstruct backup gpt parts + header
     */

    //println!("mbr: {:#?}", mbr_v.len());
    //println!("header: {:#?}", header_v.len());
    //println!("parts: {:#?}", parts_v.len());

    // check that our gpt data pointers are correct
    if (src_hdr.last_usable + 1 + parts_v.len() as u64 / sector_size) != src_hdr.backup_lba {
        return Err(CopyError::GPTCopyError);
    }

    // move to start of backup parts
    let forward = (src_hdr.last_usable + 1) * sector_size - position;
    if forward > 0 {
        output
            .seek_forward(forward)
            .map_err(CopyError::WriteError)?;
        position += forward;
        //println!("Moving forward to {:#?} (+{:#?})", position, forward);
    }
    
    // write parts, those do not change from main parts
    output
        .write_all(&parts_v)
        .map_err(CopyError::WriteError)?;

    /* 
     * For backup header, we need to:
     *     - Invert main header current lba and backup lba
     *     - Point parts pointer to backup parts
     *     - Reconstruct its crc32 because of the above changes
     */
    let mut backup_hdr = src_hdr.clone();
    backup_hdr.current_lba = src_hdr.backup_lba;
    backup_hdr.backup_lba = src_hdr.current_lba;
    backup_hdr.part_start = src_hdr.last_usable + 1;

    let intermediate_bytes = gptheader_as_bytes(&backup_hdr, false).unwrap();
    backup_hdr.crc32 = calculate_crc32(&intermediate_bytes);

    let mut finalbytes = gptheader_as_bytes(&backup_hdr, true).unwrap();
    finalbytes.resize(header_v.len(), 0); // make it the same size

    // finally write to disk
    output
        .write_all(&finalbytes)
        .map_err(CopyError::WriteError)?;

    Ok(())
}

fn gptheader_as_bytes(hdr: &gpt::header::Header, use_crc32: bool
        ) -> Result<Vec<u8>, std::io::Error> {
    let mut buff: Vec<u8> = Vec::new();
    let disk_guid_fields = hdr.disk_guid.as_fields();

    std::io::Write::write_all(&mut buff, hdr.signature.as_bytes())?;
    std::io::Write::write_all(&mut buff, &hdr.revision.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &hdr.header_size_le.to_le_bytes())?;

    if use_crc32 {
        std::io::Write::write_all(&mut buff, &hdr.crc32.to_le_bytes())?;
    } else {
        std::io::Write::write_all(&mut buff, &[0_u8; 4])?;
    }

    std::io::Write::write_all(&mut buff, &[0; 4])?;
    std::io::Write::write_all(&mut buff, &hdr.current_lba.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &hdr.backup_lba.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &hdr.first_usable.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &hdr.last_usable.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &disk_guid_fields.0.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &disk_guid_fields.1.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &disk_guid_fields.2.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, disk_guid_fields.3)?;
    std::io::Write::write_all(&mut buff, &hdr.part_start.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &hdr.num_parts.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &hdr.part_size.to_le_bytes())?;
    std::io::Write::write_all(&mut buff, &hdr.crc32_parts.to_le_bytes())?;
    Ok(buff)
}

const CRC_32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
fn calculate_crc32(b: &[u8]) -> u32 {
    let mut digest = CRC_32.digest();
    digest.update(b);
    digest.finalize()
}
