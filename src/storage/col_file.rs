use std::{fs::File, io::{Read, Write}};

pub trait LeBytesCodec: Sized {
    fn byte_width() -> usize;
    fn encode_le(value: &Self) -> Vec<u8>;
    fn decode_le(bytes: &[u8]) -> Self;
}

impl LeBytesCodec for u8 {
    fn byte_width() -> usize {
        1
    }

    fn encode_le(value: &Self) -> Vec<u8> {
        vec![*value]
    }

    fn decode_le(bytes: &[u8]) -> Self {
        bytes[0]
    }
}

impl LeBytesCodec for u32 {
    fn byte_width() -> usize {
        4
    }

    fn encode_le(value: &Self) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn decode_le(bytes: &[u8]) -> Self {
        let mut array = [0u8; 4];
        array.copy_from_slice(&bytes[0..4]);
        u32::from_le_bytes(array)
    }
}

impl LeBytesCodec for bool {
    fn byte_width() -> usize {
        1
    }

    fn encode_le(value: &Self) -> Vec<u8> {
        vec![if *value { 1 } else { 0 }]
    }

    fn decode_le(bytes: &[u8]) -> Self {
        bytes[0] != 0
    }
}

pub fn write_plan_col<T, P>(path: P, data: &[T]) -> std::io::Result<()>
where
    T: LeBytesCodec,
    P: AsRef<std::path::Path>,
{
   let file = File::create(path)?;
    let mut writer = std::io::BufWriter::new(file);

    let bw = T::byte_width();
    let mut buf = vec![0u8; data.len() * bw];
    for (i, item) in data.iter().enumerate() {
        let encoded = T::encode_le(item);
        let start = i * bw;
        let end = start + bw;
        buf[start..end].copy_from_slice(&encoded);
    }

    writer.write_all(&buf)?;
    writer.flush()?;
    Ok(())
}

pub fn append_plan_col<T, P>(path: P, data: &[T]) -> std::io::Result<()>
where
    T: LeBytesCodec,
    P: AsRef<std::path::Path>,
{
    let file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)?;
    let mut writer = std::io::BufWriter::new(file);

    let bw = T::byte_width();
    let mut buf = vec![0u8; data.len() * bw];
    for (i, item) in data.iter().enumerate() {
        let encoded = T::encode_le(item);
        let start = i * bw;
        let end = start + bw;
        buf[start..end].copy_from_slice(&encoded);
    }

    writer.write_all(&buf)?;
    writer.flush()?;
    Ok(())
}

pub fn read_plan_col_all<T, P>(path: P) -> std::io::Result<Vec<T>>
where
    T: LeBytesCodec,
    P: AsRef<std::path::Path>,
{
    let file = File::open(path)?;
    let mut reader = std::io::BufReader::new(file);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;

    let bw = T::byte_width();
    let item_count = buf.len() / bw;
    let mut result = Vec::with_capacity(item_count);

    for i in 0..item_count {
        let start = i * bw;
        let end = start + bw;
        let item = T::decode_le(&buf[start..end]);
        result.push(item);
    }

    Ok(result)   
}

pub fn read_plain_column_with_rows<T, R>(reader: &mut R, rows: usize) -> std::io::Result<Vec<T>>
where
    T: LeBytesCodec,
    R: Read,
{
    let bw = T::byte_width();
    let mut buf = vec![0u8; rows * bw];
    reader.read_exact(&mut buf)?;

    let mut out = Vec::with_capacity(rows);
    for chunk in buf.chunks_exact(bw) {
        out.push(T::decode_le(chunk));
    }
    Ok(out)
}