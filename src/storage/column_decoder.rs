use std::{error::Error, fmt::format, fs::File, io, mem, path::Path, str::EncodeUtf16};

use memmap2::Mmap;

use crate::catalog::{ColumType, ColumnMetadata, ColumnTypeCheck, TableMetadata};

fn is_bit_set(mask_bytes: &[u8], idx: usize) -> bool {
    let byte_idx = idx / 8;
    let bit_offset = idx % 8;

    if byte_idx > mask_bytes.len() {
        return false;
    }

    let byte = mask_bytes[byte_idx];

    (byte & (1 << bit_offset)) != 0
}

#[derive(Debug)]
pub struct ColumnDecoder {
    pub table: TableMetadata,
    data_map: Mmap,
}

impl ColumnDecoder {
    pub fn open<P: AsRef<Path>>(path: P, table: TableMetadata) -> io::Result<Self> {
        let file = File::open(path)?;

        let data_map = unsafe { Mmap::map(&file)? };

        Ok(ColumnDecoder { table, data_map })
    }

    fn get_mask_bytes(&self, col_meta: &ColumnMetadata) -> Option<&[u8]> {
        let start = col_meta.null_mask_offset?;
        let length = col_meta.null_mask_length?;
        let end = start + length;
        if end > self.data_map.len() {
            return None;
        }

        Some(&self.data_map[start..end])
    }

    pub fn get_nullable_column_as_vec<T: ColumnTypeCheck + Clone>(
        &self,
        col_name: &str,
    ) -> Result<Vec<Option<T>>, String> {
        let data_slice = self.get_column_as_slice::<T>(col_name)?;

        let col_meta = self
            .table
            .columns
            .iter()
            .find(|c| c.name == col_name)
            .unwrap();

        let mask_bytes = self
            .get_mask_bytes(col_meta)
            .ok_or(format!("Column {} do not have null mask", col_name))?;

        let mut result_vec = Vec::with_capacity(data_slice.len());

        for (idx, item) in data_slice.iter().enumerate() {
            if is_bit_set(mask_bytes, idx) {
                result_vec.push(Some(item.clone()));
            } else {
                result_vec.push(None);
            }
        }
        Ok(result_vec)
    }

    pub fn get_column_bytes(&self, column_name: &str) -> Option<&[u8]> {
        let col_meta = self.table.columns.iter().find(|c| c.name == column_name)?;

        let start = col_meta.offset;
        let end = col_meta.offset + col_meta.length;

        if end > self.data_map.len() {
            return None;
        }

        Some(&self.data_map[start..end])
    }

    fn bytes_to_typed_slice<T: ColumnTypeCheck>(&self, bytes: &[u8]) -> Option<&[T]> {
        let el_size = mem::size_of::<T>();

        if bytes.len() & el_size != 0 {
            eprintln!(
                "Error bytes size ({}) if not correct with bytes size ({}) type",
                bytes.len(),
                el_size
            );
            return None;
        }

        let len = bytes.len() / el_size;
        let ptr = bytes.as_ptr();
        let typed_ptr = ptr as *const T;

        unsafe { Some(std::slice::from_raw_parts(typed_ptr, len)) }
    }

    pub fn get_column_as_slice<T: ColumnTypeCheck>(&self, col_name: &str) -> Result<&[T], String> {
        let col_meta = self
            .table
            .columns
            .iter()
            .find(|c| c.name == col_name)
            .ok_or(format!("Not found column: {}", col_name))?;

        if col_meta.data_type != T::get_column_type() {
            return Err(format!(
                "Column {} require type {:?} but type is {:?}",
                col_name,
                T::get_column_type(),
                col_meta.data_type,
            ));
        }

        let bytes = self
            .get_column_bytes(col_name)
            .ok_or(format!("Error when get byte column: {}", col_name))?;

        self.bytes_to_typed_slice::<T>(bytes)
            .ok_or(format!("Error parse data for column: {}", col_name))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumType, ColumnMetadata};

    use super::*;

    #[test]
    fn test_01() {}

    #[test]
    fn test_02() {}
}
