#[derive(Debug, PartialEq)]
pub enum ColumType {
    Int32,
    Int64,
    Float64,
    Bool,
}

pub trait ColumnTypeCheck: Sized {
    fn get_column_type() -> ColumType;
}

impl ColumnTypeCheck for i32 {
    fn get_column_type() -> ColumType {
        ColumType::Int32
    }
}

impl ColumnTypeCheck for i64 {
    fn get_column_type() -> ColumType {
        ColumType::Int64
    }
}

impl ColumnTypeCheck for f64 {
    fn get_column_type() -> ColumType {
        ColumType::Float64
    }
}

impl ColumnTypeCheck for bool {
    fn get_column_type() -> ColumType {
        ColumType::Bool
    }
}

#[derive(Debug)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: ColumType,
    pub offset: usize,
    pub length: usize,

    // Nullable metadata for bitmask
    pub null_mask_offset: Option<usize>,
    pub null_mask_length: Option<usize>,
}

#[derive(Debug)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub num_rows: usize,
}
