use arrow_schema::DataType;

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

pub trait IntoArrowType {
    fn to_arrow_type(&self) -> DataType;
}

impl IntoArrowType for ColumType {
    fn to_arrow_type(&self) -> DataType {
        match self {
            ColumType::Int32 => DataType::Int32,
            ColumType::Int64 => DataType::Int64,
            ColumType::Float64 => DataType::Float64,
            ColumType::Bool => DataType::Boolean,
        }
    }
}
