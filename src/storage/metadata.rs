use std::io;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub enum MetadataError {
    Io(io::Error),
    Json(serde_json::Error),
}

impl From<io::Error> for MetadataError {
    fn from(err: io::Error) -> MetadataError {
        MetadataError::Io(err)
    }
}

impl From<serde_json::Error> for MetadataError {
    fn from(err: serde_json::Error) -> MetadataError {
        MetadataError::Json(err)
    }
}

pub type Result<T> = std::result::Result<T, MetadataError>;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "detail")]
pub enum LogicalType {
    UInt8,
    UInt32,
    Bool
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnDef {
  pub name: String,
  pub logical_type: LogicalType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnChunkMeta {
  pub name: String,
  pub file: String,
  pub encoding: String,
  pub min: Option<serde_json::Value>,
  pub max: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SegmentMeta {
  pub id: String,
  pub row_count: u64,
  pub columns: Vec<ColumnChunkMeta>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableMeta {
  pub name: String,
  pub version: u32,
  pub columns: Vec<ColumnDef>,
  pub segments: Vec<SegmentMeta>,
}

impl TableMeta {
    pub fn new(tb_name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        TableMeta {
            name: tb_name.into(),
            version: 1,
            columns,
            segments: vec![],
        }
    }

    pub fn add_segment(&mut self, segment: SegmentMeta) {
        self.segments.push(segment);
    }

    pub fn find_col_def(&self, col_name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == col_name)
    }

    pub fn find_seg(&self, seg_id: &str) -> Option<&SegmentMeta> {
        self.segments.iter().find(|s| s.id == seg_id)
    }
}

pub fn load_tb_meta<P: AsRef<Path>>(path: P) -> Result<TableMeta> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let tb_meta: TableMeta = serde_json::from_reader(reader)?;
    Ok(tb_meta)
}

pub fn save_tb_meta<P: AsRef<Path>>(path: P, tb_meta: &TableMeta) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, tb_meta)?;
    Ok(())
}