mod catalog;
mod storage;

use crate::storage::col_file;
use crate::storage::metadata::{
    ColumnChunkMeta, ColumnDef, LogicalType, SegmentMeta, TableMeta,
};
use serde_json::json;

fn main() {
    println!("Hello, world!");
    if let Err(e) = example_build_users_table() {
        eprintln!("Error: {}", e);
    }
}

fn example_build_users_table() -> Result<(), Box<dyn std::error::Error>> {
    let user_ids: Vec<u32> = vec![100, 101];
    let ages: Vec<u8> = vec![30, 25];
    let is_active: Vec<bool> = vec![true, false];

    std::fs::create_dir_all("db/users/segments/0001")?;
    col_file::write_plan_col::<u32, _>(
        "db/users/segments/0001/data_user_id.bin",
        &user_ids,
    )?;
    col_file::write_plan_col::<u8, _>(
        "db/users/segments/0001/data_age.bin",
        &ages,
    )?;
    col_file::write_plan_col::<bool, _>(
        "db/users/segments/0001/data_is_active.bin",
        &is_active,
    )?;

    let mut table_meta = TableMeta::new(
        "users",
        vec![
            ColumnDef {
                name: "user_id".to_string(),
                logical_type: LogicalType::UInt32,
            },
            ColumnDef {
                name: "age".to_string(),
                logical_type: LogicalType::UInt8,
            },
            ColumnDef {
                name: "is_active".to_string(),
                logical_type: LogicalType::Bool,
            },
        ],
    );

    let seg = SegmentMeta {
        id: "0001".to_string(),
        row_count: user_ids.len() as u64,
        columns: vec![
            ColumnChunkMeta {
                name: "user_id".to_string(),
                file: "segments/0001/data_user_id.bin".to_string(),
                encoding: "plain_le_u32".to_string(),
                min: Some(json!(100)),
                max: Some(json!(101)),
            },
            ColumnChunkMeta {
                name: "age".to_string(),
                file: "segments/0001/data_age.bin".to_string(),
                encoding: "plain_u8".to_string(),
                min: Some(json!(25)),
                max: Some(json!(30)),
            },
            ColumnChunkMeta {
                name: "is_active".to_string(),
                file: "segments/0001/data_is_active.bin".to_string(),
                encoding: "plain_u8_bool".to_string(),
                min: Some(json!(0)),
                max: Some(json!(1)),
            },
        ],
    };

    table_meta.add_segment(seg);

    std::fs::create_dir_all("db/users")?;
    crate::storage::metadata::save_tb_meta("db/users/table_meta.json", &table_meta)
        .map_err(|e| format!("Failed to save table metadata: {:?}", e))?;

    Ok(())
}