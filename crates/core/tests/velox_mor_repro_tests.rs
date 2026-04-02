/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Unit tests for MOR file group reader behavior.
//!
//! These tests exercise the FileGroupReader against a real MOR table with
//! base parquet files and AVRO log files.  They validate column projection,
//! merge correctness, and the interaction between OutputColumns config and
//! the reader.
//!
//! Test data layout (created by `dev/test-hudi-mor.sh`):
//!   /home/ubuntu/ws2/test-data/hudi_mor_partitioned/
//!     .hoodie/hoodie.properties   — table config
//!     city=sf/   — 1 base parquet (2 rows) + 1 log file (1 upsert)
//!     city=nyc/  — same pattern
//!     city=chi/  — same pattern
//!     city=la/   — same pattern
//!
//! Parquet schema (10 columns):
//!   0: _hoodie_commit_time    (meta)
//!   1: _hoodie_commit_seqno   (meta)
//!   2: _hoodie_record_key     (meta)
//!   3: _hoodie_partition_path (meta)
//!   4: _hoodie_file_name      (meta)
//!   5: id                     (data - Int32)
//!   6: name                   (data - Utf8)
//!   7: age                    (data - Int32)
//!   8: ts                     (data - Utf8)
//!   9: city                   (data - Utf8)
//!
//! Commit timeline:
//!   20260327190925114 → INSERT 8 rows (2 per partition) → base parquet files
//!   20260327190932786 → UPSERT 4 rows (1 per partition) → log files

use arrow_array::{Array, Int32Array, StringArray};
use hudi_core::error::Result;
use hudi_core::file_group::FileGroup;
use hudi_core::file_group::reader::FileGroupReader;

const BASE_URI: &str = "file:///home/ubuntu/ws2/test-data/hudi_mor_partitioned";

/// One partition's MOR split metadata.
struct PartitionCase {
    partition_path: &'static str,
    base_file_name: &'static str,
    log_file_names: &'static [&'static str],
    expected_rows: usize,
    /// (id, expected_name, expected_age) for the record updated by Commit 2.
    updated_record: (i32, &'static str, i32),
    /// (id, expected_name, expected_age) for the record NOT updated.
    unchanged_record: (i32, &'static str, i32),
}

const PARTITIONS: &[PartitionCase] = &[
    PartitionCase {
        partition_path: "city=sf",
        base_file_name: "a50cb7ba-a9eb-46bd-8613-5a1d4914ac4a-0_0-21-40_20260327190925114.parquet",
        log_file_names: &[".a50cb7ba-a9eb-46bd-8613-5a1d4914ac4a-0_20260327190932786.log.1_0-42-87"],
        expected_rows: 2,
        updated_record: (1, "Alice-V2", 31),
        unchanged_record: (2, "Bob", 25),
    },
    PartitionCase {
        partition_path: "city=nyc",
        base_file_name: "5910dc3f-242b-4530-af06-d99a0db44803-0_3-21-43_20260327190925114.parquet",
        log_file_names: &[".5910dc3f-242b-4530-af06-d99a0db44803-0_20260327190932786.log.1_3-42-90"],
        expected_rows: 2,
        updated_record: (3, "Carol-V2", 36),
        unchanged_record: (4, "Dave", 28),
    },
    PartitionCase {
        partition_path: "city=chi",
        base_file_name: "da10ee68-1cd5-4662-a5ad-be0b68c2ebb0-0_2-21-42_20260327190925114.parquet",
        log_file_names: &[".da10ee68-1cd5-4662-a5ad-be0b68c2ebb0-0_20260327190932786.log.1_2-42-89"],
        expected_rows: 2,
        updated_record: (5, "Eve-V2", 33),
        unchanged_record: (6, "Frank", 40),
    },
    PartitionCase {
        partition_path: "city=la",
        base_file_name: "30701f2b-85b7-4a28-a065-25ed3ed3e03e-0_1-21-41_20260327190925114.parquet",
        log_file_names: &[".30701f2b-85b7-4a28-a065-25ed3ed3e03e-0_20260327190932786.log.1_1-42-88"],
        expected_rows: 2,
        updated_record: (7, "Grace-V2", 28),
        unchanged_record: (8, "Hank", 45),
    },
];

/// Helper: look up a column by name in a RecordBatch.
fn col_by_name<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> &'a arrow_array::ArrayRef {
    let idx = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column '{name}' not found in schema: {:?}", batch.schema()));
    batch.column(idx)
}

/// Helper: read one partition using the given FileGroupReader.
async fn read_partition(reader: &FileGroupReader, case: &PartitionCase) -> Result<arrow_array::RecordBatch> {
    let mut file_group =
        FileGroup::new_with_base_file_name(case.base_file_name, case.partition_path)?;
    file_group.add_log_files_from_names(case.log_file_names)?;
    let (_commit_ts, file_slice) = file_group
        .file_slices
        .iter()
        .next()
        .expect("FileGroup must have exactly one FileSlice");
    reader.read_file_slice(file_slice).await
}

/// Validates that today's FileGroupReader, when reading a MOR file slice
/// (base parquet + log file), returns ALL 10 columns from the parquet file.
///
/// This documents the current behavior: no column projection is applied,
/// even though only a subset of columns may be needed for the query.
#[tokio::test]
async fn test_mor_read_returns_all_parquet_columns() -> Result<()> {
    // env_logger not available as dev dep; RUST_LOG still works via log crate.

    // Create reader with NO output columns config — default behavior.
    let reader = FileGroupReader::new_with_options(BASE_URI, Vec::<(&str, &str)>::new()).await?;

    let case = &PARTITIONS[0]; // city=sf
    let batch = read_partition(&reader, case).await?;

    // The parquet file has 10 columns (5 meta + 5 data).
    // Current behavior: ALL 10 columns are returned even though a query
    // might only need 2-3 of them.
    let schema = batch.schema();
    let col_names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    eprintln!(
        "test_mor_read_returns_all_parquet_columns: got {} cols: {:?}",
        col_names.len(),
        col_names
    );

    assert_eq!(
        batch.num_columns(),
        10,
        "Expected all 10 parquet columns, got {} — schema: {:?}",
        batch.num_columns(),
        batch.schema()
    );

    // Verify MOR merge is correct: 2 rows, updated record has V2 values.
    assert_eq!(batch.num_rows(), case.expected_rows);
    let id_arr = col_by_name(&batch, "id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let name_arr = col_by_name(&batch, "name")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let age_arr = col_by_name(&batch, "age")
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let (upd_id, upd_name, upd_age) = case.updated_record;
    let upd_row = (0..batch.num_rows())
        .find(|&r| id_arr.value(r) == upd_id)
        .expect("Updated record not found");
    assert_eq!(name_arr.value(upd_row), upd_name);
    assert_eq!(age_arr.value(upd_row), upd_age);

    let (unc_id, unc_name, unc_age) = case.unchanged_record;
    let unc_row = (0..batch.num_rows())
        .find(|&r| id_arr.value(r) == unc_id)
        .expect("Unchanged record not found");
    assert_eq!(name_arr.value(unc_row), unc_name);
    assert_eq!(age_arr.value(unc_row), unc_age);

    Ok(())
}

/// Validates that FileGroupReader applies column projection when
/// `hoodie.read.output.columns` is set.
///
/// Requesting `id,name` should return those 2 columns plus merge-required
/// fields: `_hoodie_commit_time`, `_hoodie_record_key`, `_hoodie_commit_seqno`, `ts`
/// (ts is the ordering/precombine field for this table).
/// Total: 6 columns instead of 10.
#[tokio::test]
async fn test_mor_read_applies_column_projection() -> Result<()> {
    // env_logger not available as dev dep; RUST_LOG still works via log crate.

    let reader = FileGroupReader::new_with_options(
        BASE_URI,
        [("hoodie.read.output.columns", "id,name")],
    )
    .await?;

    let case = &PARTITIONS[0]; // city=sf
    let batch = read_partition(&reader, case).await?;

    let schema = batch.schema();
    let col_names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    eprintln!(
        "test_mor_read_applies_column_projection: requested id,name → got {} cols: {:?}",
        col_names.len(),
        col_names
    );

    // With column projection: 2 requested (id, name) + 4 merge-required
    // (_hoodie_commit_time, _hoodie_record_key, _hoodie_commit_seqno, ts).
    let expected_cols = [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "id",
        "name",
        "ts",
    ];
    assert_eq!(
        batch.num_columns(),
        expected_cols.len(),
        "Expected {} projected cols, got {} — schema: {:?}",
        expected_cols.len(),
        batch.num_columns(),
        batch.schema()
    );
    for expected in &expected_cols {
        assert!(
            col_names.contains(expected),
            "Column '{}' missing from projected schema: {:?}",
            expected,
            col_names,
        );
    }

    // MOR merge should still be correct with projected columns.
    assert_eq!(batch.num_rows(), case.expected_rows);

    // Verify merge correctness: updated record has V2 values.
    let id_arr = col_by_name(&batch, "id")
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let name_arr = col_by_name(&batch, "name")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let (upd_id, upd_name, _) = case.updated_record;
    let upd_row = (0..batch.num_rows())
        .find(|&r| id_arr.value(r) == upd_id)
        .expect("Updated record not found");
    assert_eq!(name_arr.value(upd_row), upd_name);

    let (unc_id, unc_name, _) = case.unchanged_record;
    let unc_row = (0..batch.num_rows())
        .find(|&r| id_arr.value(r) == unc_id)
        .expect("Unchanged record not found");
    assert_eq!(name_arr.value(unc_row), unc_name);

    Ok(())
}
