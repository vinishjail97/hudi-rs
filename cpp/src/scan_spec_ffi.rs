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

//! Phase 1 ScanSpec FFI roundtrip.
//!
//! Mirrors the JSON layout produced by
//! `velox/connectors/hive/hudi/ScanSpecJson.{h,cpp}`. Parses the JSON,
//! validates the schema version, and prints a deterministic
//! `[SCANSPEC-FFI]` block so the C++ test wrapper can grep-assert it.
//!
//! Phase 2 will reuse the very same `ScanSpecJson`/`FilterJson` types
//! from inside `new_file_group_reader_with_context`; do not redefine
//! them in another module.

use serde::Deserialize;

pub const SCAN_SPEC_JSON_VERSION: i32 = 1;

/// Top-level wire shape: a versioned envelope around a `ScanSpecJson` tree.
/// Only the root carries `v`; nested specs are bare `ScanSpecJson`s.
#[derive(Debug, Deserialize)]
pub struct ScanSpecRootJson {
    pub v: i32,
    #[serde(flatten)]
    pub spec: ScanSpecJson,
}

#[derive(Debug, Deserialize)]
pub struct ScanSpecJson {
    pub name: String,
    pub subscript: i64,
    pub channel: i64,
    #[serde(rename = "projectOut")]
    pub project_out: bool,
    #[serde(rename = "filterDisabled")]
    pub filter_disabled: bool,
    #[serde(rename = "columnType")]
    pub column_type: String,
    pub filter: Option<FilterJson>,
    pub children: Vec<ScanSpecJson>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum FilterJson {
    AlwaysTrue {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
    },
    AlwaysFalse {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
    },
    IsNull {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
    },
    IsNotNull {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
    },
    BoolValue {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        value: bool,
    },
    BigintRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        lower: i64,
        upper: i64,
    },
    NegatedBigintRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        lower: i64,
        upper: i64,
    },
    BigintValuesUsingHashTable {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        min: i64,
        max: i64,
        values: Vec<i64>,
    },
    BigintValuesUsingBitmask {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        min: i64,
        max: i64,
        values: Vec<i64>,
    },
    NegatedBigintValuesUsingHashTable {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        min: i64,
        max: i64,
        values: Vec<i64>,
    },
    NegatedBigintValuesUsingBitmask {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        min: i64,
        max: i64,
        values: Vec<i64>,
    },
    DoubleRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "lowerDouble")]
        lower: f64,
        #[serde(rename = "upperDouble")]
        upper: f64,
        #[serde(rename = "lowerUnbounded")]
        lower_unbounded: bool,
        #[serde(rename = "upperUnbounded")]
        upper_unbounded: bool,
        #[serde(rename = "lowerExclusive")]
        lower_exclusive: bool,
        #[serde(rename = "upperExclusive")]
        upper_exclusive: bool,
    },
    FloatRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "lowerFloat")]
        lower: f64,
        #[serde(rename = "upperFloat")]
        upper: f64,
        #[serde(rename = "lowerUnbounded")]
        lower_unbounded: bool,
        #[serde(rename = "upperUnbounded")]
        upper_unbounded: bool,
        #[serde(rename = "lowerExclusive")]
        lower_exclusive: bool,
        #[serde(rename = "upperExclusive")]
        upper_exclusive: bool,
    },
    BytesRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "lowerBytes")]
        lower: String,
        #[serde(rename = "upperBytes")]
        upper: String,
        #[serde(rename = "lowerUnbounded")]
        lower_unbounded: bool,
        #[serde(rename = "upperUnbounded")]
        upper_unbounded: bool,
        #[serde(rename = "lowerExclusive")]
        lower_exclusive: bool,
        #[serde(rename = "upperExclusive")]
        upper_exclusive: bool,
    },
    NegatedBytesRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "lowerBytes")]
        lower: String,
        #[serde(rename = "upperBytes")]
        upper: String,
        #[serde(rename = "lowerUnbounded")]
        lower_unbounded: bool,
        #[serde(rename = "upperUnbounded")]
        upper_unbounded: bool,
        #[serde(rename = "lowerExclusive")]
        lower_exclusive: bool,
        #[serde(rename = "upperExclusive")]
        upper_exclusive: bool,
    },
    BytesValues {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "stringValues")]
        values: Vec<String>,
    },
    NegatedBytesValues {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "stringValues")]
        values: Vec<String>,
    },
    HugeintRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "lowerHi")]
        lower_hi: i64,
        #[serde(rename = "lowerLo")]
        lower_lo: i64,
        #[serde(rename = "upperHi")]
        upper_hi: i64,
        #[serde(rename = "upperLo")]
        upper_lo: i64,
    },
    HugeintValuesUsingHashTable {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "minHi")]
        min_hi: i64,
        #[serde(rename = "minLo")]
        min_lo: i64,
        #[serde(rename = "maxHi")]
        max_hi: i64,
        #[serde(rename = "maxLo")]
        max_lo: i64,
        #[serde(rename = "valuesHi")]
        values_hi: Vec<i64>,
        #[serde(rename = "valuesLo")]
        values_lo: Vec<i64>,
    },
    TimestampRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        #[serde(rename = "lowerTsSec")]
        lower_sec: i64,
        #[serde(rename = "lowerTsNanos")]
        lower_nanos: i64,
        #[serde(rename = "upperTsSec")]
        upper_sec: i64,
        #[serde(rename = "upperTsNanos")]
        upper_nanos: i64,
    },
    BigintMultiRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        ranges: Vec<FilterJson>,
    },
    MultiRange {
        #[serde(rename = "nullAllowed")]
        null_allowed: bool,
        subfilters: Vec<FilterJson>,
    },
}

/// Parse `json` and emit `[SCANSPEC-FFI]` lines describing the deserialised
/// `ScanSpec`. Returns `Err` on unknown version or parse failures.
pub fn parse_and_print_scan_spec(json: String) -> Result<(), String> {
    let parsed: ScanSpecRootJson = serde_json::from_str(&json)
        .map_err(|e| format!("scan_spec_ffi: parse error: {e}"))?;
    if parsed.v != SCAN_SPEC_JSON_VERSION {
        return Err(format!(
            "scan_spec_ffi: unsupported version {} (expected {})",
            parsed.v, SCAN_SPEC_JSON_VERSION,
        ));
    }
    eprintln!("[SCANSPEC-FFI] BEGIN v={}", parsed.v);
    print_spec(&parsed.spec, 0);
    eprintln!("[SCANSPEC-FFI] END");
    Ok(())
}

fn print_spec(s: &ScanSpecJson, depth: usize) {
    let indent = "  ".repeat(depth);
    eprintln!(
        "[SCANSPEC-FFI] {indent}spec name={:?} subscript={} channel={} \
         projectOut={} filterDisabled={} columnType={}",
        s.name, s.subscript, s.channel, s.project_out, s.filter_disabled, s.column_type,
    );
    let child_indent = "  ".repeat(depth + 1);
    if let Some(f) = &s.filter {
        print_filter(f, depth + 1);
    } else {
        eprintln!("[SCANSPEC-FFI] {child_indent}filter=<none>");
    }
    for c in &s.children {
        print_spec(c, depth + 1);
    }
}

fn print_filter(f: &FilterJson, depth: usize) {
    let indent = "  ".repeat(depth);
    match f {
        FilterJson::AlwaysTrue { null_allowed } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=alwaysTrue nullAllowed={null_allowed}"
        ),
        FilterJson::AlwaysFalse { null_allowed } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=alwaysFalse nullAllowed={null_allowed}"
        ),
        FilterJson::IsNull { null_allowed } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=isNull nullAllowed={null_allowed}"
        ),
        FilterJson::IsNotNull { null_allowed } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=isNotNull nullAllowed={null_allowed}"
        ),
        FilterJson::BoolValue { null_allowed, value } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=boolValue value={value} nullAllowed={null_allowed}"
        ),
        FilterJson::BigintRange { null_allowed, lower, upper } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=bigintRange lower={lower} upper={upper} nullAllowed={null_allowed}"
        ),
        FilterJson::NegatedBigintRange { null_allowed, lower, upper } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=negatedBigintRange lower={lower} upper={upper} nullAllowed={null_allowed}"
        ),
        FilterJson::BigintValuesUsingHashTable { null_allowed, min, max, values } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=bigintValuesUsingHashTable min={min} max={max} values={values:?} nullAllowed={null_allowed}"
        ),
        FilterJson::BigintValuesUsingBitmask { null_allowed, min, max, values } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=bigintValuesUsingBitmask min={min} max={max} values={values:?} nullAllowed={null_allowed}"
        ),
        FilterJson::NegatedBigintValuesUsingHashTable { null_allowed, min, max, values } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=negatedBigintValuesUsingHashTable min={min} max={max} values={values:?} nullAllowed={null_allowed}"
        ),
        FilterJson::NegatedBigintValuesUsingBitmask { null_allowed, min, max, values } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=negatedBigintValuesUsingBitmask min={min} max={max} values={values:?} nullAllowed={null_allowed}"
        ),
        FilterJson::DoubleRange {
            null_allowed,
            lower,
            upper,
            lower_unbounded,
            upper_unbounded,
            lower_exclusive,
            upper_exclusive,
        } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=doubleRange lower={lower} upper={upper} lowerUnbounded={lower_unbounded} upperUnbounded={upper_unbounded} lowerExclusive={lower_exclusive} upperExclusive={upper_exclusive} nullAllowed={null_allowed}"
        ),
        FilterJson::FloatRange {
            null_allowed,
            lower,
            upper,
            lower_unbounded,
            upper_unbounded,
            lower_exclusive,
            upper_exclusive,
        } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=floatRange lower={lower} upper={upper} lowerUnbounded={lower_unbounded} upperUnbounded={upper_unbounded} lowerExclusive={lower_exclusive} upperExclusive={upper_exclusive} nullAllowed={null_allowed}"
        ),
        FilterJson::BytesRange {
            null_allowed,
            lower,
            upper,
            lower_unbounded,
            upper_unbounded,
            lower_exclusive,
            upper_exclusive,
        } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=bytesRange lower={lower:?} upper={upper:?} lowerUnbounded={lower_unbounded} upperUnbounded={upper_unbounded} lowerExclusive={lower_exclusive} upperExclusive={upper_exclusive} nullAllowed={null_allowed}"
        ),
        FilterJson::NegatedBytesRange {
            null_allowed,
            lower,
            upper,
            lower_unbounded,
            upper_unbounded,
            lower_exclusive,
            upper_exclusive,
        } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=negatedBytesRange lower={lower:?} upper={upper:?} lowerUnbounded={lower_unbounded} upperUnbounded={upper_unbounded} lowerExclusive={lower_exclusive} upperExclusive={upper_exclusive} nullAllowed={null_allowed}"
        ),
        FilterJson::BytesValues { null_allowed, values } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=bytesValues values={values:?} nullAllowed={null_allowed}"
        ),
        FilterJson::NegatedBytesValues { null_allowed, values } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=negatedBytesValues values={values:?} nullAllowed={null_allowed}"
        ),
        FilterJson::HugeintRange {
            null_allowed,
            lower_hi,
            lower_lo,
            upper_hi,
            upper_lo,
        } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=hugeintRange lowerHi={lower_hi} lowerLo={lower_lo} upperHi={upper_hi} upperLo={upper_lo} nullAllowed={null_allowed}"
        ),
        FilterJson::HugeintValuesUsingHashTable {
            null_allowed,
            min_hi,
            min_lo,
            max_hi,
            max_lo,
            values_hi,
            values_lo,
        } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=hugeintValuesUsingHashTable minHi={min_hi} minLo={min_lo} maxHi={max_hi} maxLo={max_lo} valuesHi={values_hi:?} valuesLo={values_lo:?} nullAllowed={null_allowed}"
        ),
        FilterJson::TimestampRange {
            null_allowed,
            lower_sec,
            lower_nanos,
            upper_sec,
            upper_nanos,
        } => eprintln!(
            "[SCANSPEC-FFI] {indent}filter kind=timestampRange lowerSec={lower_sec} lowerNanos={lower_nanos} upperSec={upper_sec} upperNanos={upper_nanos} nullAllowed={null_allowed}"
        ),
        FilterJson::BigintMultiRange { null_allowed, ranges } => {
            eprintln!(
                "[SCANSPEC-FFI] {indent}filter kind=bigintMultiRange n={} nullAllowed={null_allowed}",
                ranges.len()
            );
            for r in ranges {
                print_filter(r, depth + 1);
            }
        }
        FilterJson::MultiRange { null_allowed, subfilters } => {
            eprintln!(
                "[SCANSPEC-FFI] {indent}filter kind=multiRange n={} nullAllowed={null_allowed}",
                subfilters.len()
            );
            for r in subfilters {
                print_filter(r, depth + 1);
            }
        }
    }
}
