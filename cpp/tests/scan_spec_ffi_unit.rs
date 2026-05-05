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

//! Rust-side smoke test for the Phase 1 ScanSpec FFI parser.
//!
//! Independent of the C++ side: hand-writes JSON in the layout produced
//! by `scanSpecToJson` and confirms `parse_and_print_scan_spec`
//! deserialises it without error and rejects bad versions.

use hudi::parse_and_print_scan_spec;

#[test]
fn parse_and_print_minimal_root() {
    let json = r#"{
        "v": 1,
        "name": "<root>",
        "subscript": -1,
        "channel": -1,
        "projectOut": false,
        "filterDisabled": false,
        "columnType": "regular",
        "filter": null,
        "children": []
    }"#;
    parse_and_print_scan_spec(json.to_string()).expect("minimal root should parse");
}

#[test]
fn parse_and_print_bigint_range() {
    let json = r#"{
        "v": 1,
        "name": "id",
        "subscript": 0,
        "channel": 0,
        "projectOut": true,
        "filterDisabled": false,
        "columnType": "regular",
        "filter": {
            "kind": "bigintRange",
            "nullAllowed": false,
            "lower": 9,
            "upper": 9
        },
        "children": []
    }"#;
    parse_and_print_scan_spec(json.to_string()).expect("bigintRange should parse");
}

#[test]
fn rejects_unknown_version() {
    let json = r#"{
        "v": 99,
        "name": "x",
        "subscript": -1,
        "channel": -1,
        "projectOut": false,
        "filterDisabled": false,
        "columnType": "regular",
        "filter": null,
        "children": []
    }"#;
    let err = parse_and_print_scan_spec(json.to_string())
        .expect_err("v=99 should be rejected");
    assert!(err.contains("unsupported version 99"), "unexpected error: {err}");
}

#[test]
fn rejects_garbage_json() {
    let err = parse_and_print_scan_spec("not json".to_string())
        .expect_err("non-JSON should fail");
    assert!(err.contains("parse error"), "unexpected error: {err}");
}
