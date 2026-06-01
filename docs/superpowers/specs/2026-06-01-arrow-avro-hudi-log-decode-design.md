# Design: Route Hudi Avro log-block decode through `arrow-avro`

**Date:** 2026-06-01
**Status:** Approved (pre-implementation)
**Branch:** `feat/arrow-avro-log-decode` (hudi-rs) + `feat/avro-body-decoder` off tag `57.3.1` (arrow-rs fork)

## 1. Goal

Replace hudi-rs's hand-rolled Avro→Arrow conversion for **data log blocks** with
Apache Arrow's `arrow-avro` decoder, reusing its conversion engine instead of the
`apache_avro::from_avro_datum` → `Value` → `AvroArrowArrayReader` pipeline.

This is a proof-of-concept. **Acceptance criterion:** all 17 `#[tokio::test]` cases in
`crates/core/tests/file_group_reader_tests.rs` (real v9 MOR tables, parquet base + Avro
logs, read through `HoodieFileGroupReader`) pass green, and logs prove the new path ran.

## 2. Scope

**In scope**
- The `BlockType::AvroData` branch of `Decoder::decode_avro_record_content`
  (`crates/core/src/file_group/log_file/content.rs:100`) — the single chokepoint both
  readers funnel through.
- A minimal, additive public API in the `arrow-avro` crate (forked at tag `57.3.1`).

**Out of scope**
- Delete blocks, Parquet, HFile branches.
- The legacy reader `crates/core/src/file_group/reader_v1.rs::FileGroupReader` — must only
  keep compiling. Since we change a function *body* (not signature), it is untouched.
- Downstream merge / output-converter / schema-handler logic.
- Migrating delete blocks off the old converter (flagged as follow-up).

## 3. Key facts established during exploration

- **Single chokepoint:** both `HoodieFileGroupReader` (target) and `FileGroupReader`
  (legacy) reach Avro→Arrow via `Decoder::decode_avro_record_content`. A body-only swap
  there hits the target and leaves legacy compiling.
- **Version match:** `arrow-avro` at tag `57.3.1` carries the internals we need
  (`RecordDecoder`, `AvroFieldBuilder`, `AvroField::data_type`, `AvroSchema` — byte-for-byte
  the same as 57.0.0) and its workspace arrow is `57.3.1`, semver-compatible with hudi-rs's
  `arrow = "57"` (`^57`). See §3.1 for version rationale.
- **Dependency hazard:** `arrow-avro`'s arrow deps are `workspace = true` → local paths.
  A plain path-dep would give cargo *two* `arrow-array` crates (crates.io 57 vs
  local-path 57) and `RecordBatch` would not type-unify. Fixed via `[patch.crates-io]`
  (see §6).
- **Downstream is schema-tolerant by name:** `record_context.batch_to_buffered_records`
  extracts columns by field name (`index_of`, `column_with_name`), and
  `reconcile_batch_to_schema` (`reader/buffer/row_extraction.rs:88`) already bridges
  "Avro-derived schemas (from log files) vs Parquet-derived schemas" via name lookup +
  `arrow_cast`. This is the reuse vehicle for divergence handling.

### 3.1 Version rationale (pin 57.3.1 now, track 58.3.0 later)

| | Version | Arrow workspace | Compatible with hudi `arrow = "57"` (`^57`) |
|---|---|---|---|
| Latest overall | 58.3.0 | 58.3.0 | No — would force an arrow-58 bump across all of hudi-rs |
| Latest on the 57 line | **57.3.1** (chosen) | 57.3.1 | Yes |

- **Within the 57 line (57.0.0 → 57.3.1):** almost entirely **writer** changes plus dep
  bumps; the **reader** engine moved ~7 lines in `record.rs` / ~8 in `codec.rs`. For our
  decode path, 57.0.0 and 57.3.1 are effectively identical, so the `AvroBodyDecoder` patch
  applies unchanged.
- **58-only reader fixes we forgo** (need arrow 58): #9328 union resolution, #9237 schema
  resolution, #9605 skipper for resolved named records, #9280 configurable timestamp tz,
  #9291 additional Arrow types. The correctness fixes are **all on the schema-resolution
  path** — they fire only when a *reader schema differs from the writer schema*
  (evolution / projection). Our design decodes with the **writer schema only** and
  reconciles downstream by name + `arrow_cast`, so it does not exercise that path.
- **Follow-up:** track 58.3.0. The trigger to move (and bump hudi to arrow 58) is if we
  ever replace the downstream reconcile with arrow-avro's native reader-schema / projection
  resolution — that is where the 58-era reader fixes matter.

## 4. New `arrow-avro` public API (additive, ~25 LOC in `reader/mod.rs`)

`reader/mod.rs` already imports `AvroFieldBuilder` and `RecordDecoder`, so the wrapper
calls existing `pub(crate)` items directly. Nothing existing changes.

```rust
/// Decodes bare Avro record bodies (no OCF/SOE/length framing) into Arrow `RecordBatch`es.
/// The caller owns record framing; feed each datum body, flush in batches.
pub struct AvroBodyDecoder { inner: RecordDecoder }

impl AvroBodyDecoder {
    pub fn try_new(writer_schema: &AvroSchema, utf8_view: bool, strict_mode: bool)
        -> Result<Self, ArrowError> {
        let ws = writer_schema.schema()?;                 // pub(crate), reachable in-crate
        let root = AvroFieldBuilder::new(&ws)
            .with_utf8view(utf8_view)
            .with_strict_mode(strict_mode)
            .build()?;
        Ok(Self { inner: RecordDecoder::try_new_with_options(root.data_type())? })
    }

    /// arrow-avro's derived Arrow schema — for divergence logging / comparison.
    pub fn schema(&self) -> SchemaRef { self.inner.schema().clone() }

    /// Decode `count` Avro record bodies from `body`; returns bytes consumed.
    pub fn decode(&mut self, body: &[u8], count: usize) -> Result<usize, ArrowError> {
        self.inner.decode(body, count)
    }

    /// Drain decoded rows into a `RecordBatch`.
    pub fn flush(&mut self) -> Result<RecordBatch, ArrowError> { self.inner.flush() }
}
```

We deliberately do **not** expose arrow-avro's reader-schema/projection path: downstream
extraction is by-name and `reconcile_batch_to_schema` casts types, so projection /
evolution is already handled hudi-side (YAGNI).

## 5. New `decode_avro_record_content` flow (hudi side)

Signature unchanged. All bespoke framing reads preserved; only the engine swaps.

```
validate block version == 3                         (unchanged)
schema_json = header[Schema]
read record_count (BE u32)                           (unchanged)

# --- equivalence oracle: the schema the OLD path produced ---
expected = to_arrow_schema(&apache_avro::Schema::parse_str(schema_json))   # schema-mapping only

# --- new engine ---
decoder = AvroBodyDecoder::try_new(AvroSchema::new(schema_json), false, false)
log::info!("[arrow-avro] decoding Avro data block: {record_count} records")
if decoder.schema() != expected {
    log::warn!("[arrow-avro] schema divergence: avro-avro={...} expected(to_arrow_schema)={...}")
}

read_to_end(rest) → payload         # records region [L0][d0][L1][d1]...; in-memory Bytes, cheap
walk payload for N records:
    Li = BE u32; body = next Li bytes
    decoder.decode(body, 1)
    every batch_size records: flush_one()
flush_one()                          # remainder; only when rows were decoded

# flush_one():
#   batch = decoder.flush()
#   if batch.num_rows() == 0 { return }                  # match old behavior: no empty batch
#   batch = if batch.schema() == expected { batch }
#           else { reconcile_batch_to_schema(&batch, &expected) }   # by-name + arrow_cast
#   batches.push_data_batch(batch)
```

Notes:
- `to_arrow_schema` (Avro→Arrow schema) is **kept** — it is schema mapping, not value
  conversion, and serves as the equivalence oracle so downstream sees the identical schema
  it always saw.
- The writer schema is parsed twice (once via `apache_avro` for the oracle, once inside
  arrow-avro). Negligible: once per block.
- `reconcile_batch_to_schema` is promoted from private to `pub(crate)` and reused as-is.

## 6. Dependency wiring (single arrow source)

In hudi-rs workspace `Cargo.toml`:

```toml
# new dependency — compression codecs not needed (we feed raw bodies, not OCF)
arrow-avro = { path = "../arrow-rs/arrow-avro", default-features = false }

# force a single arrow source so RecordBatch unifies between hudi-core and arrow-avro
[patch.crates-io]
arrow-array  = { path = "../arrow-rs/arrow-array" }
arrow-schema = { path = "../arrow-rs/arrow-schema" }
arrow-buffer = { path = "../arrow-rs/arrow-buffer" }
arrow-data   = { path = "../arrow-rs/arrow-data" }
arrow-select = { path = "../arrow-rs/arrow-select" }
# extend to other arrow-* crates if the build still reports duplicates
```

The local arrow-rs checkout sits on a branch off tag `57.3.1` (identical to crates.io
57.3.1), so behavior is unchanged; the patch only collapses the two sources into one.

## 7. Removing old conversion from the fg-read **data** path

- **Delete** `AvroDataBlockContentReader` and its file
  `crates/core/src/file_group/log_file/avro.rs` (+ its `mod avro;` declaration and unit
  test) — only the AvroData branch referenced it; now dead.
- After the swap the AvroData branch invokes neither `from_avro_datum` nor
  `AvroArrowArrayReader`; the per-value `build_struct_array` / `resolve_*` engine is off
  the data path.
- `avro_to_arrow/arrow_array_reader.rs` (`AvroArrowArrayReader`) and `avro_to_arrow/schema.rs`
  (`to_arrow_schema`) **remain compiled**: the delete-block branch still uses the former,
  and the new data path uses the latter as its oracle. So "old conversion removed" applies
  to the **data** path specifically — which is what the logs verify.

## 8. Error handling

- `ArrowError` from `AvroBodyDecoder` maps to `CoreError::ArrowError` (as today).
- Malformed body / bad schema / truncated length prefix surface as errors, same as the
  current path.
- Zero-record block → no flush, no empty batch (matches current behavior).

## 9. Testing & verification

- **Acceptance gate:** all 17 `crates/core/tests/file_group_reader_tests.rs` e2e cases green.
- **Existing unit test** `content.rs::test_decode_avro_content` must pass unchanged
  (synthetic block through the new path).
- **Execution proof:** run with `RUST_LOG=info` (e.g. `hudi_core=info`); the `[arrow-avro]`
  log lines must appear for each MOR data log block. Absence ⇒ swap didn't take.
- `cargo build` + `cargo clippy --all-targets --all-features --workspace` clean, legacy
  `reader_v1` included.

## 10. Primary risk & fallback

`arrow-avro`'s derived Arrow schema may differ from `to_arrow_schema(writer)` in detail
(timestamp tz, decimal width, union shape, nested field names, field metadata,
nullability). The §5 reconcile (by-name + `arrow_cast`, with an ArrayData-rebuild
fallback) is designed to absorb these. If a structural difference defeats `arrow_cast`
(e.g. dense-union representation mismatch), the affected e2e test will fail and point at
the exact column; the fallback is a targeted extension of `reconcile_batch_to_schema` for
that case. No speculative generality is built ahead of a failing test.

## 11. Work breakdown (for the implementation plan)

1. arrow-rs fork: branch off `57.3.1`, add `AvroBodyDecoder` to `arrow-avro/src/reader/mod.rs`, build it.
2. hudi-rs: add `arrow-avro` dep + `[patch.crates-io]`; confirm `cargo build` unifies arrow.
3. hudi-rs: promote `reconcile_batch_to_schema` to `pub(crate)`.
4. hudi-rs: rewrite `decode_avro_record_content` body (engine swap + logging + oracle + reconcile).
5. hudi-rs: delete `avro.rs` (`AvroDataBlockContentReader`) and its `mod` decl.
6. Run unit test + 17 e2e tests with `RUST_LOG=info`; verify green + log lines.
7. clippy + fmt clean.
