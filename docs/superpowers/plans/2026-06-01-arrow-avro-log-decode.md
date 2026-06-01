# Route Hudi Avro Log Decode Through arrow-avro — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hudi-rs's hand-rolled Avro→Arrow conversion for **data** log blocks with Apache `arrow-avro`'s decoder, behind a behavior-preserving schema reconcile, so all 17 `file_group_reader_tests.rs` e2e cases stay green.

**Architecture:** Add one additive public type `AvroBodyDecoder` to a fork of `arrow-avro` (tag `57.3.1`). In hudi-rs, wire that fork in via a path dep + `[patch.crates-io]` (single arrow source), then rewrite the body of `Decoder::decode_avro_record_content` to feed each bare Avro record body to `AvroBodyDecoder` and conform the resulting batches to `to_arrow_schema(writer_schema)` (the schema the old path produced) using the existing `reconcile_batch_to_schema`. Delete the now-dead `AvroDataBlockContentReader`.

**Tech Stack:** Rust (edition 2024, rustc 1.88), `arrow`/`arrow-array`/`arrow-schema` 57.x, `arrow-avro` 57.3.1 (forked), `apache-avro` 0.21 (retained for the delete path only).

**Repos / paths (absolute):**
- hudi-rs: `/home/ubuntu/ws3/hudi-rs` — on branch `feat/arrow-avro-log-decode`
- arrow-rs fork: `/home/ubuntu/ws3/arrow-rs` — tag `57.3.1` already fetched

**Reference spec:** `docs/superpowers/specs/2026-06-01-arrow-avro-hudi-log-decode-design.md`

---

## Task 1: Add `AvroBodyDecoder` to the arrow-avro fork

**Files (in `/home/ubuntu/ws3/arrow-rs`):**
- Modify: `arrow-avro/src/reader/mod.rs` (append a public type + a test module, just before the existing `#[cfg(test)] mod tests`)

- [ ] **Step 1: Create the fork branch off 57.3.1**

```bash
cd /home/ubuntu/ws3/arrow-rs
git checkout -b feat/avro-body-decoder 57.3.1
git log --oneline -1   # expect the 57.3.1 release commit
grep -m1 '^version' Cargo.toml   # expect: version = "57.3.1"
```

- [ ] **Step 2: Write the failing test**

Append this module to the **end** of `arrow-avro/src/reader/mod.rs` (after the final `}` of the file, i.e. after the existing `#[cfg(test)] mod tests { ... }` block):

```rust
#[cfg(test)]
mod body_decoder_tests {
    use super::AvroBodyDecoder;
    use arrow_array::Array;

    #[test]
    fn test_avro_body_decoder_decodes_bare_bodies() {
        let schema = crate::schema::AvroSchema::new(
            r#"{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}"#.to_string(),
        );
        let mut dec = AvroBodyDecoder::try_new(&schema, false, false).unwrap();
        // Avro long 7 => zig-zag 14 => single byte 0x0E (a bare body, no framing)
        let consumed = dec.decode(&[0x0E], 1).unwrap();
        assert_eq!(consumed, 1);
        let batch = dec.flush().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 7);
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

```bash
cd /home/ubuntu/ws3/arrow-rs
cargo test -p arrow-avro body_decoder_tests 2>&1 | tail -20
```
Expected: FAIL to **compile** — `cannot find type AvroBodyDecoder in this scope` (it does not exist yet).

- [ ] **Step 4: Add the `AvroBodyDecoder` implementation**

Insert this block immediately **before** the existing `#[cfg(test)] mod tests {` line in `arrow-avro/src/reader/mod.rs`. No new imports are needed — `AvroFieldBuilder`, `AvroSchema`, `RecordDecoder`, `RecordBatch`, `ArrowError`, `SchemaRef` are all already imported at the top of this file.

```rust
/// A low-level decoder that turns **bare Avro record bodies** (no OCF, SOE, Confluent, or
/// length framing) directly into Arrow [`RecordBatch`]es using a fixed writer schema.
///
/// This is the engine behind [`Decoder`]/[`Reader`] exposed for callers that own their own
/// record framing (e.g. Hudi log blocks, which prefix each datum with a 4-byte length).
/// Feed each record body to [`AvroBodyDecoder::decode`], then drain with
/// [`AvroBodyDecoder::flush`].
#[derive(Debug)]
pub struct AvroBodyDecoder {
    inner: RecordDecoder,
}

impl AvroBodyDecoder {
    /// Build a decoder for `writer_schema`.
    ///
    /// * `utf8_view` — produce `Utf8View`/`BinaryView` columns instead of `Utf8`/`Binary`.
    /// * `strict_mode` — reject `['T','null']`-shaped unions (vs. the default lenient handling).
    pub fn try_new(
        writer_schema: &AvroSchema,
        utf8_view: bool,
        strict_mode: bool,
    ) -> Result<Self, ArrowError> {
        let ws = writer_schema.schema()?;
        let root = AvroFieldBuilder::new(&ws)
            .with_utf8view(utf8_view)
            .with_strict_mode(strict_mode)
            .build()?;
        Ok(Self {
            inner: RecordDecoder::try_new_with_options(root.data_type())?,
        })
    }

    /// The Arrow schema this decoder produces (derived from the writer schema).
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema().clone()
    }

    /// Decode `count` consecutive Avro record bodies from the front of `body`.
    /// Returns the number of bytes consumed.
    pub fn decode(&mut self, body: &[u8], count: usize) -> Result<usize, ArrowError> {
        self.inner.decode(body, count)
    }

    /// Drain all rows decoded since the last flush into a single [`RecordBatch`].
    pub fn flush(&mut self) -> Result<RecordBatch, ArrowError> {
        self.inner.flush()
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

```bash
cd /home/ubuntu/ws3/arrow-rs
cargo test -p arrow-avro body_decoder_tests 2>&1 | tail -20
```
Expected: PASS (`test result: ok. 1 passed`).

- [ ] **Step 6: Commit (in the arrow-rs fork)**

```bash
cd /home/ubuntu/ws3/arrow-rs
git add arrow-avro/src/reader/mod.rs
git commit -m "feat(arrow-avro): add AvroBodyDecoder for bare-body decode

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Wire the fork into hudi-rs with a single arrow source

**Files (in `/home/ubuntu/ws3/hudi-rs`):**
- Modify: `Cargo.toml` (workspace root — add `arrow-avro` to `[workspace.dependencies]`, add `[patch.crates-io]`)
- Modify: `crates/core/Cargo.toml` (add `arrow-avro = { workspace = true }`)

- [ ] **Step 1: Add `arrow-avro` to the workspace dependencies**

In `/home/ubuntu/ws3/hudi-rs/Cargo.toml`, under `[workspace.dependencies]` (the `# arrow` group, right after the `arrow-select = { version = "57" }` line at ~line 50), add:

```toml
arrow-avro = { path = "../arrow-rs/arrow-avro", default-features = false }
```

- [ ] **Step 2: Add the `[patch.crates-io]` block (single arrow source)**

Append to the **end** of `/home/ubuntu/ws3/hudi-rs/Cargo.toml`:

```toml
# POC: force all arrow-* leaf crates to resolve to the local arrow-rs @ 57.3.1 checkout,
# so RecordBatch/Schema types unify between hudi-core and the forked arrow-avro.
# The local checkout is byte-identical to crates.io 57.3.1, so behavior is unchanged.
[patch.crates-io]
arrow-array  = { path = "../arrow-rs/arrow-array" }
arrow-schema = { path = "../arrow-rs/arrow-schema" }
arrow-buffer = { path = "../arrow-rs/arrow-buffer" }
arrow-data   = { path = "../arrow-rs/arrow-data" }
arrow-select = { path = "../arrow-rs/arrow-select" }
```

- [ ] **Step 3: Add the dependency to hudi-core**

In `/home/ubuntu/ws3/hudi-rs/crates/core/Cargo.toml`, in the `# arrow` group (after `arrow-select = { workspace = true }` at ~line 45), add:

```toml
arrow-avro = { workspace = true }
```

- [ ] **Step 4: Verify it resolves to a single arrow-array source**

```bash
cd /home/ubuntu/ws3/hudi-rs
cargo build -p hudi-core 2>&1 | tail -20
cargo tree -p hudi-core -i arrow-array 2>&1 | head -20
```
Expected: build succeeds; `cargo tree` shows **one** `arrow-array v57.3.1` entry sourced from the local path (no duplicate crates.io copy).

If the build reports a duplicate/conflict for another arrow crate (e.g. `arrow-ord`, `arrow-cast`), add that crate to the `[patch.crates-io]` block with the same `{ path = "../arrow-rs/<crate>" }` form and rebuild.

- [ ] **Step 5: Commit**

```bash
cd /home/ubuntu/ws3/hudi-rs
git add Cargo.toml crates/core/Cargo.toml Cargo.lock
git commit -m "build: depend on forked arrow-avro 57.3.1 with single-arrow-source patch

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Expose `reconcile_batch_to_schema` to the log-file decoder

**Files:**
- Modify: `crates/core/src/file_group/reader/buffer/row_extraction.rs:88`

- [ ] **Step 1: Promote the function visibility**

In `crates/core/src/file_group/reader/buffer/row_extraction.rs`, change line 88 from:

```rust
fn reconcile_batch_to_schema(batch: &RecordBatch, target_schema: &SchemaRef) -> RecordBatch {
```
to:
```rust
pub(crate) fn reconcile_batch_to_schema(batch: &RecordBatch, target_schema: &SchemaRef) -> RecordBatch {
```

- [ ] **Step 2: Verify it still compiles**

```bash
cd /home/ubuntu/ws3/hudi-rs
cargo build -p hudi-core 2>&1 | tail -10
```
Expected: build succeeds (no behavior change; only visibility widened).

- [ ] **Step 3: Commit**

```bash
cd /home/ubuntu/ws3/hudi-rs
git add crates/core/src/file_group/reader/buffer/row_extraction.rs
git commit -m "refactor: make reconcile_batch_to_schema pub(crate)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Rewrite `decode_avro_record_content` to use `AvroBodyDecoder`

**Files:**
- Modify: `crates/core/src/file_group/log_file/content.rs` (imports + `decode_avro_record_content` + a new private helper)

The existing unit test `content.rs::tests::test_decode_avro_content` is the regression anchor: it builds a synthetic v3 Avro block (`{id: long, name: ["null","string"]}`, 2 records) and asserts `id = [42, 43]`, `name = ["Alice", null]`. It must keep passing through the new path.

- [ ] **Step 1: Run the anchor test against the OLD implementation (baseline)**

```bash
cd /home/ubuntu/ws3/hudi-rs
cargo test -p hudi-core --lib file_group::log_file::content::tests::test_decode_avro_content 2>&1 | tail -15
```
Expected: PASS (proves the synthetic block + assertions are valid before we change the engine).

- [ ] **Step 2: Update the imports in `content.rs`**

In `crates/core/src/file_group/log_file/content.rs`, **remove** this line:

```rust
use crate::file_group::log_file::avro::AvroDataBlockContentReader;
```

and **add** these lines to the import block (top of file):

```rust
use crate::avro_to_arrow::to_arrow_schema;
use crate::file_group::reader::buffer::row_extraction::reconcile_batch_to_schema;
use arrow_avro::reader::AvroBodyDecoder;
use arrow_avro::schema::AvroSchema as ArrowAvroSchema;
use arrow_schema::SchemaRef;
```

(Keep `use apache_avro::{Schema as AvroSchema, from_avro_datum};` and `use crate::avro_to_arrow::arrow_array_reader::AvroArrowArrayReader;` — both are still used by the **delete** path.)

- [ ] **Step 3: Replace the body of `decode_avro_record_content` and add the flush helper**

Replace the entire existing `fn decode_avro_record_content(...) { ... }` (currently `content.rs:100-127`) with:

```rust
    fn decode_avro_record_content(
        &self,
        mut reader: impl Read,
        header: &HashMap<BlockMetadataKey, String>,
    ) -> Result<RecordBatches> {
        Decoder::validate_log_block_version(&mut reader)?;

        let writer_schema_json = header.get(&BlockMetadataKey::Schema).ok_or_else(|| {
            CoreError::LogBlockError("Schema not found in block header".to_string())
        })?;

        // Record count (big-endian u32) — bespoke Hudi framing.
        let mut record_count_buf = [0u8; 4];
        reader.read_exact(&mut record_count_buf)?;
        let record_count = u32::from_be_bytes(record_count_buf);

        // Equivalence oracle: the exact Arrow schema the previous (apache-avro +
        // AvroArrowArrayReader) path produced. We conform arrow-avro's output to this so
        // downstream behavior is unchanged while the decode engine is swapped.
        let writer_schema = AvroSchema::parse_str(writer_schema_json)?;
        let expected_schema: SchemaRef = Arc::new(to_arrow_schema(&writer_schema)?);

        // New engine: arrow-avro decodes bare Avro record bodies straight into Arrow.
        let mut decoder = AvroBodyDecoder::try_new(
            &ArrowAvroSchema::new(writer_schema_json.clone()),
            false, // utf8_view
            false, // strict_mode
        )
        .map_err(CoreError::ArrowError)?;

        log::info!(
            "[arrow-avro] decoding Avro data block: {record_count} records (batch_size={})",
            self.batch_size
        );
        let arrow_avro_schema = decoder.schema();
        if arrow_avro_schema.as_ref() != expected_schema.as_ref() {
            log::warn!(
                "[arrow-avro] schema divergence; reconciling to oracle. arrow-avro={:?} expected={:?}",
                arrow_avro_schema,
                expected_schema
            );
        } else {
            log::debug!("[arrow-avro] derived schema matches to_arrow_schema oracle");
        }

        // The remaining payload is the records region: [L0][datum0][L1][datum1]...
        // It is already in the in-memory file buffer, so read_to_end is a cheap copy.
        let mut payload = Vec::new();
        reader.read_to_end(&mut payload)?;

        let mut batches =
            RecordBatches::new_with_capacity(record_count as usize / self.batch_size + 1, 0);
        let mut pos = 0usize;
        let mut rows_in_batch = 0usize;

        for i in 0..record_count as usize {
            // Per-record 4-byte big-endian length prefix.
            let len_end = pos.checked_add(4).filter(|&e| e <= payload.len()).ok_or_else(|| {
                CoreError::LogBlockError(format!("Truncated record length prefix for record {i}"))
            })?;
            let li = u32::from_be_bytes(payload[pos..len_end].try_into().unwrap()) as usize;
            pos = len_end;
            let body_end = pos.checked_add(li).filter(|&e| e <= payload.len()).ok_or_else(|| {
                CoreError::LogBlockError(format!("Truncated datum for record {i}"))
            })?;

            decoder
                .decode(&payload[pos..body_end], 1)
                .map_err(CoreError::ArrowError)?;
            pos = body_end;
            rows_in_batch += 1;

            if rows_in_batch == self.batch_size {
                Self::flush_decoder(&mut decoder, &expected_schema, &mut batches)?;
                rows_in_batch = 0;
            }
        }
        if rows_in_batch > 0 {
            Self::flush_decoder(&mut decoder, &expected_schema, &mut batches)?;
        }

        Ok(batches)
    }

    /// Flush one batch from `decoder`, conforming it to `expected_schema` (by-name +
    /// `arrow_cast`, via [`reconcile_batch_to_schema`]) so downstream sees the same schema
    /// the previous decode path produced.
    fn flush_decoder(
        decoder: &mut AvroBodyDecoder,
        expected_schema: &SchemaRef,
        batches: &mut RecordBatches,
    ) -> Result<()> {
        let batch = decoder.flush().map_err(CoreError::ArrowError)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch = if batch.schema().as_ref() == expected_schema.as_ref() {
            batch
        } else {
            reconcile_batch_to_schema(&batch, expected_schema)
        };
        batches.push_data_batch(batch);
        Ok(())
    }
```

- [ ] **Step 4: Run the anchor test against the NEW implementation**

```bash
cd /home/ubuntu/ws3/hudi-rs
cargo test -p hudi-core --lib file_group::log_file::content::tests::test_decode_avro_content 2>&1 | tail -20
```
Expected: PASS (same assertions, now satisfied by the arrow-avro path).

- [ ] **Step 5: Commit**

```bash
cd /home/ubuntu/ws3/hudi-rs
git add crates/core/src/file_group/log_file/content.rs
git commit -m "feat: decode Avro log data blocks via arrow-avro AvroBodyDecoder

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Delete the now-dead `AvroDataBlockContentReader`

**Files:**
- Delete: `crates/core/src/file_group/log_file/avro.rs`
- Modify: `crates/core/src/file_group/log_file/mod.rs:27` (remove `mod avro;`)

- [ ] **Step 1: Remove the module declaration**

In `crates/core/src/file_group/log_file/mod.rs`, delete line 27:

```rust
mod avro;
```

- [ ] **Step 2: Delete the file**

```bash
cd /home/ubuntu/ws3/hudi-rs
git rm crates/core/src/file_group/log_file/avro.rs
```

- [ ] **Step 3: Verify no remaining references**

```bash
cd /home/ubuntu/ws3/hudi-rs
grep -rn "AvroDataBlockContentReader\|log_file::avro" crates/ ; echo "exit=$?"
```
Expected: no matches (a non-zero grep exit / empty output).

- [ ] **Step 4: Build the whole workspace (legacy reader included)**

```bash
cd /home/ubuntu/ws3/hudi-rs
cargo build --workspace --all-targets 2>&1 | tail -20
```
Expected: build succeeds. (The legacy `reader_v1.rs` shares `decode_avro_record_content`, which still compiles; it now silently uses the new engine.)

- [ ] **Step 5: Commit**

```bash
cd /home/ubuntu/ws3/hudi-rs
git add crates/core/src/file_group/log_file/mod.rs
git commit -m "refactor: remove dead AvroDataBlockContentReader (data path now uses arrow-avro)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Acceptance — e2e tests green + new-path logs + lint clean

**Files:** none (verification only)

- [ ] **Step 1: Run the 17 e2e file-group-reader tests with new-path logging**

```bash
cd /home/ubuntu/ws3/hudi-rs
RUST_LOG=hudi_core=info cargo test -p hudi-core --test file_group_reader_tests -- --nocapture 2>&1 | tee /tmp/fg_e2e.log | tail -40
```
Expected: `test result: ok.` with all tests passed and **0 failed**.

- [ ] **Step 2: Confirm the new path actually executed**

```bash
grep -c "\[arrow-avro\] decoding Avro data block" /tmp/fg_e2e.log
```
Expected: a count `>= 1` (one line per decoded MOR data log block). A `0` here means the new path did not run — investigate before declaring success.

- [ ] **Step 3: Run the full hudi-core test suite (catch collateral breakage)**

```bash
cd /home/ubuntu/ws3/hudi-rs
cargo test -p hudi-core 2>&1 | tail -25
```
Expected: all tests pass.

- [ ] **Step 4: Lint + format (matches CI)**

```bash
cd /home/ubuntu/ws3/hudi-rs
cargo fmt --all
cargo clippy --all-targets --all-features --workspace --no-deps -- -D warnings 2>&1 | tail -25
```
Expected: clippy clean (no warnings-as-errors), fmt produces no further changes.

- [ ] **Step 5: Commit any fmt fixups**

```bash
cd /home/ubuntu/ws3/hudi-rs
git add -A -- crates/ Cargo.toml
git commit -m "style: cargo fmt after arrow-avro log decode swap" || echo "nothing to commit"
```

---

## Notes & fallbacks

- **Schema divergence:** if a structural difference defeats `arrow_cast` inside
  `reconcile_batch_to_schema` (e.g. a dense-union representation mismatch), a specific e2e
  test will fail and name the column. Fix by extending `reconcile_batch_to_schema` for that
  case — do **not** pre-build generality before a failing test points at it.
- **Delete blocks are unchanged:** `decode_delete_record_content` still uses
  `from_avro_datum` + `AvroArrowArrayReader`. Migrating it is out of scope for this POC.
- **Patch list:** if `cargo build` (Task 2 Step 4) reports a duplicate arrow crate beyond
  the five patched, add it to `[patch.crates-io]` with the same local-path form.
- **`.claude/` is untracked** in hudi-rs; never `git add -A` from the repo root (it sweeps
  in an embedded worktree). The Task commits stage explicit paths.
```
