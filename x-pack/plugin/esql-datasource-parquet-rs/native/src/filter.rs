use super::jni_utils::*;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, Scalar, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::kernels::cmp;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use jni::EnvUnowned;
use jni::errors::{Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JLongArray, JString};
use jni::sys::jlong;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection, RowSelector};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::schema::types::SchemaDescriptor;
use std::sync::Arc;

/// Filter expression tree built from Java via JNI, evaluated against
/// both row group statistics (for pruning) and decoded Arrow batches (for row filtering).
#[derive(Clone, Debug)]
pub enum FilterExpr {
    Column(String),
    LiteralInt(i32),
    LiteralLong(i64),
    LiteralDouble(f64),
    LiteralBool(bool),
    LiteralString(String),
    LiteralTimestampMillis(i64),
    Eq(Box<FilterExpr>, Box<FilterExpr>),
    NotEq(Box<FilterExpr>, Box<FilterExpr>),
    Gt(Box<FilterExpr>, Box<FilterExpr>),
    GtEq(Box<FilterExpr>, Box<FilterExpr>),
    Lt(Box<FilterExpr>, Box<FilterExpr>),
    LtEq(Box<FilterExpr>, Box<FilterExpr>),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
    IsNull(Box<FilterExpr>),
    IsNotNull(Box<FilterExpr>),
    InList(Box<FilterExpr>, Vec<FilterExpr>),
    /// SQL LIKE. The `bool` is `case_insensitive`: when true, the pattern is matched
    /// using arrow's `ilike` kernel (Unicode-aware case folding) instead of `like`.
    /// Set by the Java side from `WildcardLike.caseInsensitive()`, which is produced
    /// by the optimizer rule `ReplaceStringCasingWithInsensitiveRegexMatch` for
    /// `TO_UPPER(field) LIKE "..."` / `TO_LOWER(field) LIKE "..."` patterns.
    Like(Box<FilterExpr>, String, bool),
    NotLike(Box<FilterExpr>, String, bool),
    StartsWith(Box<FilterExpr>, String, Option<String>),
}

impl std::fmt::Display for FilterExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterExpr::Column(name) => write!(f, "{name}"),
            FilterExpr::LiteralInt(v) => write!(f, "{v}"),
            FilterExpr::LiteralLong(v) => write!(f, "{v}L"),
            FilterExpr::LiteralDouble(v) => write!(f, "{v}"),
            FilterExpr::LiteralBool(v) => write!(f, "{v}"),
            FilterExpr::LiteralString(v) => write!(f, "'{v}'"),
            FilterExpr::LiteralTimestampMillis(v) => write!(f, "ts_millis({v})"),
            FilterExpr::Eq(l, r) => write!(f, "{l} = {r}"),
            FilterExpr::NotEq(l, r) => write!(f, "{l} != {r}"),
            FilterExpr::Gt(l, r) => write!(f, "{l} > {r}"),
            FilterExpr::GtEq(l, r) => write!(f, "{l} >= {r}"),
            FilterExpr::Lt(l, r) => write!(f, "{l} < {r}"),
            FilterExpr::LtEq(l, r) => write!(f, "{l} <= {r}"),
            FilterExpr::And(l, r) => write!(f, "({l} AND {r})"),
            FilterExpr::Or(l, r) => write!(f, "({l} OR {r})"),
            FilterExpr::Not(inner) => write!(f, "NOT ({inner})"),
            FilterExpr::IsNull(inner) => write!(f, "{inner} IS NULL"),
            FilterExpr::IsNotNull(inner) => write!(f, "{inner} IS NOT NULL"),
            FilterExpr::InList(expr, items) => {
                write!(f, "{expr} IN (")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{item}")?;
                }
                write!(f, ")")
            }
            FilterExpr::Like(expr, pattern, ci) => {
                let op = if *ci { "ILIKE" } else { "LIKE" };
                write!(f, "{expr} {op} '{pattern}'")
            }
            FilterExpr::NotLike(expr, pattern, ci) => {
                let op = if *ci { "NOT ILIKE" } else { "NOT LIKE" };
                write!(f, "{expr} {op} '{pattern}'")
            }
            FilterExpr::StartsWith(expr, prefix, _) => write!(f, "{expr} STARTS WITH '{prefix}'"),
        }
    }
}

fn box_expr(expr: FilterExpr) -> jlong {
    Box::into_raw(Box::new(expr)) as jlong
}

unsafe fn unbox_expr(handle: jlong) -> Box<FilterExpr> {
    unsafe { Box::from_raw(handle as *mut FilterExpr) }
}

// ---------------------------------------------------------------------------
// Row group pruning using column statistics
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub enum StatValue {
    Int(i32),
    Long(i64),
    Double(f64),
    Bool(bool),
    Str(String),
}

impl std::fmt::Display for StatValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatValue::Int(v) => write!(f, "{v}"),
            StatValue::Long(v) => write!(f, "{v}"),
            StatValue::Double(v) => write!(f, "{v}"),
            StatValue::Bool(v) => write!(f, "{v}"),
            StatValue::Str(v) => write!(f, "{v}"),
        }
    }
}

struct ColMinMax {
    min: Option<StatValue>,
    max: Option<StatValue>,
    null_count: i64,
    num_rows: i64,
}

fn get_col_stats(
    rg: &RowGroupMetaData,
    col_name: &str,
    schema: &SchemaDescriptor,
) -> Option<ColMinMax> {
    let col_idx = (0..schema.num_columns())
        .find(|&i| schema.column(i).name().eq_ignore_ascii_case(col_name))?;
    let col_meta = rg.column(col_idx);
    let stats = col_meta.statistics()?;
    use parquet::file::statistics::Statistics::*;
    let (min, max) = match stats {
        Int32(s) => (
            s.min_opt().map(|v| StatValue::Int(*v)),
            s.max_opt().map(|v| StatValue::Int(*v)),
        ),
        Int64(s) => (
            s.min_opt().map(|v| StatValue::Long(*v)),
            s.max_opt().map(|v| StatValue::Long(*v)),
        ),
        Float(s) => (
            s.min_opt().map(|v| StatValue::Double(*v as f64)),
            s.max_opt().map(|v| StatValue::Double(*v as f64)),
        ),
        Double(s) => (
            s.min_opt().map(|v| StatValue::Double(*v)),
            s.max_opt().map(|v| StatValue::Double(*v)),
        ),
        Boolean(s) => (
            s.min_opt().map(|v| StatValue::Bool(*v)),
            s.max_opt().map(|v| StatValue::Bool(*v)),
        ),
        ByteArray(s) => (
            s.min_opt().map(|v| StatValue::Str(String::from_utf8_lossy(v.data()).into())),
            s.max_opt().map(|v| StatValue::Str(String::from_utf8_lossy(v.data()).into())),
        ),
        _ => (None, None),
    };
    let null_count = stats.null_count_opt().unwrap_or(0) as i64;
    Some(ColMinMax { min, max, null_count, num_rows: rg.num_rows() })
}

fn literal_to_stat(expr: &FilterExpr) -> Option<StatValue> {
    match expr {
        FilterExpr::LiteralInt(v) => Some(StatValue::Int(*v)),
        FilterExpr::LiteralLong(v) | FilterExpr::LiteralTimestampMillis(v) => Some(StatValue::Long(*v)),
        FilterExpr::LiteralDouble(v) => Some(StatValue::Double(*v)),
        FilterExpr::LiteralBool(v) => Some(StatValue::Bool(*v)),
        FilterExpr::LiteralString(v) => Some(StatValue::Str(v.clone())),
        _ => None,
    }
}

/// Collects AND-connected equality predicates as `(column_name, candidate_values)` pairs.
///
/// Only traverses `And` nodes — `Or` nodes are not traversed because pruning requires that
/// the predicate must hold for all candidate rows (not just one branch).
/// Used for dictionary-based row group pruning: if none of the candidate values appear in a
/// row group's column dictionary, that row group can be skipped without reading any data.
pub fn collect_eq_predicates(expr: &FilterExpr) -> Vec<(String, Vec<StatValue>)> {
    let mut out = Vec::new();
    collect_eq_inner(expr, &mut out);
    out
}

fn collect_eq_inner(expr: &FilterExpr, out: &mut Vec<(String, Vec<StatValue>)>) {
    match expr {
        FilterExpr::Eq(left, right) => {
            if let (FilterExpr::Column(col), Some(val)) = (left.as_ref(), literal_to_stat(right)) {
                out.push((col.clone(), vec![val]));
            }
        }
        FilterExpr::InList(col_expr, items) => {
            if let FilterExpr::Column(col) = col_expr.as_ref() {
                let vals: Vec<StatValue> = items.iter().filter_map(literal_to_stat).collect();
                if !vals.is_empty() {
                    out.push((col.clone(), vals));
                }
            }
        }
        FilterExpr::And(a, b) => {
            collect_eq_inner(a, out);
            collect_eq_inner(b, out);
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Page-range-aware I/O: column index → RowSelector list
// ---------------------------------------------------------------------------

/// A single-column comparison predicate extracted for column-index page pruning.
#[derive(Clone, Debug)]
pub enum ColPred {
    Eq(StatValue),
    GtEq(StatValue),
    Gt(StatValue),
    LtEq(StatValue),
    Lt(StatValue),
}

pub struct IndexPred {
    pub col_name: String,
    pub pred: ColPred,
}

/// Collects AND-connected simple comparison predicates (col OP literal).
/// Or/Not branches are skipped conservatively.
pub fn collect_index_predicates(expr: &FilterExpr) -> Vec<IndexPred> {
    let mut out = Vec::new();
    collect_index_preds_inner(expr, &mut out);
    out
}

fn collect_index_preds_inner(expr: &FilterExpr, out: &mut Vec<IndexPred>) {
    match expr {
        FilterExpr::Eq(left, right) => {
            if let (FilterExpr::Column(col), Some(val)) = (left.as_ref(), literal_to_stat(right)) {
                out.push(IndexPred { col_name: col.clone(), pred: ColPred::Eq(val) });
            }
        }
        FilterExpr::GtEq(left, right) => {
            if let (FilterExpr::Column(col), Some(val)) = (left.as_ref(), literal_to_stat(right)) {
                out.push(IndexPred { col_name: col.clone(), pred: ColPred::GtEq(val) });
            }
        }
        FilterExpr::Gt(left, right) => {
            if let (FilterExpr::Column(col), Some(val)) = (left.as_ref(), literal_to_stat(right)) {
                out.push(IndexPred { col_name: col.clone(), pred: ColPred::Gt(val) });
            }
        }
        FilterExpr::LtEq(left, right) => {
            if let (FilterExpr::Column(col), Some(val)) = (left.as_ref(), literal_to_stat(right)) {
                out.push(IndexPred { col_name: col.clone(), pred: ColPred::LtEq(val) });
            }
        }
        FilterExpr::Lt(left, right) => {
            if let (FilterExpr::Column(col), Some(val)) = (left.as_ref(), literal_to_stat(right)) {
                out.push(IndexPred { col_name: col.clone(), pred: ColPred::Lt(val) });
            }
        }
        FilterExpr::And(a, b) => {
            collect_index_preds_inner(a, out);
            collect_index_preds_inner(b, out);
        }
        _ => {}
    }
}

/// Cross-type numeric comparison for page statistics.
///
/// `StatValue` derives `PartialOrd`, which compares discriminants first. That means
/// `Int(x) >= Long(y)` is always false regardless of the actual values, because
/// `Int` has a lower discriminant than `Long`. This arises whenever a predicate
/// literal (e.g. `LiteralLong` for a date) has a different numeric type than the
/// column's physical storage (e.g. INT32 EventDate → `StatValue::Int`). We promote
/// mixed integer/float pairs to f64 to get a meaningful comparison.
fn stat_cmp(a: &StatValue, b: &StatValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (StatValue::Int(x), StatValue::Int(y)) => x.partial_cmp(y),
        (StatValue::Long(x), StatValue::Long(y)) => x.partial_cmp(y),
        (StatValue::Double(x), StatValue::Double(y)) => x.partial_cmp(y),
        (StatValue::Bool(x), StatValue::Bool(y)) => x.partial_cmp(y),
        (StatValue::Str(x), StatValue::Str(y)) => x.partial_cmp(y),
        // Cross numeric types: promote to f64.
        (StatValue::Int(x), StatValue::Long(y)) => (*x as f64).partial_cmp(&(*y as f64)),
        (StatValue::Long(x), StatValue::Int(y)) => (*x as f64).partial_cmp(&(*y as f64)),
        (StatValue::Int(x), StatValue::Double(y)) => (*x as f64).partial_cmp(y),
        (StatValue::Double(x), StatValue::Int(y)) => x.partial_cmp(&(*y as f64)),
        (StatValue::Long(x), StatValue::Double(y)) => (*x as f64).partial_cmp(y),
        (StatValue::Double(x), StatValue::Long(y)) => x.partial_cmp(&(*y as f64)),
        _ => None,
    }
}

fn page_survives_col_pred(pmin: &Option<StatValue>, pmax: &Option<StatValue>, pred: &ColPred) -> bool {
    match pred {
        ColPred::GtEq(val) => pmax.as_ref().map_or(true, |max| stat_cmp(max, val).map_or(true, |o| o != std::cmp::Ordering::Less)),
        ColPred::Gt(val)   => pmax.as_ref().map_or(true, |max| stat_cmp(max, val).map_or(true, |o| o == std::cmp::Ordering::Greater)),
        ColPred::LtEq(val) => pmin.as_ref().map_or(true, |min| stat_cmp(min, val).map_or(true, |o| o != std::cmp::Ordering::Greater)),
        ColPred::Lt(val)   => pmin.as_ref().map_or(true, |min| stat_cmp(min, val).map_or(true, |o| o == std::cmp::Ordering::Less)),
        ColPred::Eq(val) => {
            let lo_ok = pmin.as_ref().map_or(true, |min| stat_cmp(val, min).map_or(true, |o| o != std::cmp::Ordering::Less));
            let hi_ok = pmax.as_ref().map_or(true, |max| stat_cmp(val, max).map_or(true, |o| o != std::cmp::Ordering::Greater));
            lo_ok && hi_ok
        }
    }
}

fn col_index_page_stats(col_idx: &ColumnIndexMetaData, page_i: usize) -> (Option<StatValue>, Option<StatValue>) {
    match col_idx {
        ColumnIndexMetaData::INT32(prim) => (
            prim.min_value(page_i).map(|v| StatValue::Int(*v)),
            prim.max_value(page_i).map(|v| StatValue::Int(*v)),
        ),
        ColumnIndexMetaData::INT64(prim) => (
            prim.min_value(page_i).map(|v| StatValue::Long(*v)),
            prim.max_value(page_i).map(|v| StatValue::Long(*v)),
        ),
        ColumnIndexMetaData::FLOAT(prim) => (
            prim.min_value(page_i).map(|v| StatValue::Double(*v as f64)),
            prim.max_value(page_i).map(|v| StatValue::Double(*v as f64)),
        ),
        ColumnIndexMetaData::DOUBLE(prim) => (
            prim.min_value(page_i).map(|v| StatValue::Double(*v)),
            prim.max_value(page_i).map(|v| StatValue::Double(*v)),
        ),
        ColumnIndexMetaData::BYTE_ARRAY(ba) => (
            ba.min_value(page_i).map(|v| StatValue::Str(String::from_utf8_lossy(v).into_owned())),
            ba.max_value(page_i).map(|v| StatValue::Str(String::from_utf8_lossy(v).into_owned())),
        ),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(ba) => (
            ba.min_value(page_i).map(|v| StatValue::Str(String::from_utf8_lossy(v).into_owned())),
            ba.max_value(page_i).map(|v| StatValue::Str(String::from_utf8_lossy(v).into_owned())),
        ),
        _ => (None, None),
    }
}

/// Computes page-level `RowSelector`s for one row group using column index statistics.
///
/// For each predicate column, builds a per-column selector list aligned to that column's
/// page boundaries (`select(n)` for surviving pages, `skip(n)` for prunable ones). The
/// returned list is the **AND-intersection** of all per-column lists, computed via
/// `RowSelection::intersection` which handles unaligned page boundaries correctly:
/// a row is selected iff **every** column's page covering that row could not be pruned.
///
/// Returns `None` when no pages can be skipped across any column. Callers concatenate
/// selectors across row groups to build a `RowSelection` for the whole chunk, then pass
/// it via `with_row_selection` so parquet-rs skips page I/O accordingly. A `RowFilter`
/// must still be applied for exact row-level correctness.
pub fn row_group_page_selection(
    expr: &FilterExpr,
    metadata: &ParquetMetaData,
    parquet_schema: &SchemaDescriptor,
    rg_idx: usize,
) -> Option<Vec<RowSelector>> {
    const TGT: &str = "esql_parquet_rs::pruning";

    let column_index = match metadata.column_index() {
        Some(ci) => ci,
        None => {
            log::trace!(target: TGT, "rg={} no column_index in metadata, skipping page pruning", rg_idx);
            return None;
        }
    };
    let offset_index = match metadata.offset_index() {
        Some(oi) => oi,
        None => {
            log::trace!(target: TGT, "rg={} no offset_index in metadata, skipping page pruning", rg_idx);
            return None;
        }
    };

    let rg_col_idx = column_index.get(rg_idx)?;
    let rg_off_idx = offset_index.get(rg_idx)?;

    let total_rows = metadata.row_group(rg_idx).num_rows() as usize;

    let preds = collect_index_predicates(expr);
    if preds.is_empty() {
        log::trace!(target: TGT, "rg={} no extractable index predicates from expr={}", rg_idx, expr);
        return None;
    }

    // Group predicate indices by lowercase column name.
    let mut col_groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (pi, ip) in preds.iter().enumerate() {
        let key = ip.col_name.to_ascii_lowercase();
        if let Some(entry) = col_groups.iter_mut().find(|(k, _)| *k == key) {
            entry.1.push(pi);
        } else {
            col_groups.push((key, vec![pi]));
        }
    }

    // Build a per-column page-survival selector list for every predicate column that
    // actually skips at least one page. Columns that can't prune anything are omitted
    // (they would contribute an all-`select` identity to the AND, so skipping them is
    // both correct and cheaper than running them through the intersection fold).
    //
    // Intersecting is done with `RowSelection::intersection`, which is row-aligned —
    // the result splits at the union of page boundaries from every contributing column.
    let mut accumulated: Option<RowSelection> = None;
    let mut contributing_cols: Vec<(String, usize, usize, usize)> = Vec::new(); // (col, pages_skipped, pages_total, rows_skipped)

    for (col_lower, pred_indices) in &col_groups {
        let col_idx = match (0..parquet_schema.num_columns())
            .find(|&i| parquet_schema.column(i).name().to_ascii_lowercase() == *col_lower)
        {
            Some(i) => i,
            None => {
                log::debug!(target: TGT, "rg={} predicate column [{}] not found in parquet schema", rg_idx, col_lower);
                continue;
            }
        };

        let col_index = match rg_col_idx.get(col_idx) { Some(c) => c, None => continue };
        let col_off   = match rg_off_idx.get(col_idx)  { Some(o) => o, None => continue };

        let locs = col_off.page_locations();
        if locs.is_empty() {
            log::trace!(target: TGT, "rg={} col={} empty page_locations", rg_idx, col_lower);
            continue;
        }
        let num_pages = locs.len();

        // Parquet spec invariants for a column chunk's offset index within a row group:
        //   1. The first page must start at row-group-relative row 0.
        //   2. Every page's first_row_index must lie inside [0, total_rows).
        //   3. The pages partition [0, total_rows): summing page row counts == total_rows.
        // parquet-mr / parquet-rs writers respect this, but we re-derive page_rows from
        // these offsets and fold the result into a `RowSelection` that parquet-rs applies
        // across ALL columns of the row group. A violation here would silently mis-skip
        // rows. `debug_assert!`s catch this in dev/CI; release builds log a warning and
        // skip page pruning for this column (correctness-safe — keeps all rows).
        let first_row_index = locs[0].first_row_index;
        let last_page_start = locs[num_pages - 1].first_row_index as usize;
        let first_ok = first_row_index == 0;
        let last_in_range = last_page_start < total_rows || total_rows == 0;
        debug_assert!(
            first_ok,
            "rg={} col={}: first page first_row_index={} expected 0 (offset-index spec violation)",
            rg_idx, col_lower, first_row_index
        );
        debug_assert!(
            last_in_range,
            "rg={} col={}: last page first_row_index={} >= total_rows={} (offset-index spec violation)",
            rg_idx, col_lower, last_page_start, total_rows
        );
        if !first_ok || !last_in_range {
            log::warn!(
                target: TGT,
                "rg={} col={}: malformed offset index (first_row_index={}, last_page_start={}, total_rows={}); \
                 skipping page pruning for this column to avoid mis-skipping rows",
                rg_idx, col_lower, first_row_index, last_page_start, total_rows
            );
            continue;
        }

        let mut selectors: Vec<RowSelector> = Vec::with_capacity(num_pages);
        let mut skipped = 0usize;
        let mut skipped_pages = 0usize;
        let mut emitted_rows = 0usize;

        for page_i in 0..num_pages {
            let row_start = locs[page_i].first_row_index as usize;
            let row_end = if page_i + 1 < num_pages {
                locs[page_i + 1].first_row_index as usize
            } else {
                total_rows
            };
            let page_rows = row_end.saturating_sub(row_start);
            emitted_rows += page_rows;

            let (pmin, pmax) = col_index_page_stats(col_index, page_i);
            let survives = pred_indices.iter().all(|&pi| {
                page_survives_col_pred(&pmin, &pmax, &preds[pi].pred)
            });

            if survives {
                selectors.push(RowSelector::select(page_rows));
            } else {
                selectors.push(RowSelector::skip(page_rows));
                skipped += page_rows;
                skipped_pages += 1;
                log::trace!(
                    target: TGT,
                    "rg={} col={} page={} rows={} SKIP (min={:?} max={:?} preds=[{}])",
                    rg_idx, col_lower, page_i, page_rows, pmin, pmax,
                    pred_indices.iter().map(|&pi| format!("{:?}", preds[pi].pred)).collect::<Vec<_>>().join(",")
                );
            }
        }

        // Invariant 3: pages must partition [0, total_rows). Per-page row counts
        // are derived from successive first_row_index values + total_rows for the
        // last page, so any mismatch here means either monotonicity is broken or
        // the offsets straddle total_rows.
        debug_assert_eq!(
            emitted_rows, total_rows,
            "rg={} col={}: page partition sum {} != total_rows {} (offset-index spec violation)",
            rg_idx, col_lower, emitted_rows, total_rows
        );
        if emitted_rows != total_rows {
            log::warn!(
                target: TGT,
                "rg={} col={}: page partition sum {} != total_rows {}; skipping page pruning for this column",
                rg_idx, col_lower, emitted_rows, total_rows
            );
            continue;
        }

        log::debug!(
            target: TGT,
            "rg={} col={} pages={} pages_skipped={} rows={} rows_skipped={}",
            rg_idx, col_lower, num_pages, skipped_pages, total_rows, skipped
        );

        if skipped == 0 {
            // Identity contribution to the AND: drop it.
            continue;
        }

        contributing_cols.push((col_lower.clone(), skipped_pages, num_pages, skipped));
        let this_sel = RowSelection::from(selectors);
        accumulated = Some(match accumulated {
            None => this_sel,
            Some(prev) => prev.intersection(&this_sel),
        });
    }

    let merged = match accumulated {
        Some(s) => s,
        None => {
            log::debug!(
                target: TGT,
                "rg={} no column-index pruning possible (rows={}, {} candidate cols)",
                rg_idx, total_rows, col_groups.len()
            );
            return None;
        }
    };

    let merged_selectors: Vec<RowSelector> = merged.into();
    let merged_skipped: usize = merged_selectors.iter().filter(|s| s.skip).map(|s| s.row_count).sum();

    if log::log_enabled!(log::Level::Debug) {
        log::debug!(
            target: TGT,
            "rg={} INTERSECTED {} contributing col(s) [{}]: rows_skipped={}/{} ({:.1}%) selectors={}",
            rg_idx,
            contributing_cols.len(),
            contributing_cols.iter()
                .map(|(c, ps, pt, rs)| format!("{}={}/{}p,{}r", c, ps, pt, rs))
                .collect::<Vec<_>>().join(","),
            merged_skipped, total_rows,
            if total_rows > 0 { 100.0 * merged_skipped as f64 / total_rows as f64 } else { 0.0 },
            merged_selectors.len(),
        );
    }

    Some(merged_selectors)
}

/// Returns false if the row group can definitely be skipped (no matching rows).
pub fn row_group_matches(
    expr: &FilterExpr,
    rg: &RowGroupMetaData,
    schema: &SchemaDescriptor,
) -> bool {
    match expr {
        FilterExpr::Eq(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let (Some(min), Some(max)) = (&cs.min, &cs.max) {
                            let lo = stat_cmp(&lit, min).map_or(true, |o| o != std::cmp::Ordering::Less);
                            let hi = stat_cmp(&lit, max).map_or(true, |o| o != std::cmp::Ordering::Greater);
                            return lo && hi;
                        }
                    }
                }
            }
            true
        }
        FilterExpr::Lt(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let Some(min) = &cs.min {
                            return stat_cmp(min, &lit).map_or(true, |o| o == std::cmp::Ordering::Less);
                        }
                    }
                }
            }
            true
        }
        FilterExpr::LtEq(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let Some(min) = &cs.min {
                            return stat_cmp(min, &lit).map_or(true, |o| o != std::cmp::Ordering::Greater);
                        }
                    }
                }
            }
            true
        }
        FilterExpr::Gt(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let Some(max) = &cs.max {
                            return stat_cmp(max, &lit).map_or(true, |o| o == std::cmp::Ordering::Greater);
                        }
                    }
                }
            }
            true
        }
        FilterExpr::GtEq(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let Some(max) = &cs.max {
                            return stat_cmp(max, &lit).map_or(true, |o| o != std::cmp::Ordering::Less);
                        }
                    }
                }
            }
            true
        }
        FilterExpr::IsNull(inner) => {
            if let FilterExpr::Column(col) = inner.as_ref() {
                if let Some(cs) = get_col_stats(rg, col, schema) {
                    return cs.null_count > 0;
                }
            }
            true
        }
        FilterExpr::IsNotNull(inner) => {
            if let FilterExpr::Column(col) = inner.as_ref() {
                if let Some(cs) = get_col_stats(rg, col, schema) {
                    return cs.null_count < cs.num_rows;
                }
            }
            true
        }
        FilterExpr::NotEq(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let (Some(min), Some(max)) = (&cs.min, &cs.max) {
                            if stat_cmp(min, max) == Some(std::cmp::Ordering::Equal)
                                && stat_cmp(min, &lit) == Some(std::cmp::Ordering::Equal)
                            {
                                return false;
                            }
                        }
                    }
                }
            }
            true
        }
        FilterExpr::InList(col_expr, items) => {
            if let FilterExpr::Column(col) = col_expr.as_ref() {
                if let Some(cs) = get_col_stats(rg, col, schema) {
                    if let (Some(min), Some(max)) = (&cs.min, &cs.max) {
                        let any_in_range = items.iter().any(|item| {
                            if let Some(lit) = literal_to_stat(item) {
                                let lo = stat_cmp(&lit, min).map_or(true, |o| o != std::cmp::Ordering::Less);
                                let hi = stat_cmp(&lit, max).map_or(true, |o| o != std::cmp::Ordering::Greater);
                                lo && hi
                            } else {
                                true
                            }
                        });
                        return any_in_range;
                    }
                }
            }
            true
        }
        FilterExpr::And(a, b) => {
            row_group_matches(a, rg, schema) && row_group_matches(b, rg, schema)
        }
        FilterExpr::Or(a, b) => {
            row_group_matches(a, rg, schema) || row_group_matches(b, rg, schema)
        }
        FilterExpr::Not(inner) => {
            // Conservative: can't generally negate stat-based pruning
            let _ = inner;
            true
        }
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Arrow row-level filter: convert FilterExpr to ArrowPredicateFn
// ---------------------------------------------------------------------------

/// Collects all column names referenced in the filter expression.
pub fn collect_columns(expr: &FilterExpr) -> Vec<String> {
    let mut cols = Vec::new();
    collect_columns_inner(expr, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn collect_columns_inner(expr: &FilterExpr, cols: &mut Vec<String>) {
    match expr {
        FilterExpr::Column(name) => cols.push(name.clone()),
        FilterExpr::Eq(a, b) | FilterExpr::NotEq(a, b) | FilterExpr::Gt(a, b)
        | FilterExpr::GtEq(a, b) | FilterExpr::Lt(a, b) | FilterExpr::LtEq(a, b)
        | FilterExpr::And(a, b) | FilterExpr::Or(a, b) => {
            collect_columns_inner(a, cols);
            collect_columns_inner(b, cols);
        }
        FilterExpr::Not(inner) | FilterExpr::IsNull(inner) | FilterExpr::IsNotNull(inner) => {
            collect_columns_inner(inner, cols);
        }
        FilterExpr::InList(e, items) => {
            collect_columns_inner(e, cols);
            for item in items {
                collect_columns_inner(item, cols);
            }
        }
        FilterExpr::Like(e, _, _) | FilterExpr::NotLike(e, _, _) => collect_columns_inner(e, cols),
        FilterExpr::StartsWith(e, _, _) => collect_columns_inner(e, cols),
        _ => {}
    }
}

/// Builds a `RowFilter` from a `FilterExpr` for use with `ParquetRecordBatchReaderBuilder`.
pub fn build_row_filter(
    expr: &FilterExpr,
    _schema: SchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> RowFilter {
    let cols = collect_columns(expr);
    let col_indices: Vec<usize> = cols.iter()
        .filter_map(|name| {
            (0..parquet_schema.num_columns())
                .find(|&i| parquet_schema.column(i).name().eq_ignore_ascii_case(name))
        })
        .collect();

    let projection = ProjectionMask::leaves(parquet_schema, col_indices);
    let expr_clone = expr.clone();
    let pred = ArrowPredicateFn::new(projection, move |batch: RecordBatch| {
        evaluate_filter(&expr_clone, &batch)
    });
    RowFilter::new(vec![Box::new(pred)])
}

fn evaluate_filter(expr: &FilterExpr, batch: &RecordBatch) -> arrow::error::Result<BooleanArray> {
    let num_rows = batch.num_rows();
    match expr {
        FilterExpr::Eq(left, right) => eval_comparison(left, right, batch, cmp::eq),
        FilterExpr::NotEq(left, right) => eval_comparison(left, right, batch, cmp::neq),
        FilterExpr::Lt(left, right) => eval_comparison(left, right, batch, cmp::lt),
        FilterExpr::LtEq(left, right) => eval_comparison(left, right, batch, cmp::lt_eq),
        FilterExpr::Gt(left, right) => eval_comparison(left, right, batch, cmp::gt),
        FilterExpr::GtEq(left, right) => eval_comparison(left, right, batch, cmp::gt_eq),
        FilterExpr::And(a, b) => {
            let la = evaluate_filter(a, batch)?;
            let lb = evaluate_filter(b, batch)?;
            Ok(arrow::compute::kernels::boolean::and(&la, &lb)?)
        }
        FilterExpr::Or(a, b) => {
            let la = evaluate_filter(a, batch)?;
            let lb = evaluate_filter(b, batch)?;
            Ok(arrow::compute::kernels::boolean::or(&la, &lb)?)
        }
        FilterExpr::Not(inner) => {
            let inner_result = evaluate_filter(inner, batch)?;
            Ok(arrow::compute::kernels::boolean::not(&inner_result)?)
        }
        FilterExpr::IsNull(inner) => {
            if let FilterExpr::Column(name) = inner.as_ref() {
                if let Some(col) = find_column(batch, name) {
                    return Ok(arrow::compute::is_null(col.as_ref())?);
                }
            }
            Ok(BooleanArray::from(vec![false; num_rows]))
        }
        FilterExpr::IsNotNull(inner) => {
            if let FilterExpr::Column(name) = inner.as_ref() {
                if let Some(col) = find_column(batch, name) {
                    return Ok(arrow::compute::is_not_null(col.as_ref())?);
                }
            }
            Ok(BooleanArray::from(vec![false; num_rows]))
        }
        FilterExpr::InList(col_expr, items) => {
            if let FilterExpr::Column(name) = col_expr.as_ref() {
                if let Some(col) = find_column(batch, name) {
                    return eval_in_list(col, items, num_rows);
                }
            }
            Ok(BooleanArray::from(vec![false; num_rows]))
        }
        FilterExpr::Like(col_expr, pattern, ci) => eval_like(col_expr, pattern, batch, num_rows, false, *ci),
        FilterExpr::NotLike(col_expr, pattern, ci) => eval_like(col_expr, pattern, batch, num_rows, true, *ci),
        FilterExpr::StartsWith(col_expr, prefix, upper) => {
            eval_starts_with(col_expr, prefix, upper.as_deref(), batch, num_rows)
        }
        _ => Ok(BooleanArray::from(vec![false; num_rows])),
    }
}

fn find_column(batch: &RecordBatch, name: &str) -> Option<Arc<dyn Array>> {
    let lower = name.to_ascii_lowercase();
    batch.schema().fields().iter().enumerate()
        .find(|(_, f)| f.name().to_ascii_lowercase() == lower)
        .map(|(i, _)| batch.column(i).clone())
}

/// Build a scalar array matching the column's data type from a FilterExpr literal.
/// Returns None if the literal type is incompatible with the column type.
fn make_scalar(col: &dyn Array, lit: &FilterExpr) -> Option<ArrayRef> {
    let dt = col.data_type();
    match lit {
        FilterExpr::LiteralInt(v) => match dt {
            DataType::Int32 => Some(Arc::new(Int32Array::from(vec![*v]))),
            DataType::Int64 => Some(Arc::new(Int64Array::from(vec![*v as i64]))),
            DataType::Int16 => Some(Arc::new(Int16Array::from(vec![*v as i16]))),
            DataType::Int8 => Some(Arc::new(Int8Array::from(vec![*v as i8]))),
            DataType::UInt64 => Some(Arc::new(UInt64Array::from(vec![*v as u64]))),
            DataType::UInt32 => Some(Arc::new(UInt32Array::from(vec![*v as u32]))),
            DataType::UInt16 => Some(Arc::new(UInt16Array::from(vec![*v as u16]))),
            DataType::UInt8 => Some(Arc::new(UInt8Array::from(vec![*v as u8]))),
            _ => None,
        },
        FilterExpr::LiteralLong(v) | FilterExpr::LiteralTimestampMillis(v) => match dt {
            DataType::Int64 => Some(Arc::new(Int64Array::from(vec![*v]))),
            DataType::Int32 => Some(Arc::new(Int32Array::from(vec![*v as i32]))),
            DataType::Date32 => Some(Arc::new(Date32Array::from(vec![*v as i32]))),
            DataType::Date64 => Some(Arc::new(Date64Array::from(vec![*v]))),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, tz) => {
                Some(Arc::new(TimestampMillisecondArray::from(vec![*v]).with_timezone_opt(tz.clone())))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz) => {
                Some(Arc::new(TimestampMicrosecondArray::from(vec![*v * 1000]).with_timezone_opt(tz.clone())))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, tz) => {
                Some(Arc::new(TimestampNanosecondArray::from(vec![*v * 1_000_000]).with_timezone_opt(tz.clone())))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, tz) => {
                Some(Arc::new(TimestampSecondArray::from(vec![*v / 1000]).with_timezone_opt(tz.clone())))
            }
            DataType::UInt64 => Some(Arc::new(UInt64Array::from(vec![*v as u64]))),
            DataType::UInt32 => Some(Arc::new(UInt32Array::from(vec![*v as u32]))),
            _ => None,
        },
        FilterExpr::LiteralDouble(v) => match dt {
            DataType::Float64 => Some(Arc::new(Float64Array::from(vec![*v]))),
            DataType::Float32 => Some(Arc::new(Float32Array::from(vec![*v as f32]))),
            _ => None,
        },
        FilterExpr::LiteralBool(v) => match dt {
            DataType::Boolean => Some(Arc::new(BooleanArray::from(vec![*v]))),
            _ => None,
        },
        FilterExpr::LiteralString(v) => match dt {
            DataType::Utf8 => Some(Arc::new(StringArray::from(vec![v.as_str()]))),
            DataType::Binary => Some(Arc::new(BinaryArray::from(vec![v.as_bytes()]))),
            _ => None,
        },
        _ => None,
    }
}

type CmpFn = fn(&dyn arrow::array::Datum, &dyn arrow::array::Datum) -> arrow::error::Result<BooleanArray>;

fn eval_comparison(
    left: &FilterExpr,
    right: &FilterExpr,
    batch: &RecordBatch,
    cmp_fn: CmpFn,
) -> arrow::error::Result<BooleanArray> {
    let num_rows = batch.num_rows();
    if let FilterExpr::Column(name) = left {
        if let Some(col) = find_column(batch, name) {
            if let Some(scalar_arr) = make_scalar(col.as_ref(), right) {
                let scalar = Scalar::new(scalar_arr);
                return cmp_fn(&col, &scalar);
            }
        }
    }
    Ok(BooleanArray::from(vec![false; num_rows]))
}

fn eval_in_list(
    col: Arc<dyn Array>,
    items: &[FilterExpr],
    num_rows: usize,
) -> arrow::error::Result<BooleanArray> {
    let mut combined: Option<BooleanArray> = None;
    for item in items {
        if let Some(scalar_arr) = make_scalar(col.as_ref(), item) {
            let scalar = Scalar::new(scalar_arr);
            let eq_result = cmp::eq(&col, &scalar)?;
            combined = Some(match combined {
                Some(prev) => arrow::compute::kernels::boolean::or(&prev, &eq_result)?,
                None => eq_result,
            });
        }
    }
    Ok(combined.unwrap_or_else(|| BooleanArray::from(vec![false; num_rows])))
}

fn eval_like(
    col_expr: &FilterExpr,
    pattern: &str,
    batch: &RecordBatch,
    num_rows: usize,
    negate: bool,
    case_insensitive: bool,
) -> arrow::error::Result<BooleanArray> {
    if let FilterExpr::Column(name) = col_expr {
        if let Some(col) = find_column(batch, name) {
            let str_col = as_string_array(&col);
            if let Some(str_arr) = str_col.as_ref().or_else(|| col.as_any().downcast_ref::<StringArray>()) {
                let pattern_scalar = StringArray::new_scalar(pattern);
                // ESQL's case-insensitive WildcardLike (built via Lucene's RegExp.CASE_INSENSITIVE)
                // is ASCII-only, while arrow's ilike/nilike apply Unicode case folding. The two
                // agree on every common upper/lower pair (including extended Latin like 'ü'/'Ü');
                // they only diverge on a handful of Unicode ligatures (e.g. 'ﬀ' vs "FF", 'ß' vs "SS").
                // For the supported text in ESQL this difference is not user-visible.
                let result = match (negate, case_insensitive) {
                    (false, false) => arrow::compute::kernels::comparison::like(str_arr, &pattern_scalar)?,
                    (false, true) => arrow::compute::kernels::comparison::ilike(str_arr, &pattern_scalar)?,
                    (true, false) => arrow::compute::kernels::comparison::nlike(str_arr, &pattern_scalar)?,
                    (true, true) => arrow::compute::kernels::comparison::nilike(str_arr, &pattern_scalar)?,
                };
                return Ok(result);
            }
        }
    }
    Ok(BooleanArray::from(vec![false; num_rows]))
}

/// Try to interpret a column as a StringArray, converting from BinaryArray if needed.
fn as_string_array(col: &Arc<dyn Array>) -> Option<StringArray> {
    if col.as_any().downcast_ref::<StringArray>().is_some() {
        return None;
    }
    if let Some(bin_arr) = col.as_any().downcast_ref::<BinaryArray>() {
        let strs: Vec<Option<&str>> = (0..bin_arr.len())
            .map(|i| {
                if bin_arr.is_null(i) {
                    None
                } else {
                    std::str::from_utf8(bin_arr.value(i)).ok()
                }
            })
            .collect();
        return Some(StringArray::from(strs));
    }
    None
}

fn eval_starts_with(
    col_expr: &FilterExpr,
    prefix: &str,
    upper: Option<&str>,
    batch: &RecordBatch,
    num_rows: usize,
) -> arrow::error::Result<BooleanArray> {
    if let FilterExpr::Column(name) = col_expr {
        if let Some(col) = find_column(batch, name) {
            let str_arr = col.as_any().downcast_ref::<StringArray>();
            let bin_arr = col.as_any().downcast_ref::<BinaryArray>();
            let mut results = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                if col.is_null(i) {
                    results.push(Some(false));
                } else {
                    let val: Option<&str> = str_arr.map(|a| a.value(i))
                        .or_else(|| bin_arr.and_then(|a| std::str::from_utf8(a.value(i)).ok()));
                    let m = val.map(|v| v >= prefix && upper.map_or(true, |u| v < u)).unwrap_or(false);
                    results.push(Some(m));
                }
            }
            return Ok(BooleanArray::from(results));
        }
    }
    Ok(BooleanArray::from(vec![false; num_rows]))
}


// ---------------------------------------------------------------------------
// JNI entry points for building FilterExpr trees
// ---------------------------------------------------------------------------
//
// # Handle ownership contract
//
// Every `create*` JNI method below either:
//
// 1. Returns a fresh handle (jlong) that the Java caller owns and must
//    eventually pass to `freeExpr` (or to another `create*` call as an input).
// 2. Throws a Java exception (RuntimeException via the `ThrowRuntimeExAndDefault`
//    policy) and returns 0.
//
// All input handles passed to a `create*` call MUST come from a previous
// `create*` call and must not have been freed or passed to any other `create*`
// call yet (handles are linear / single-use).
//
// **Consumption semantics on failure** are the source of subtle bugs, so the
// implementations below follow a strict pattern:
//
//   1. Validate all input handles (non-zero) and extract any fallible JNI
//      arguments (string conversions, array reads) BEFORE calling
//      `unbox_expr` on any input handle.
//   2. After all fallible operations have succeeded, call `unbox_expr` on
//      every input handle in sequence (no `?` between them, no panic-prone
//      intermediates other than `Box::new` allocation).
//   3. Construct the result via `box_expr`.
//
// Combined with the `catch_unwind` that `EnvUnowned::with_env` performs, this
// gives the Java caller a single, simple rule:
//
//   **After passing a handle to a `create*` call, the caller MUST NOT free
//   it, regardless of whether the call returned successfully or threw a
//   Java exception.** Either the native side consumed it (success, or
//   failure after the commit point) or the input was never owned by the
//   native side (validation failed before the commit point — but the only
//   validation that can fail is `handle == 0`, in which case `freeExpr(0)`
//   is a no-op anyway).
//
// If/when this layer migrates from JNI to a different FFI mechanism (Project
// Panama, plain `extern "C"` with out-pointers, etc.), this contract is
// FFI-agnostic and should be preserved on the new boundary.

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createColumn(
    mut env: EnvUnowned,
    _class: JClass,
    name: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let col_name = name.try_to_string(env)?;
        Ok(box_expr(FilterExpr::Column(col_name)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLiteralInt(
    mut env: EnvUnowned, _class: JClass, value: jni::sys::jint,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> { Ok(box_expr(FilterExpr::LiteralInt(value))) })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLiteralLong(
    mut env: EnvUnowned, _class: JClass, value: jni::sys::jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> { Ok(box_expr(FilterExpr::LiteralLong(value))) })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLiteralTimestampMillis(
    mut env: EnvUnowned, _class: JClass, value: jni::sys::jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> { Ok(box_expr(FilterExpr::LiteralTimestampMillis(value))) })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLiteralDouble(
    mut env: EnvUnowned, _class: JClass, value: jni::sys::jdouble,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> { Ok(box_expr(FilterExpr::LiteralDouble(value))) })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLiteralBool(
    mut env: EnvUnowned, _class: JClass, value: jni::sys::jboolean,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> { Ok(box_expr(FilterExpr::LiteralBool(value != jni::sys::JNI_FALSE as jni::sys::jboolean))) })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLiteralString(
    mut env: EnvUnowned, _class: JClass, value: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let s = value.try_to_string(env)?;
        Ok(box_expr(FilterExpr::LiteralString(s)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

macro_rules! binary_filter_op {
    ($jni_name:ident, $variant:ident) => {
        #[unsafe(no_mangle)]
        pub extern "system" fn $jni_name(
            mut env: EnvUnowned, _class: JClass, left: jlong, right: jlong,
        ) -> jlong {
            env.with_env(|_env| -> JniResult<jlong> {
                if left == 0 || right == 0 {
                    return Err(jni_err("null input handle to binary filter op"));
                }
                // Commit point: from here both inputs are consumed (success
                // or panic-unwind drop). See the contract comment above.
                let l = unsafe { *unbox_expr(left) };
                let r = unsafe { *unbox_expr(right) };
                Ok(box_expr(FilterExpr::$variant(Box::new(l), Box::new(r))))
            })
            .resolve::<ThrowRuntimeExAndDefault>()
        }
    };
}

binary_filter_op!(Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createEquals, Eq);
binary_filter_op!(Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createNotEquals, NotEq);
binary_filter_op!(Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createGreaterThan, Gt);
binary_filter_op!(Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createGreaterThanOrEqual, GtEq);
binary_filter_op!(Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLessThan, Lt);
binary_filter_op!(Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLessThanOrEqual, LtEq);
binary_filter_op!(Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createAnd, And);
binary_filter_op!(Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createOr, Or);

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createNot(
    mut env: EnvUnowned, _class: JClass, child: jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        if child == 0 {
            return Err(jni_err("null child handle to createNot"));
        }
        let c = unsafe { *unbox_expr(child) };
        Ok(box_expr(FilterExpr::Not(Box::new(c))))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createIsNull(
    mut env: EnvUnowned, _class: JClass, child: jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        if child == 0 {
            return Err(jni_err("null child handle to createIsNull"));
        }
        let c = unsafe { *unbox_expr(child) };
        Ok(box_expr(FilterExpr::IsNull(Box::new(c))))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createIsNotNull(
    mut env: EnvUnowned, _class: JClass, child: jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        if child == 0 {
            return Err(jni_err("null child handle to createIsNotNull"));
        }
        let c = unsafe { *unbox_expr(child) };
        Ok(box_expr(FilterExpr::IsNotNull(Box::new(c))))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createInList(
    mut env: EnvUnowned, _class: JClass, expr_handle: jlong, list_handles: JLongArray,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        // Validate and extract everything fallible BEFORE consuming any handle,
        // so that on failure neither expr_handle nor list_handles are consumed.
        if expr_handle == 0 {
            return Err(jni_err("null expr_handle to createInList"));
        }
        let len = list_handles.len(env)?;
        let mut handles = vec![0i64; len];
        list_handles.get_region(env, 0, &mut handles)?;
        for &h in &handles {
            if h == 0 {
                return Err(jni_err("null handle in createInList list"));
            }
        }
        // Commit point: from here every input handle is consumed.
        let e = unsafe { *unbox_expr(expr_handle) };
        let mut items = Vec::with_capacity(len);
        for h in handles {
            items.push(unsafe { *unbox_expr(h) });
        }
        Ok(box_expr(FilterExpr::InList(Box::new(e), items)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createStartsWith(
    mut env: EnvUnowned, _class: JClass, col_handle: jlong, prefix: JString, upper_bound: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        if col_handle == 0 {
            return Err(jni_err("null col_handle to createStartsWith"));
        }
        // Resolve all fallible JNI conversions BEFORE consuming col_handle.
        let prefix_str = prefix.try_to_string(env)?;
        let upper_opt = jstring_to_opt_string(&upper_bound, env)?;
        let c = unsafe { *unbox_expr(col_handle) };
        Ok(box_expr(FilterExpr::StartsWith(Box::new(c), prefix_str, upper_opt)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createLike(
    mut env: EnvUnowned, _class: JClass, col_handle: jlong, pattern: JString, case_insensitive: jni::sys::jboolean,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        if col_handle == 0 {
            return Err(jni_err("null col_handle to createLike"));
        }
        let pat = pattern.try_to_string(env)?;
        let ci = case_insensitive != jni::sys::JNI_FALSE as jni::sys::jboolean;
        let c = unsafe { *unbox_expr(col_handle) };
        Ok(box_expr(FilterExpr::Like(Box::new(c), pat, ci)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_createNotLike(
    mut env: EnvUnowned, _class: JClass, col_handle: jlong, pattern: JString, case_insensitive: jni::sys::jboolean,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        if col_handle == 0 {
            return Err(jni_err("null col_handle to createNotLike"));
        }
        let pat = pattern.try_to_string(env)?;
        let ci = case_insensitive != jni::sys::JNI_FALSE as jni::sys::jboolean;
        let c = unsafe { *unbox_expr(col_handle) };
        Ok(box_expr(FilterExpr::NotLike(Box::new(c), pat, ci)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_freeExpr(
    mut env: EnvUnowned, _class: JClass, handle: jlong,
) {
    env.with_env(|_env| -> JniResult<()> {
        if handle != 0 {
            unsafe { unbox_expr(handle) };
        }
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_describeExpr(
    mut env: EnvUnowned, _class: JClass, handle: jlong,
) -> jni::sys::jobject {
    env.with_env(|env| -> JniResult<jni::sys::jobject> {
        let expr = unsafe { &*(handle as *const FilterExpr) };
        let desc = format!("{expr}");
        let jstr = env.new_string(&desc)?;
        Ok(jstr.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
