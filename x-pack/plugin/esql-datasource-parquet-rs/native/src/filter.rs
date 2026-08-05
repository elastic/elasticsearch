use super::jni_utils::*;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, DictionaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, Scalar,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::kernels::cmp;
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;
use jni::EnvUnowned;
use jni::errors::{Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JLongArray, JString};
use jni::sys::jlong;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
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
    /// Skip pages where every value equals the rejected literal (min == max == v).
    NotEq(StatValue),
    /// Skip pages whose entire [min, max] range contains none of the listed values.
    InList(Vec<StatValue>),
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
        FilterExpr::NotEq(left, right) => {
            if let (FilterExpr::Column(col), Some(val)) = (left.as_ref(), literal_to_stat(right)) {
                out.push(IndexPred { col_name: col.clone(), pred: ColPred::NotEq(val) });
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
        FilterExpr::InList(col_expr, items) => {
            if let FilterExpr::Column(col) = col_expr.as_ref() {
                let vals: Vec<StatValue> = items.iter().filter_map(literal_to_stat).collect();
                if vals.is_empty() == false {
                    out.push(IndexPred { col_name: col.clone(), pred: ColPred::InList(vals) });
                }
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
        ColPred::NotEq(val) => {
            // Can prune only when every value in the page equals val (min == max == val).
            match (pmin.as_ref(), pmax.as_ref()) {
                (Some(min), Some(max)) => {
                    let min_eq = stat_cmp(min, val).map_or(false, |o| o == std::cmp::Ordering::Equal);
                    let max_eq = stat_cmp(max, val).map_or(false, |o| o == std::cmp::Ordering::Equal);
                    !(min_eq && max_eq)
                }
                _ => true,
            }
        }
        ColPred::InList(vals) => {
            // Can prune when no list value falls within [page_min, page_max].
            match (pmin.as_ref(), pmax.as_ref()) {
                (Some(min), Some(max)) => vals.iter().any(|v| {
                    let gte_min = stat_cmp(v, min).map_or(true, |o| o != std::cmp::Ordering::Less);
                    let lte_max = stat_cmp(v, max).map_or(true, |o| o != std::cmp::Ordering::Greater);
                    gte_min && lte_max
                }),
                _ => true,
            }
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

/// Returns `true` when row-group statistics prove that **every** row in the group
/// satisfies the predicate — no RowFilter evaluation or filter-column I/O is needed.
///
/// This is the complement of [`row_group_matches`]: where that function returns `false`
/// to eliminate row groups that cannot contain any matching rows, this function returns
/// `true` to skip the runtime filter for row groups where every row is guaranteed to pass.
///
/// Conservative: returns `false` for anything that cannot be determined from min/max stats
/// alone (e.g. `Like`, `Not`, `InList`). `And` requires both branches to trivially pass;
/// `Or` requires at least one branch to trivially pass.
pub fn row_group_trivially_passes(
    expr: &FilterExpr,
    rg: &RowGroupMetaData,
    schema: &SchemaDescriptor,
) -> bool {
    match expr {
        FilterExpr::NotEq(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let (Some(min), Some(max)) = (&cs.min, &cs.max) {
                            // All values != lit when the entire range lies strictly below or above lit.
                            let all_below = stat_cmp(max, &lit).map_or(false, |o| o == std::cmp::Ordering::Less);
                            let all_above = stat_cmp(min, &lit).map_or(false, |o| o == std::cmp::Ordering::Greater);
                            return all_below || all_above;
                        }
                    }
                }
            }
            false
        }
        FilterExpr::Gt(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let Some(min) = &cs.min {
                            return stat_cmp(min, &lit).map_or(false, |o| o == std::cmp::Ordering::Greater);
                        }
                    }
                }
            }
            false
        }
        FilterExpr::GtEq(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let Some(min) = &cs.min {
                            return stat_cmp(min, &lit).map_or(false, |o| o != std::cmp::Ordering::Less);
                        }
                    }
                }
            }
            false
        }
        FilterExpr::Lt(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let Some(max) = &cs.max {
                            return stat_cmp(max, &lit).map_or(false, |o| o == std::cmp::Ordering::Less);
                        }
                    }
                }
            }
            false
        }
        FilterExpr::LtEq(left, right) => {
            if let FilterExpr::Column(col) = left.as_ref() {
                if let Some(lit) = literal_to_stat(right) {
                    if let Some(cs) = get_col_stats(rg, col, schema) {
                        if let Some(max) = &cs.max {
                            return stat_cmp(max, &lit).map_or(false, |o| o != std::cmp::Ordering::Greater);
                        }
                    }
                }
            }
            false
        }
        FilterExpr::IsNotNull(inner) => {
            if let FilterExpr::Column(col) = inner.as_ref() {
                if let Some(cs) = get_col_stats(rg, col, schema) {
                    return cs.null_count == 0;
                }
            }
            false
        }
        FilterExpr::And(a, b) => {
            row_group_trivially_passes(a, rg, schema) && row_group_trivially_passes(b, rg, schema)
        }
        FilterExpr::Or(a, b) => {
            row_group_trivially_passes(a, rg, schema) || row_group_trivially_passes(b, rg, schema)
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Arrow row-level filter: convert FilterExpr to ArrowPredicateFn
// ---------------------------------------------------------------------------
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

pub fn evaluate_filter(expr: &FilterExpr, batch: &RecordBatch) -> arrow::error::Result<BooleanArray> {
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
                    return eval_in_list(col, items);
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

/// Runs `f` against the dictionary's small `values()` array when `col` is a
/// `DictionaryArray<Int32Type>`, then expands the per-value mask back to a per-row
/// mask via `take(mask, keys)`. Null keys propagate to nulls in the result, which
/// `arrow::compute::filter_record_batch` treats as `false` — matching the row-level
/// null semantics every evaluator already relies on.
///
/// For non-dictionary columns (or dictionaries with key types other than Int32 — not
/// emitted by the dict-promotion path), the closure is invoked directly on `col`.
fn eval_per_element<F>(col: &dyn Array, f: F) -> arrow::error::Result<BooleanArray>
where
    F: FnOnce(&dyn Array) -> arrow::error::Result<BooleanArray>,
{
    if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values_mask = f(dict.values().as_ref())?;
        let expanded = arrow::compute::take(&values_mask, dict.keys(), None)?;
        return Ok(expanded
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("take on BooleanArray returns BooleanArray")
            .clone());
    }
    f(col)
}

/// Rebuild a `DictionaryArray<Int32>` so its `values()` contains only entries actually
/// referenced by `keys()`, with keys remapped to the new positions.
///
/// `arrow::compute::filter` (and `take` with row selection) on a `DictionaryArray` keeps the
/// original dictionary intact and only filters the keys, leaving the dict sparse — values
/// that no key references are still present.
///
/// On the Java side `BytesRefBlockHash#add(BytesRefVector)` registers **every** dictionary
/// entry as a hash group. So a sparse dict produces phantom output groups (e.g. an empty
/// "" group with `COUNT(*) = 0`) for every value the filter dropped from view. Compacting
/// here keeps that contract intact and is cheap when the dict is already compact (early
/// return with the original `Arc`).
pub fn compact_dict_array(array: &ArrayRef) -> arrow::error::Result<ArrayRef> {
    let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() else {
        return Ok(Arc::clone(array));
    };

    let keys = dict.keys();
    let values = dict.values();
    let dict_size = values.len();

    if dict_size == 0 || keys.len() == 0 {
        return Ok(Arc::clone(array));
    }

    // Pass 1: which dict positions are referenced by at least one non-null key?
    let mut used = vec![false; dict_size];
    for i in 0..keys.len() {
        if keys.is_valid(i) {
            let k = keys.value(i) as usize;
            if k < dict_size {
                used[k] = true;
            }
        }
    }
    let used_count = used.iter().filter(|&&b| b).count();
    if used_count == dict_size {
        return Ok(Arc::clone(array));
    }

    // Build remapping table: old_idx -> new_idx (-1 if not used).
    let mut new_idx = vec![-1i32; dict_size];
    let mut next: i32 = 0;
    let mut take_indices: Vec<i32> = Vec::with_capacity(used_count);
    for i in 0..dict_size {
        if used[i] {
            new_idx[i] = next;
            next += 1;
            take_indices.push(i as i32);
        }
    }

    // Pass 2: build the compacted values via take().
    let take_arr = Int32Array::from(take_indices);
    let new_values = arrow::compute::take(values.as_ref(), &take_arr, None)?;

    // Pass 3: remap keys, preserving null-key positions.
    let mut new_keys_buf: Vec<Option<i32>> = Vec::with_capacity(keys.len());
    for i in 0..keys.len() {
        if keys.is_valid(i) {
            new_keys_buf.push(Some(new_idx[keys.value(i) as usize]));
        } else {
            new_keys_buf.push(None);
        }
    }
    let new_keys = Int32Array::from(new_keys_buf);

    let new_dict = DictionaryArray::<Int32Type>::try_new(new_keys, new_values)?;
    Ok(Arc::new(new_dict))
}

/// Apply [`compact_dict_array`] to every column of `batch`. Returns the original `batch`
/// when no column was modified, so non-dict batches are zero-cost.
pub fn compact_record_batch_dicts(batch: RecordBatch) -> arrow::error::Result<RecordBatch> {
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut any_changed = false;
    for col in batch.columns() {
        let compacted = compact_dict_array(col)?;
        if !Arc::ptr_eq(col, &compacted) {
            any_changed = true;
        }
        new_columns.push(compacted);
    }
    if !any_changed {
        return Ok(batch);
    }
    RecordBatch::try_new(batch.schema(), new_columns)
}

/// Build a scalar array matching the column's data type from a FilterExpr literal.
/// Returns None if the literal type is incompatible with the column type.
///
/// Peels `Dictionary(_, value_t)` so a literal is matched against the dictionary's
/// value type. Callers that wrap evaluation in [`eval_per_element`] already pass the
/// values array (already unwrapped); the peel here keeps the function correct when
/// invoked directly on a dictionary column.
fn make_scalar(col: &dyn Array, lit: &FilterExpr) -> Option<ArrayRef> {
    let dt = match col.data_type() {
        DataType::Dictionary(_, value_t) => value_t.as_ref(),
        other => other,
    };
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
            return eval_per_element(col.as_ref(), |arr| {
                if let Some(scalar_arr) = make_scalar(arr, right) {
                    cmp_fn(&arr, &Scalar::new(scalar_arr))
                } else {
                    Ok(BooleanArray::from(vec![false; arr.len()]))
                }
            });
        }
    }
    Ok(BooleanArray::from(vec![false; num_rows]))
}

fn eval_in_list(
    col: Arc<dyn Array>,
    items: &[FilterExpr],
) -> arrow::error::Result<BooleanArray> {
    eval_per_element(col.as_ref(), |arr| {
        let mut combined: Option<BooleanArray> = None;
        for item in items {
            if let Some(scalar_arr) = make_scalar(arr, item) {
                let scalar = Scalar::new(scalar_arr);
                let eq_result = cmp::eq(&arr, &scalar)?;
                combined = Some(match combined {
                    Some(prev) => arrow::compute::kernels::boolean::or(&prev, &eq_result)?,
                    None => eq_result,
                });
            }
        }
        // No item matched the column type: produce an all-false mask at this layer's
        // length. For a dict column that's per-value (gets expanded to per-row by take);
        // for a flat column it's already per-row.
        Ok(combined.unwrap_or_else(|| BooleanArray::from(vec![false; arr.len()])))
    })
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
            return eval_per_element(col.as_ref(), |arr| {
                let converted = as_string_array(arr);
                if let Some(str_arr) = converted.as_ref().or_else(|| arr.as_any().downcast_ref::<StringArray>()) {
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
                Ok(BooleanArray::from(vec![false; arr.len()]))
            });
        }
    }
    Ok(BooleanArray::from(vec![false; num_rows]))
}

/// Try to interpret a column as a StringArray, converting from BinaryArray if needed.
fn as_string_array(col: &dyn Array) -> Option<StringArray> {
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
            return eval_per_element(col.as_ref(), |arr| {
                let str_arr = arr.as_any().downcast_ref::<StringArray>();
                let bin_arr = arr.as_any().downcast_ref::<BinaryArray>();
                let n = arr.len();
                let mut results = Vec::with_capacity(n);
                for i in 0..n {
                    if arr.is_null(i) {
                        results.push(Some(false));
                    } else {
                        let val: Option<&str> = str_arr.map(|a| a.value(i))
                            .or_else(|| bin_arr.and_then(|a| std::str::from_utf8(a.value(i)).ok()));
                        let m = val.map(|v| v >= prefix && upper.map_or(true, |u| v < u)).unwrap_or(false);
                        results.push(Some(m));
                    }
                }
                Ok(BooleanArray::from(results))
            });
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// Build a single-column RecordBatch from a (name, array) pair.
    fn batch(name: &str, col: ArrayRef) -> RecordBatch {
        let field = Field::new(name, col.data_type().clone(), true);
        let schema = Arc::new(Schema::new(vec![field]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    /// Build a `Dictionary(Int32, Utf8)` column from values like ["a", "", "b", null, "a"].
    /// Each input becomes its own dictionary entry; nulls become null keys (so the row is null).
    fn dict_utf8(values: &[Option<&str>]) -> ArrayRef {
        let mut keys: Vec<Option<i32>> = Vec::with_capacity(values.len());
        let mut dict_vals: Vec<&str> = Vec::new();
        for v in values {
            match v {
                Some(s) => {
                    keys.push(Some(dict_vals.len() as i32));
                    dict_vals.push(s);
                }
                None => keys.push(None),
            }
        }
        let key_arr = Int32Array::from(keys);
        let val_arr = StringArray::from(dict_vals);
        Arc::new(DictionaryArray::<Int32Type>::try_new(key_arr, Arc::new(val_arr)).unwrap())
    }

    /// Convenience: collect a BooleanArray into Vec<Option<bool>> for assertions.
    fn collect_bool(arr: &BooleanArray) -> Vec<Option<bool>> {
        (0..arr.len()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }).collect()
    }

    fn col_expr(name: &str) -> FilterExpr {
        FilterExpr::Column(name.to_string())
    }

    fn lit_str(s: &str) -> FilterExpr {
        FilterExpr::LiteralString(s.to_string())
    }

    // -- eval_per_element ---------------------------------------------------

    #[test]
    fn eval_per_element_passes_through_flat_columns() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b"), None]));
        let mask = eval_per_element(arr.as_ref(), |a| {
            // Predicate: true for non-null
            Ok(arrow::compute::is_not_null(a)?)
        })
        .unwrap();
        assert_eq!(collect_bool(&mask), vec![Some(true), Some(true), Some(false)]);
    }

    #[test]
    fn eval_per_element_unwraps_dict_and_expands() {
        // Dict has 2 unique values referenced by 5 keys (one null).
        let arr = dict_utf8(&[Some("foo"), Some("bar"), Some("foo"), None, Some("bar")]);
        let mask = eval_per_element(arr.as_ref(), |values| {
            // Predicate evaluated on the dict's small values array.
            // values = ["foo", "bar"] -> mark "bar" as true.
            let s = values.as_any().downcast_ref::<StringArray>().unwrap();
            let bools: Vec<bool> = (0..s.len()).map(|i| s.value(i) == "bar").collect();
            Ok(BooleanArray::from(bools))
        })
        .unwrap();
        // Position 3 has a null key -> propagates to null in the result.
        assert_eq!(
            collect_bool(&mask),
            vec![Some(false), Some(true), Some(false), None, Some(true)]
        );
    }

    // -- eval_comparison (covers eq/neq/lt/lte/gt/gte) ----------------------

    #[test]
    fn neq_on_dict_utf8_filters_correctly() {
        // Q10 minimal repro: WHERE col != "" on a dict-promoted string column.
        let arr = dict_utf8(&[Some(""), Some("iPad"), Some(""), Some("iPhone"), None]);
        let b = batch("col", arr);
        let expr = FilterExpr::NotEq(Box::new(col_expr("col")), Box::new(lit_str("")));
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(false), Some(true), Some(false), Some(true), None]
        );
    }

    #[test]
    fn eq_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some(""), Some("iPad"), Some(""), Some("iPhone"), None]);
        let b = batch("col", arr);
        let expr = FilterExpr::Eq(Box::new(col_expr("col")), Box::new(lit_str("iPad")));
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(false), Some(true), Some(false), Some(false), None]
        );
    }

    #[test]
    fn lt_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some("a"), Some("c"), Some("b"), Some("d")]);
        let b = batch("col", arr);
        let expr = FilterExpr::Lt(Box::new(col_expr("col")), Box::new(lit_str("c")));
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(true), Some(false), Some(true), Some(false)]
        );
    }

    // -- eval_in_list -------------------------------------------------------

    #[test]
    fn in_list_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some("apple"), Some("banana"), Some("cherry"), Some("date"), None]);
        let b = batch("col", arr);
        let expr = FilterExpr::InList(
            Box::new(col_expr("col")),
            vec![lit_str("apple"), lit_str("cherry")],
        );
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(true), Some(false), Some(true), Some(false), None]
        );
    }

    // -- eval_like ----------------------------------------------------------

    #[test]
    fn like_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some("foo"), Some("foobar"), Some("baz"), None]);
        let b = batch("col", arr);
        let expr = FilterExpr::Like(Box::new(col_expr("col")), "foo%".to_string(), false);
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(true), Some(true), Some(false), None]
        );
    }

    #[test]
    fn ilike_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some("Foo"), Some("FOOBAR"), Some("baz")]);
        let b = batch("col", arr);
        let expr = FilterExpr::Like(Box::new(col_expr("col")), "foo%".to_string(), true);
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(true), Some(true), Some(false)]
        );
    }

    #[test]
    fn nlike_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some("foo"), Some("baz")]);
        let b = batch("col", arr);
        let expr = FilterExpr::NotLike(Box::new(col_expr("col")), "foo%".to_string(), false);
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(collect_bool(&mask), vec![Some(false), Some(true)]);
    }

    // -- eval_starts_with ---------------------------------------------------

    #[test]
    fn starts_with_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some("apple"), Some("apricot"), Some("banana"), None]);
        let b = batch("col", arr);
        // Range-style upper bound: rows starting with "ap" are >= "ap" and < "aq".
        let expr = FilterExpr::StartsWith(
            Box::new(col_expr("col")),
            "ap".to_string(),
            Some("aq".to_string()),
        );
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(true), Some(true), Some(false), None]
        );
    }

    // -- IsNull / IsNotNull (dict-aware via arrow's is_null on &dyn Array) --

    #[test]
    fn is_null_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some("a"), None, Some("b"), None]);
        let b = batch("col", arr);
        let expr = FilterExpr::IsNull(Box::new(col_expr("col")));
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(false), Some(true), Some(false), Some(true)]
        );
    }

    #[test]
    fn is_not_null_on_dict_utf8_filters_correctly() {
        let arr = dict_utf8(&[Some("a"), None, Some("b"), None]);
        let b = batch("col", arr);
        let expr = FilterExpr::IsNotNull(Box::new(col_expr("col")));
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(true), Some(false), Some(true), Some(false)]
        );
    }

    // -- Regression: flat (non-dict) Utf8 columns still work ---------------

    #[test]
    fn neq_on_flat_utf8_unchanged() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some(""), Some("x"), None, Some("")]));
        let b = batch("col", arr);
        let expr = FilterExpr::NotEq(Box::new(col_expr("col")), Box::new(lit_str("")));
        let mask = evaluate_filter(&expr, &b).unwrap();
        assert_eq!(
            collect_bool(&mask),
            vec![Some(false), Some(true), None, Some(false)]
        );
    }

    // -- compact_dict_array -------------------------------------------------

    /// Build a `Dictionary(Int32, Utf8)` directly from explicit keys + dict values, so we
    /// can construct the sparse shape that `filter_record_batch` produces (a dict whose
    /// values include entries no key references).
    fn dict_utf8_raw(keys: Vec<Option<i32>>, vals: &[&str]) -> ArrayRef {
        let key_arr = Int32Array::from(keys);
        let val_arr = StringArray::from(vals.to_vec());
        Arc::new(DictionaryArray::<Int32Type>::try_new(key_arr, Arc::new(val_arr)).unwrap())
    }

    fn dict_to_strings(arr: &ArrayRef) -> Vec<Option<String>> {
        let dict = arr
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let vals = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let keys = dict.keys();
        (0..keys.len())
            .map(|i| {
                if keys.is_null(i) {
                    None
                } else {
                    Some(vals.value(keys.value(i) as usize).to_string())
                }
            })
            .collect()
    }

    #[test]
    fn compact_dict_drops_unreferenced_values() {
        // Dict has 3 values ["", "iPad", "iPhone"] but only key=2 ("iPhone") is referenced.
        let arr = dict_utf8_raw(vec![Some(2), Some(2), Some(2)], &["", "iPad", "iPhone"]);
        let compacted = compact_dict_array(&arr).unwrap();
        let dict = compacted
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict.values().len(), 1, "values should be compacted to 1");
        let vals = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(vals.value(0), "iPhone");
        // Same row content, just compact storage.
        assert_eq!(
            dict_to_strings(&compacted),
            vec![
                Some("iPhone".to_string()),
                Some("iPhone".to_string()),
                Some("iPhone".to_string()),
            ]
        );
    }

    #[test]
    fn compact_dict_returns_same_arc_when_already_compact() {
        let arr = dict_utf8_raw(vec![Some(0), Some(1), Some(0)], &["a", "b"]);
        let compacted = compact_dict_array(&arr).unwrap();
        assert!(
            Arc::ptr_eq(&arr, &compacted),
            "already-compact dict should not be rebuilt"
        );
    }

    #[test]
    fn compact_dict_preserves_null_keys() {
        // Two dict entries; only "y" is referenced by non-null keys, plus null keys mixed in.
        let arr = dict_utf8_raw(
            vec![Some(1), None, Some(1), None],
            &["x" /* unused */, "y"],
        );
        let compacted = compact_dict_array(&arr).unwrap();
        let dict = compacted
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict.values().len(), 1);
        assert_eq!(
            dict_to_strings(&compacted),
            vec![Some("y".to_string()), None, Some("y".to_string()), None]
        );
    }

    #[test]
    fn compact_dict_handles_no_referenced_values() {
        // All keys null: no value is referenced, dict should compact to empty.
        let arr = dict_utf8_raw(vec![None, None], &["a", "b"]);
        let compacted = compact_dict_array(&arr).unwrap();
        let dict = compacted
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict.values().len(), 0);
        assert_eq!(dict.keys().len(), 2);
        assert!(dict.keys().is_null(0) && dict.keys().is_null(1));
    }

    #[test]
    fn compact_dict_passes_through_non_dict_columns() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
        let compacted = compact_dict_array(&arr).unwrap();
        assert!(
            Arc::ptr_eq(&arr, &compacted),
            "non-dict column should pass through untouched"
        );
    }

    #[test]
    fn compact_record_batch_is_noop_when_no_columns_change() {
        let arr = dict_utf8_raw(vec![Some(0), Some(1)], &["a", "b"]);
        let b = batch("col", arr);
        let original_ptr = b.column(0).as_ref() as *const _;
        let out = compact_record_batch_dicts(b).unwrap();
        let out_ptr = out.column(0).as_ref() as *const _;
        assert_eq!(
            original_ptr, out_ptr,
            "compact_record_batch_dicts should not allocate when nothing changed"
        );
    }
}
