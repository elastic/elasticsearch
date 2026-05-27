use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::errors::Result;
use parquet::file::metadata::ParquetMetaData;

/// Maximum gap between two byte ranges before we stop merging them.
/// We read up to this many bytes of unused data to save a round-trip.
/// 1 MiB sits well below typical Parquet page sizes while being large enough
/// to amortise S3 request latency in most cases.
pub const DEFAULT_COALESCE_THRESHOLD: u64 = 1024 * 1024;

/// Wraps [`ParquetObjectReader`] and coalesces nearby byte-range requests
/// before submitting them to object storage.
///
/// parquet-rs issues one I/O request per surviving page range (after offset-index
/// pruning). For object storage each request carries independent latency overhead.
/// When two page ranges sit close together in the file it is cheaper to fetch a
/// single merged range and discard the gap bytes than to pay two round-trips.
///
/// `get_bytes` and `get_metadata` are forwarded unchanged; only `get_byte_ranges`
/// applies coalescing.
#[derive(Clone)]
pub struct CoalescingReader {
    inner: ParquetObjectReader,
    threshold: u64,
}

impl CoalescingReader {
    pub fn new(inner: ParquetObjectReader) -> Self {
        Self {
            inner,
            threshold: DEFAULT_COALESCE_THRESHOLD,
        }
    }
}

impl AsyncFileReader for CoalescingReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        if ranges.len() <= 1 {
            return self.inner.get_byte_ranges(ranges);
        }

        let (merged, mapping) = build_coalesce_plan(&ranges, self.threshold);

        let saved = ranges.len() - merged.len();
        let bytes_requested: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        let bytes_fetched: u64 = merged.iter().map(|r| r.end - r.start).sum();
        log::info!(
            target: "esql_parquet_rs::coalescing",
            "ranges={} merged={} saved={} bytes_requested={} bytes_fetched={} overhead={}",
            ranges.len(), merged.len(), saved, bytes_requested, bytes_fetched,
            bytes_fetched.saturating_sub(bytes_requested)
        );

        self.inner
            .get_byte_ranges(merged)
            .map(move |result| {
                let fetched = result?;
                Ok(ranges
                    .iter()
                    .enumerate()
                    .map(|(i, r)| {
                        let (mi, off) = mapping[i];
                        let len = (r.end - r.start) as usize;
                        fetched[mi].slice(off..off + len)
                    })
                    .collect())
            })
            .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
}

/// Returns `(merged_ranges, mapping)` where `mapping[i] = (merged_idx, byte_offset_within_merged)`.
///
/// Consecutive ranges whose gap is ≤ `threshold` are merged into one. The caller
/// fetches `merged_ranges` and uses `mapping` to slice each original sub-range back
/// out of the corresponding merged buffer.
fn build_coalesce_plan(
    ranges: &[Range<u64>],
    threshold: u64,
) -> (Vec<Range<u64>>, Vec<(usize, usize)>) {
    debug_assert!(!ranges.is_empty());

    // Sort by start offset while keeping track of the original index.
    let mut sorted: Vec<(usize, &Range<u64>)> = ranges.iter().enumerate().collect();
    sorted.sort_by_key(|(_, r)| r.start);

    let mut merged: Vec<Range<u64>> = Vec::new();
    let mut mapping: Vec<(usize, usize)> = vec![(0, 0); ranges.len()];

    let (first_orig, first_r) = sorted[0];
    let mut group_start = first_r.start;
    let mut group_end = first_r.end;
    let mut group_members: Vec<(usize, usize)> = vec![(first_orig, 0)];

    for &(orig_idx, r) in &sorted[1..] {
        if r.start <= group_end.saturating_add(threshold) {
            let off = (r.start - group_start) as usize;
            group_end = group_end.max(r.end);
            group_members.push((orig_idx, off));
        } else {
            let mi = merged.len();
            for &(oi, off) in &group_members {
                mapping[oi] = (mi, off);
            }
            merged.push(group_start..group_end);
            group_start = r.start;
            group_end = r.end;
            group_members = vec![(orig_idx, 0)];
        }
    }

    let mi = merged.len();
    for &(oi, off) in &group_members {
        mapping[oi] = (mi, off);
    }
    merged.push(group_start..group_end);

    (merged, mapping)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(v: &[(u64, u64)]) -> Vec<Range<u64>> {
        v.iter().map(|&(s, e)| s..e).collect()
    }

    #[test]
    fn single_range_passthrough() {
        let ranges = r(&[(0, 100)]);
        let (merged, mapping) = build_coalesce_plan(&ranges, 1024);
        assert_eq!(merged, r(&[(0, 100)]));
        assert_eq!(mapping, vec![(0, 0)]);
    }

    #[test]
    fn adjacent_ranges_merge() {
        let ranges = r(&[(0, 100), (100, 200)]);
        let (merged, mapping) = build_coalesce_plan(&ranges, 0);
        assert_eq!(merged, r(&[(0, 200)]));
        assert_eq!(mapping[0], (0, 0));
        assert_eq!(mapping[1], (0, 100));
    }

    #[test]
    fn gap_within_threshold_merges() {
        let ranges = r(&[(0, 100), (150, 250)]);
        let (merged, mapping) = build_coalesce_plan(&ranges, 100);
        assert_eq!(merged, r(&[(0, 250)]));
        assert_eq!(mapping[0], (0, 0));
        assert_eq!(mapping[1], (0, 150));
    }

    #[test]
    fn gap_beyond_threshold_stays_separate() {
        let ranges = r(&[(0, 100), (300, 400)]);
        let (merged, mapping) = build_coalesce_plan(&ranges, 100);
        assert_eq!(merged, r(&[(0, 100), (300, 400)]));
        assert_eq!(mapping[0], (0, 0));
        assert_eq!(mapping[1], (1, 0));
    }

    #[test]
    fn partial_coalesce() {
        let ranges = r(&[(0, 100), (150, 250), (1000, 1100)]);
        let (merged, mapping) = build_coalesce_plan(&ranges, 100);
        assert_eq!(merged, r(&[(0, 250), (1000, 1100)]));
        assert_eq!(mapping[0], (0, 0));
        assert_eq!(mapping[1], (0, 150));
        assert_eq!(mapping[2], (1, 0));
    }

    #[test]
    fn unsorted_input_preserves_original_indices() {
        // Passed as [300..400, 0..100, 150..250], threshold 100 → all merge into 0..400
        let ranges = r(&[(300, 400), (0, 100), (150, 250)]);
        let (merged, mapping) = build_coalesce_plan(&ranges, 100);
        assert_eq!(merged, r(&[(0, 400)]));
        assert_eq!(mapping[0], (0, 300)); // 300..400 sits at offset 300
        assert_eq!(mapping[1], (0, 0));   // 0..100 sits at offset 0
        assert_eq!(mapping[2], (0, 150)); // 150..250 sits at offset 150
    }
}
