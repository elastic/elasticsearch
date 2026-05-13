/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a per-row {@link WordMask} representing the survivors of a late-materialization
 * filter into a {@link RowRanges} expressed in the same row-group coordinate space. The
 * resulting ranges are then handed to {@link ColumnChunkPrefetcher#computeFilteredPageRanges}
 * to drive Phase-2 page-level prefetch: only data pages whose row span overlaps a surviving
 * range are fetched from remote storage.
 *
 * <p>Two-phase I/O depends on this conversion to materialize a compact set of contiguous
 * survivor runs: when the filter is highly selective (e.g., {@code Title LIKE "*foo*"}) the
 * resulting ranges typically cover only a few percent of the row group, so the Phase-2 fetch
 * skips the bulk of the projection column data.
 */
final class WordMaskRowRangesConverter {

    private WordMaskRowRangesConverter() {}

    /**
     * Builds a {@link RowRanges} matching the set bits of {@code mask}. The mask must contain
     * exactly {@code totalRows} valid bits (per its contract) and is treated as a row-aligned
     * survivor bitmap for the entire row group.
     *
     * <p>For empty masks (no surviving bits) the returned {@link RowRanges} is empty; callers
     * should detect this case before invoking Phase-2 prefetch.
     *
     * @param mask survivor mask whose set bits identify rows to keep within {@code [0, totalRows)}
     * @param totalRows the row count of the row group; must match {@code mask}'s logical size
     * @return a {@link RowRanges} of survivor runs in ascending order; {@code totalRows} matches the row group size
     */
    static RowRanges fromWordMask(WordMask mask, long totalRows) {
        if (totalRows <= 0) {
            return RowRanges.all(0);
        }
        // Walk set bits via numberOfTrailingZeros, coalescing adjacent positions into runs so the
        // resulting RowRanges has the minimum number of intervals. This is O(numBits + survivors)
        // and avoids constructing an int[] of survivor positions when only the runs are needed.
        List<long[]> runs = new ArrayList<>();
        long runStart = -1;
        long runEnd = -1;
        int numWords = (int) ((totalRows + 63) >>> 6);
        for (int w = 0; w < numWords; w++) {
            long word = mask.wordAt(w);
            if (word == 0) {
                if (runStart >= 0) {
                    runs.add(new long[] { runStart, runEnd });
                    runStart = -1;
                }
                continue;
            }
            // Mask off trailing bits in the last word that are beyond totalRows.
            if (w == numWords - 1) {
                int trailing = (int) (totalRows & 63);
                if (trailing != 0) {
                    word &= (1L << trailing) - 1;
                }
            }
            int base = w << 6;
            while (word != 0) {
                int bit = Long.numberOfTrailingZeros(word);
                long pos = (long) base + bit;
                if (runStart < 0) {
                    runStart = pos;
                    runEnd = pos + 1;
                } else if (pos == runEnd) {
                    runEnd = pos + 1;
                } else {
                    runs.add(new long[] { runStart, runEnd });
                    runStart = pos;
                    runEnd = pos + 1;
                }
                word &= word - 1;
            }
        }
        if (runStart >= 0) {
            runs.add(new long[] { runStart, runEnd });
        }
        if (runs.isEmpty()) {
            return RowRanges.of(0, 0, totalRows);
        }
        long[] starts = new long[runs.size()];
        long[] ends = new long[runs.size()];
        for (int i = 0; i < runs.size(); i++) {
            starts[i] = runs.get(i)[0];
            ends[i] = runs.get(i)[1];
        }
        return RowRanges.ofSorted(starts, ends, totalRows);
    }
}
