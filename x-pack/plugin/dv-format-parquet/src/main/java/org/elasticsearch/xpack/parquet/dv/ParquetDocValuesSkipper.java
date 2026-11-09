/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.core.Nullable;

import java.util.Arrays;

/**
 * A doc values skipper with one level, whose intervals are the row groups of a segment and whose ranges are the bounds that the footer
 * gave when the segment was attached (see {@link ExternalFilePiece.ColumnStats}). It reads nothing from the file. Range queries, sort
 * pruning and the range checks of the mapper (see {@code DateFieldType#isFieldWithinQuery}) use it to skip a row group, or to match all of
 * it, without reading its values.
 *
 * <p>The skipper is stateful, as Lucene's skippers are, so each call to {@code getSkipper} gets its own instance over the shared
 * {@link Intervals}.
 */
final class ParquetDocValuesSkipper extends DocValuesSkipper {

    /** The immutable intervals of one field of one segment, shared by the skippers of its iterators. */
    static final class Intervals {
        private final int[] minDocs;
        private final int[] maxDocs;
        private final long[] minValues;
        private final long[] maxValues;
        private final int[] docCounts;
        private final long globalMin;
        private final long globalMax;
        private final int globalDocCount;

        private Intervals(int[] minDocs, int[] maxDocs, long[] minValues, long[] maxValues, int[] docCounts) {
            this.minDocs = minDocs;
            this.maxDocs = maxDocs;
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.docCounts = docCounts;
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            long count = 0;
            for (int i = 0; i < minDocs.length; i++) {
                if (docCounts[i] > 0) {
                    min = Math.min(min, minValues[i]);
                    max = Math.max(max, maxValues[i]);
                }
                count += docCounts[i];
            }
            this.globalMin = min;
            this.globalMax = max;
            this.globalDocCount = (int) count;
        }

        /**
         * The intervals of a field, one for each row group of the piece that has rows, or {@code null} if the piece has no statistics for
         * the field or no rows.
         */
        @Nullable
        static Intervals of(ExternalFilePiece piece, String field) {
            ExternalFilePiece.ColumnStats stats = piece.stats().get(field);
            if (stats == null || piece.rowGroupRows().length == 0) {
                return null;
            }
            int groups = piece.rowGroupRows().length;
            int[] minDocs = new int[groups];
            int[] maxDocs = new int[groups];
            int count = 0;
            int startDoc = 0;
            long[] minValues = new long[groups];
            long[] maxValues = new long[groups];
            int[] docCounts = new int[groups];
            for (int i = 0; i < groups; i++) {
                int rows = piece.rowGroupRows()[i];
                if (rows == 0) {
                    continue;
                }
                minDocs[count] = startDoc;
                maxDocs[count] = startDoc + rows - 1;
                minValues[count] = stats.minValues()[i];
                maxValues[count] = stats.maxValues()[i];
                docCounts[count] = stats.docCounts()[i];
                count++;
                startDoc += rows;
            }
            if (count == 0) {
                return null;
            }
            return new Intervals(
                Arrays.copyOf(minDocs, count),
                Arrays.copyOf(maxDocs, count),
                Arrays.copyOf(minValues, count),
                Arrays.copyOf(maxValues, count),
                Arrays.copyOf(docCounts, count)
            );
        }
    }

    private final Intervals intervals;
    /** The interval that the skipper is on, {@code -1} before the first {@link #advance(int)} and the interval count at the end. */
    private int current = -1;

    ParquetDocValuesSkipper(Intervals intervals) {
        this.intervals = intervals;
    }

    @Override
    public void advance(int target) {
        if (current >= intervals.minDocs.length) {
            return;
        }
        // the first interval that ends at or after the target
        int index = Arrays.binarySearch(intervals.maxDocs, Math.max(current, 0), intervals.maxDocs.length, target);
        if (index < 0) {
            index = -index - 1;
        }
        current = index;
    }

    @Override
    public int numLevels() {
        return 1;
    }

    @Override
    public int minDocID(int level) {
        return current < 0 ? -1 : current >= intervals.minDocs.length ? DocIdSetIterator.NO_MORE_DOCS : intervals.minDocs[current];
    }

    @Override
    public int maxDocID(int level) {
        return current < 0 ? -1 : current >= intervals.minDocs.length ? DocIdSetIterator.NO_MORE_DOCS : intervals.maxDocs[current];
    }

    @Override
    public long minValue(int level) {
        return current < 0 || current >= intervals.minDocs.length ? intervals.globalMin : intervals.minValues[current];
    }

    @Override
    public long maxValue(int level) {
        return current < 0 || current >= intervals.minDocs.length ? intervals.globalMax : intervals.maxValues[current];
    }

    @Override
    public int docCount(int level) {
        return current < 0 || current >= intervals.minDocs.length ? 0 : intervals.docCounts[current];
    }

    @Override
    public long minValue() {
        return intervals.globalMin;
    }

    @Override
    public long maxValue() {
        return intervals.globalMax;
    }

    @Override
    public int docCount() {
        return intervals.globalDocCount;
    }
}
