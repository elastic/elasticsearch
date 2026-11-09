/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A doc values skipper with one level, whose intervals are the row groups of the pieces of a segment and whose ranges come from the
 * statistics in the footer snapshot. It reads nothing from the file. A row group without a usable statistic gets the widest range,
 * so the skipper says that the group may hold any value and a query reads it, which is slower and never wrong.
 *
 * <p>Without a skipper, a field that the mapping declares with one reads as an empty field for the range checks of the mapper (see
 * {@code DateFieldType#isFieldWithinQuery}), which is why every integer field of a segment has this skipper.
 */
final class ParquetDocValuesSkipper extends DocValuesSkipper {

    private final int[] minDocs;
    private final int[] maxDocs;
    private final long[] minValues;
    private final long[] maxValues;
    private final int[] docCounts;
    private final long globalMin;
    private final long globalMax;
    private final int globalDocCount;
    /** The interval that the skipper is on, {@code -1} before the first {@link #advance(int)} and the interval count at the end. */
    private int current = -1;

    private ParquetDocValuesSkipper(int[] minDocs, int[] maxDocs, long[] minValues, long[] maxValues, int[] docCounts) {
        this.minDocs = minDocs;
        this.maxDocs = maxDocs;
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.docCounts = docCounts;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long count = 0;
        for (int i = 0; i < minDocs.length; i++) {
            min = Math.min(min, minValues[i]);
            max = Math.max(max, maxValues[i]);
            count += docCounts[i];
        }
        this.globalMin = min;
        this.globalMax = max;
        this.globalDocCount = (int) count;
    }

    /**
     * Builds the skipper of the field bound to {@code binding}, with one interval for each row group of the pieces that has rows.
     * Returns {@code null} when the pieces hold no rows.
     */
    static ParquetDocValuesSkipper of(ColumnBinding binding, List<ExternalFilePiece> pieces) {
        List<int[]> docs = new ArrayList<>();
        List<long[]> values = new ArrayList<>();
        int startDoc = 0;
        for (ExternalFilePiece piece : pieces) {
            ColumnSnapshot column = piece.column(binding.parquetColumnPath());
            if (column == null) {
                throw new IllegalStateException("the segment has no snapshot of column [" + binding.parquetColumnPath() + "]");
            }
            for (int rowGroup = 0; rowGroup < piece.rowGroupRows().length; rowGroup++) {
                int rows = piece.rowGroupRows()[rowGroup];
                if (rows == 0) {
                    continue;
                }
                long count = column.valueCount(rowGroup, rows);
                long min = column.minValues()[rowGroup];
                long max = column.maxValues()[rowGroup];
                try {
                    min = min == ColumnSnapshot.UNKNOWN_MIN ? min : binding.conversion().apply(min);
                    max = max == ColumnSnapshot.UNKNOWN_MAX ? max : binding.conversion().apply(max);
                } catch (ArithmeticException e) {
                    // a value that the conversion cannot represent: the group may hold anything
                    min = ColumnSnapshot.UNKNOWN_MIN;
                    max = ColumnSnapshot.UNKNOWN_MAX;
                }
                docs.add(new int[] { startDoc, startDoc + rows - 1, (int) Math.max(count, 0) });
                values.add(new long[] { min, max });
                startDoc += rows;
            }
        }
        if (docs.isEmpty()) {
            return null;
        }
        int[] minDocs = new int[docs.size()];
        int[] maxDocs = new int[docs.size()];
        int[] counts = new int[docs.size()];
        long[] minValues = new long[docs.size()];
        long[] maxValues = new long[docs.size()];
        for (int i = 0; i < minDocs.length; i++) {
            minDocs[i] = docs.get(i)[0];
            maxDocs[i] = docs.get(i)[1];
            counts[i] = docs.get(i)[2];
            minValues[i] = values.get(i)[0];
            maxValues[i] = values.get(i)[1];
        }
        return new ParquetDocValuesSkipper(minDocs, maxDocs, minValues, maxValues, counts);
    }

    @Override
    public void advance(int target) {
        if (current >= minDocs.length) {
            return;
        }
        // the first interval that ends at or after the target
        int index = Arrays.binarySearch(maxDocs, Math.max(current, 0), maxDocs.length, target);
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
        return current < 0 ? -1 : current >= minDocs.length ? DocIdSetIterator.NO_MORE_DOCS : minDocs[current];
    }

    @Override
    public int maxDocID(int level) {
        return current < 0 ? -1 : current >= minDocs.length ? DocIdSetIterator.NO_MORE_DOCS : maxDocs[current];
    }

    @Override
    public long minValue(int level) {
        return current < 0 || current >= minDocs.length ? globalMin : minValues[current];
    }

    @Override
    public long maxValue(int level) {
        return current < 0 || current >= minDocs.length ? globalMax : maxValues[current];
    }

    @Override
    public int docCount(int level) {
        return current < 0 || current >= minDocs.length ? 0 : docCounts[current];
    }

    @Override
    public long minValue() {
        return globalMin;
    }

    @Override
    public long maxValue() {
        return globalMax;
    }

    @Override
    public int docCount() {
        return globalDocCount;
    }
}
