/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.NumericDvSingletonOrSorted;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;
import java.util.Arrays;

/**
 * Loads {@code long}s from doc values, rounding each value down to one of a sorted list of points.
 * When a {@link DocValuesSkipper} is available, attempts to shortcircuit entire blocks by checking
 * whether the min and max values for the block's doc ID range round to the same point.
 */
public class RoundToLongsFromDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    final String fieldName;
    private final long[] points;

    public RoundToLongsFromDocValuesBlockLoader(String fieldName, long[] points) {
        this.fieldName = fieldName;
        this.points = points;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.longs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        NumericDvSingletonOrSorted dv = NumericDvSingletonOrSorted.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.COLUMN_READER;
        }
        try {
            DocValuesSkipper skipper = context.reader().getDocValuesSkipper(fieldName);
            if (dv.singleton() != null) {
                return new RoundToSingleton(dv.singleton(), points, skipper);
            }
            return new RoundToSorted(dv.sorted(), points, skipper);
        } catch (IOException e) {
            if (dv.singleton() != null) {
                dv.singleton().close();
            } else {
                dv.sorted().close();
            }
            throw e;
        }
    }

    @Override
    public String toString() {
        return "RoundToLongsFromDocValues[" + fieldName + "]";
    }

    static long roundTo(long value, long[] points) {
        int idx = Arrays.binarySearch(points, value);
        return points[idx >= 0 ? idx : Math.max(0, -idx - 2)];
    }

    /**
     * Try to return a constant block by checking the skipper's min/max values for the doc range.
     * Returns {@code null} if the optimization cannot be applied.
     */
    @Nullable
    static Block tryConstantBlock(BlockFactory factory, Docs docs, int offset, long[] points, DocValuesSkipper skipper) throws IOException {
        if (skipper == null || docs.count() - offset == 0) {
            return null;
        }
        int minDocId = docs.get(offset);
        int maxDocId = docs.get(docs.count() - 1);
        if (minDocId > skipper.maxDocID(0)) {
            skipper.advance(minDocId);
        }
        if (skipper.minDocID(0) == DocIdSetIterator.NO_MORE_DOCS) {
            return null;
        }
        for (int level = 0; level < skipper.numLevels(); level++) {
            if (skipper.maxDocID(level) >= maxDocId) {
                if (skipper.docCount(level) != skipper.maxDocID(level) - skipper.minDocID(level) + 1) {
                    // some docs are missing, so we have to go doc by doc to get the nulls right
                    return null;
                }
                long roundedMin = roundTo(skipper.minValue(level), points);
                long roundedMax = roundTo(skipper.maxValue(level), points);
                if (roundedMin == roundedMax) {
                    return factory.constantLong(roundedMin, docs.count() - offset);
                }
                return null;
            }
        }
        return null;
    }

    private static class RoundToSingleton extends BlockDocValuesReader {
        private final TrackingNumericDocValues numericDocValues;
        private final long[] points;
        @Nullable
        private final DocValuesSkipper skipper;

        RoundToSingleton(TrackingNumericDocValues numericDocValues, long[] points, @Nullable DocValuesSkipper skipper) {
            super(null);
            this.numericDocValues = numericDocValues;
            this.points = points;
            this.skipper = skipper;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            Block constant = tryConstantBlock(factory, docs, offset, points, skipper);
            if (constant != null) {
                return constant;
            }
            try (LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (numericDocValues.docValues().advanceExact(doc)) {
                        builder.appendLong(roundTo(numericDocValues.docValues().longValue(), points));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "RoundToLongsFromDocValues.Singleton";
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }

    private static class RoundToSorted extends BlockDocValuesReader {
        private final TrackingSortedNumericDocValues numericDocValues;
        private final long[] points;
        @Nullable
        private final DocValuesSkipper skipper;

        RoundToSorted(TrackingSortedNumericDocValues numericDocValues, long[] points, @Nullable DocValuesSkipper skipper) {
            super(null);
            this.numericDocValues = numericDocValues;
            this.points = points;
            this.skipper = skipper;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            Block constant = tryConstantBlock(factory, docs, offset, points, skipper);
            if (constant != null) {
                return constant;
            }
            try (LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        private void read(int doc, LongBuilder builder) throws IOException {
            if (false == numericDocValues.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValues().docValueCount();
            if (count == 1) {
                builder.appendLong(roundTo(numericDocValues.docValues().nextValue(), points));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendLong(roundTo(numericDocValues.docValues().nextValue(), points));
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "RoundToLongsFromDocValues.Sorted";
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }
}
