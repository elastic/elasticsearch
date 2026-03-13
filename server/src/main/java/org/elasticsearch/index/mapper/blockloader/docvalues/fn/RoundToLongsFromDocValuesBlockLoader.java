/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;
import java.util.Arrays;

/**
 * Loads {@code long}s from doc values, rounding each value down to one of a sorted list of points.
 */
public class RoundToLongsFromDocValuesBlockLoader extends AbstractLongsFromDocValuesBlockLoader {
    private final long[] points;

    public RoundToLongsFromDocValuesBlockLoader(String fieldName, long[] points) {
        super(fieldName);
        this.points = points;
    }

    @Override
    protected ColumnAtATimeReader singletonReader(TrackingNumericDocValues docValues) {
        return new RoundToSingleton(docValues, points);
    }

    @Override
    protected ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues) {
        return new RoundToSorted(docValues, points);
    }

    @Override
    public String toString() {
        return "RoundToLongsFromDocValues[" + fieldName + "]";
    }

    static long roundTo(long value, long[] points) {
        int idx = Arrays.binarySearch(points, value);
        return points[idx >= 0 ? idx : Math.max(0, -idx - 2)];
    }

    private static class RoundToSingleton extends BlockDocValuesReader {
        private final TrackingNumericDocValues numericDocValues;
        private final long[] points;

        RoundToSingleton(TrackingNumericDocValues numericDocValues, long[] points) {
            super(null);
            this.numericDocValues = numericDocValues;
            this.points = points;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
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

        RoundToSorted(TrackingSortedNumericDocValues numericDocValues, long[] points) {
            super(null);
            this.numericDocValues = numericDocValues;
            this.points = points;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
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
