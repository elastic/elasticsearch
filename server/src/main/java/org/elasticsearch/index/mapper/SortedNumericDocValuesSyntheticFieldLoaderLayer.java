/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A {@link CompositeSyntheticFieldLoader.DocValuesLayer} that loads values from {@link SortedNumericDocValues}.
 */
public class SortedNumericDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String name;
    private final SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer.NumericValueWriter valueWriter;
    private Values docValues = NO_VALUES;

    public SortedNumericDocValuesSyntheticFieldLoaderLayer(
        String name,
        SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer.NumericValueWriter valueWriter
    ) {
        this.name = name;
        this.valueWriter = valueWriter;
    }

    @Override
    public String fieldName() {
        return name;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        SortedNumericDocValues dv = docValuesOrNull(reader, name);
        if (dv == null) {
            docValues = NO_VALUES;
            return null;
        }
        if (docIdsInLeaf != null && docIdsInLeaf.length > 1) {
            /*
             * The singleton optimization is mostly about looking up all
             * values for the field at once. If there's just a single
             * document then it's just extra overhead.
             */
            NumericDocValues single = DocValues.unwrapSingleton(dv);
            if (single != null) {
                SingletonDocValuesLoader loader = buildSingletonDocValuesLoader(single, docIdsInLeaf);
                docValues = loader == null ? NO_VALUES : loader;
                return loader;
            }
        }
        ImmediateDocValuesLoader loader = new ImmediateDocValuesLoader(dv);
        docValues = loader;
        return loader;
    }

    @Override
    public boolean hasValue() {
        return docValues.count() > 0;
    }

    @Override
    public long valueCount() {
        return docValues.count();
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        docValues.write(b);
    }

    private interface Values {
        int count();

        void write(XContentBuilder b) throws IOException;
    }

    private static final Values NO_VALUES = new Values() {
        @Override
        public int count() {
            return 0;
        }

        @Override
        public void write(XContentBuilder b) {}
    };

    private class ImmediateDocValuesLoader implements DocValuesLoader, Values {
        private final SortedNumericDocValues dv;
        private boolean hasValue;

        ImmediateDocValuesLoader(SortedNumericDocValues dv) {
            this.dv = dv;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            return hasValue = dv.advanceExact(docId);
        }

        @Override
        public int count() {
            return hasValue ? dv.docValueCount() : 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (hasValue == false) {
                return;
            }
            for (int i = 0; i < dv.docValueCount(); i++) {
                valueWriter.writeLongValue(b, dv.nextValue());
            }
        }
    }

    private SingletonDocValuesLoader buildSingletonDocValuesLoader(NumericDocValues singleton, int[] docIdsInLeaf) throws IOException {
        long[] values = new long[docIdsInLeaf.length];
        boolean[] hasValue = new boolean[docIdsInLeaf.length];
        boolean found = false;
        for (int d = 0; d < docIdsInLeaf.length; d++) {
            if (false == singleton.advanceExact(docIdsInLeaf[d])) {
                hasValue[d] = false;
                continue;
            }
            hasValue[d] = true;
            values[d] = singleton.longValue();
            found = true;
        }
        if (found == false) {
            return null;
        }
        return new SingletonDocValuesLoader(docIdsInLeaf, values, hasValue);
    }

    /**
     * Load all values for all docs up front. This should be much more
     * disk and cpu-friendly than {@link ImmediateDocValuesLoader} because
     * it resolves the values all at once, always scanning forwards on
     * the disk.
     */
    private class SingletonDocValuesLoader implements DocValuesLoader, Values {
        private final int[] docIdsInLeaf;
        private final long[] values;
        private final boolean[] hasValue;
        private int idx = -1;

        private SingletonDocValuesLoader(int[] docIdsInLeaf, long[] values, boolean[] hasValue) {
            this.docIdsInLeaf = docIdsInLeaf;
            this.values = values;
            this.hasValue = hasValue;
        }

        @Override
        public boolean advanceToDoc(int docId) {
            idx++;
            if (docIdsInLeaf[idx] != docId) {
                throw new IllegalArgumentException(
                    "Expected to be called with ["
                        + docIdsInLeaf[idx]
                        + "] but was called with "
                        + docId
                        + " instead (field: "
                        + name
                        + ", idx: "
                        + idx
                        + ", previous docID: "
                        + docIdsInLeaf[idx]
                        + ')'
                );
            }
            return hasValue[idx];
        }

        @Override
        public int count() {
            return hasValue[idx] ? 1 : 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (hasValue[idx] == false) {
                return;
            }
            valueWriter.writeLongValue(b, values[idx]);
        }
    }

    /**
     * Returns a {@link SortedNumericDocValues} or null if it doesn't have any doc values.
     * See {@link DocValues#getSortedNumeric} which is *nearly* the same, but it returns
     * an "empty" implementation if there aren't any doc values. We need to be able to
     * tell if there aren't any and return our empty leaf source loader.
     */
    public static SortedNumericDocValues docValuesOrNull(LeafReader reader, String fieldName) throws IOException {
        SortedNumericDocValues dv = reader.getSortedNumericDocValues(fieldName);
        if (dv != null) {
            return dv;
        }
        NumericDocValues single = reader.getNumericDocValues(fieldName);
        if (single != null) {
            return DocValues.singleton(single);
        }
        return null;
    }
}
