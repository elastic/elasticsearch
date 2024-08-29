/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Load {@code _source} fields from {@link SortedNumericDocValues}.
 */
public abstract class SortedNumericDocValuesSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private final String name;
    private final String simpleName;

    /**
     * Optionally loads malformed values from stored fields.
     */
    private final IgnoreMalformedStoredValues ignoreMalformedValues;

    private Values values = NO_VALUES;

    /**
     * Build a loader from doc values and, optionally, a stored field.
     * @param name the name of the field to load from doc values
     * @param simpleName the name to give the field in the rendered {@code _source}
     * @param loadIgnoreMalformedValues should we load values skipped by {@code ignore_malformed}
     */
    protected SortedNumericDocValuesSyntheticFieldLoader(String name, String simpleName, boolean loadIgnoreMalformedValues) {
        this.name = name;
        this.simpleName = simpleName;
        this.ignoreMalformedValues = loadIgnoreMalformedValues
            ? IgnoreMalformedStoredValues.stored(name)
            : IgnoreMalformedStoredValues.empty();
    }

    protected abstract void writeValue(XContentBuilder b, long value) throws IOException;

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        return ignoreMalformedValues.storedFieldLoaders();
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        SortedNumericDocValues dv = docValuesOrNull(reader, name);
        if (dv == null) {
            values = NO_VALUES;
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
                values = loader == null ? NO_VALUES : loader;
                return loader;
            }
        }
        ImmediateDocValuesLoader loader = new ImmediateDocValuesLoader(dv);
        values = loader;
        return loader;
    }

    @Override
    public boolean hasValue() {
        return values.count() > 0 || ignoreMalformedValues.count() > 0;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        switch (values.count() + ignoreMalformedValues.count()) {
            case 0:
                return;
            case 1:
                b.field(simpleName);
                if (values.count() > 0) {
                    assert values.count() == 1;
                    assert ignoreMalformedValues.count() == 0;
                    values.write(b);
                } else {
                    assert ignoreMalformedValues.count() == 1;
                    ignoreMalformedValues.write(b);
                }
                return;
            default:
                b.startArray(simpleName);
                values.write(b);
                ignoreMalformedValues.write(b);
                b.endArray();
        }
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
                writeValue(b, dv.nextValue());
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
        public boolean advanceToDoc(int docId) throws IOException {
            idx++;
            if (docIdsInLeaf[idx] != docId) {
                throw new IllegalArgumentException(
                    "expected to be called with [" + docIdsInLeaf[idx] + "] but was called with " + docId + " instead"
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
            writeValue(b, values[idx]);
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

    @Override
    public String fieldName() {
        return name;
    }
}
