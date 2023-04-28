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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

/**
 * Load {@code _source} fields from {@link SortedSetDocValues}.
 */
public abstract class SortedSetDocValuesSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private static final Logger logger = LogManager.getLogger(SortedSetDocValuesSyntheticFieldLoader.class);

    private final String name;
    private final String simpleName;
    private DocValuesFieldValues docValues = NO_VALUES;

    /**
     * Optionally loads stored fields values.
     */
    @Nullable
    private final String storedValuesName;
    private List<Object> storedValues = emptyList();

    /**
     * Optionally loads malformed values from stored fields.
     */
    private final IgnoreMalformedStoredValues ignoreMalformedValues;

    /**
     * Build a loader from doc values and, optionally, a stored field.
     * @param name the name of the field to load from doc values
     * @param simpleName the name to give the field in the rendered {@code _source}
     * @param storedValuesName the name of a stored field to load or null if there aren't any stored field for this field
     * @param loadIgnoreMalformedValues should we load values skipped by {@code ignore_malformed}
     */
    public SortedSetDocValuesSyntheticFieldLoader(
        String name,
        String simpleName,
        @Nullable String storedValuesName,
        boolean loadIgnoreMalformedValues
    ) {
        this.name = name;
        this.simpleName = simpleName;
        this.storedValuesName = storedValuesName;
        this.ignoreMalformedValues = loadIgnoreMalformedValues
            ? IgnoreMalformedStoredValues.stored(name)
            : IgnoreMalformedStoredValues.empty();
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        if (storedValuesName == null) {
            return ignoreMalformedValues.storedFieldLoaders();
        }
        return Stream.concat(
            Stream.of(Map.entry(storedValuesName, values -> this.storedValues = values)),
            ignoreMalformedValues.storedFieldLoaders()
        );
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        SortedSetDocValues dv = DocValues.getSortedSet(reader, name);
        if (dv.getValueCount() == 0) {
            docValues = NO_VALUES;
            return null;
        }
        if (docIdsInLeaf.length > 1) {
            /*
             * The singleton optimization is mostly about looking up ordinals
             * in sorted order and doesn't buy anything if there is only a single
             * document.
             */
            SortedDocValues singleton = DocValues.unwrapSingleton(dv);
            if (singleton != null) {
                SingletonDocValuesLoader loader = buildSingletonDocValuesLoader(singleton, docIdsInLeaf);
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
        return docValues.count() > 0 || storedValues.isEmpty() == false || ignoreMalformedValues.count() > 0;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        int total = docValues.count() + storedValues.size() + ignoreMalformedValues.count();
        switch (total) {
            case 0:
                return;
            case 1:
                b.field(simpleName);
                if (docValues.count() > 0) {
                    assert docValues.count() == 1;
                    assert storedValues.isEmpty();
                    assert ignoreMalformedValues.count() == 0;
                    docValues.write(b);
                } else if (storedValues.isEmpty() == false) {
                    assert docValues.count() == 0;
                    assert storedValues.size() == 1;
                    assert ignoreMalformedValues.count() == 0;
                    BytesRef converted = convert((BytesRef) storedValues.get(0));
                    b.utf8Value(converted.bytes, converted.offset, converted.length);
                    storedValues = emptyList();
                } else {
                    assert docValues.count() == 0;
                    assert storedValues.isEmpty();
                    assert ignoreMalformedValues.count() == 1;
                    ignoreMalformedValues.write(b);
                }
                return;
            default:
                b.startArray(simpleName);
                docValues.write(b);
                for (Object v : storedValues) {
                    BytesRef converted = convert((BytesRef) v);
                    b.utf8Value(converted.bytes, converted.offset, converted.length);
                }
                storedValues = emptyList();
                ignoreMalformedValues.write(b);
                b.endArray();
                return;
        }
    }

    private interface DocValuesFieldValues {
        int count();

        void write(XContentBuilder b) throws IOException;
    }

    private static final DocValuesFieldValues NO_VALUES = new DocValuesFieldValues() {
        @Override
        public int count() {
            return 0;
        }

        @Override
        public void write(XContentBuilder b) {}
    };

    /**
     * Load ordinals in line with populating the doc and immediately
     * convert from ordinals into {@link BytesRef}s.
     */
    private class ImmediateDocValuesLoader implements DocValuesLoader, DocValuesFieldValues {
        private final SortedSetDocValues dv;
        private boolean hasValue;

        ImmediateDocValuesLoader(SortedSetDocValues dv) {
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
                BytesRef c = convert(dv.lookupOrd(dv.nextOrd()));
                b.utf8Value(c.bytes, c.offset, c.length);
            }
        }
    }

    /**
     * Load all ordinals for all docs up front and resolve to their string
     * values in order. This should be much more disk-friendly than
     * {@link ImmediateDocValuesLoader} because it resolves the ordinals in order and
     * marginally more cpu friendly because it resolves the ordinals one time.
     */
    private SingletonDocValuesLoader buildSingletonDocValuesLoader(SortedDocValues singleton, int[] docIdsInLeaf) throws IOException {
        int[] ords = new int[docIdsInLeaf.length];
        int found = 0;
        for (int d = 0; d < docIdsInLeaf.length; d++) {
            if (false == singleton.advanceExact(docIdsInLeaf[d])) {
                ords[d] = -1;
                continue;
            }
            ords[d] = singleton.ordValue();
            found++;
        }
        if (found == 0) {
            return null;
        }
        int[] sortedOrds = ords.clone();
        Arrays.sort(sortedOrds);
        int unique = 0;
        int prev = -1;
        for (int ord : sortedOrds) {
            if (ord != prev) {
                prev = ord;
                unique++;
            }
        }
        int[] uniqueOrds = new int[unique];
        BytesRef[] converted = new BytesRef[unique];
        unique = 0;
        prev = -1;
        for (int ord : sortedOrds) {
            if (ord != prev) {
                prev = ord;
                uniqueOrds[unique] = ord;
                converted[unique] = preserve(convert(singleton.lookupOrd(ord)));
                unique++;
            }
        }
        logger.debug("loading [{}] on [{}] docs covering [{}] ords", name, docIdsInLeaf.length, uniqueOrds.length);
        return new SingletonDocValuesLoader(docIdsInLeaf, ords, uniqueOrds, converted);
    }

    private static class SingletonDocValuesLoader implements DocValuesLoader, DocValuesFieldValues {
        private final int[] docIdsInLeaf;
        private final int[] ords;
        private final int[] uniqueOrds;
        private final BytesRef[] converted;

        private int idx = -1;

        private SingletonDocValuesLoader(int[] docIdsInLeaf, int[] ords, int[] uniqueOrds, BytesRef[] converted) {
            this.docIdsInLeaf = docIdsInLeaf;
            this.ords = ords;
            this.uniqueOrds = uniqueOrds;
            this.converted = converted;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            idx++;
            if (docIdsInLeaf[idx] != docId) {
                throw new IllegalArgumentException(
                    "expected to be called with [" + docIdsInLeaf[idx] + "] but was called with " + docId + " instead"
                );
            }
            return ords[idx] >= 0;
        }

        @Override
        public int count() {
            return ords[idx] < 0 ? 0 : 1;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (ords[idx] < 0) {
                return;
            }
            int convertedIdx = Arrays.binarySearch(uniqueOrds, ords[idx]);
            if (convertedIdx < 0) {
                throw new IllegalStateException("received unexpected ord [" + ords[idx] + "]. Expected " + Arrays.toString(uniqueOrds));
            }
            BytesRef c = converted[convertedIdx];
            b.utf8Value(c.bytes, c.offset, c.length);
        }
    }

    /**
     * Convert a {@link BytesRef} read from the source into bytes to write
     * to the xcontent. This shouldn't make a deep copy if the conversion
     * process itself doesn't require one.
     */
    protected abstract BytesRef convert(BytesRef value);

    /**
     * Preserves {@link BytesRef bytes} returned by {@link #convert}
     * to by written later. This should make a
     * {@link BytesRef#deepCopyOf deep copy} if {@link #convert} didn't.
     */
    protected abstract BytesRef preserve(BytesRef value);
}
