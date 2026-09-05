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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * Load {@code _source} fields from {@link SortedSetDocValues}.
 */
public abstract class SortedSetDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {
    private static final Logger logger = LogManager.getLogger(SortedSetDocValuesSyntheticFieldLoaderLayer.class);

    private final String name;
    private DocValuesFieldValues docValues = NO_VALUES;

    /**
     * Build a loader from doc values and, optionally, a stored field.
     * @param name the name of the field to load from doc values
     */
    public SortedSetDocValuesSyntheticFieldLoaderLayer(String name) {
        this.name = name;
    }

    @Override
    public String fieldName() {
        return name;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        SortedSetDocValues dv = DocValues.getSortedSet(reader, name);
        if (dv.getValueCount() == 0) {
            docValues = NO_VALUES;
            return null;
        }
        if (docIdsInLeaf != null && docIdsInLeaf.length > 1) {
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
        return docValues.count() > 0;
    }

    @Override
    public long valueCount() throws IOException {
        return docValues.count();
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        docValues.write(b);
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
        // Advance to the first doc to fire a prefetch for its ordinals block. If the first doc
        // has a value the iterator is safely positioned at docIdsInLeaf[0] and we can defer
        // the bulk ordinal read and lookupOrd resolution.
        if (singleton.advanceExact(docIdsInLeaf[0])) {
            return new SingletonDocValuesLoader(docIdsInLeaf, singleton);
        }
        // First doc has no value; the iterator may have advanced past docIdsInLeaf[0],
        // so fall through to eager reads for the remaining docs scanning forward.
        int[] ords = new int[docIdsInLeaf.length];
        Arrays.fill(ords, -1);
        int found = 0;
        for (int d = 1; d < docIdsInLeaf.length; d++) {
            if (singleton.advanceExact(docIdsInLeaf[d])) {
                ords[d] = singleton.ordValue();
                found++;
            }
        }
        if (found == 0) {
            return null;
        }
        return new SingletonDocValuesLoader(docIdsInLeaf, ords, singleton);
    }

    private class SingletonDocValuesLoader implements DocValuesLoader, DocValuesFieldValues {
        private final int[] docIdsInLeaf;
        /** Non-null only in the lazy path; null once the bulk load is done. */
        private final SortedDocValues singleton;
        /** Null until the bulk load runs (lazy gate). */
        private int[] ords;
        private int[] uniqueOrds;
        private BytesRef[] converted;

        private int idx = -1;

        /** Lazy constructor: {@code singleton} is positioned at {@code docIdsInLeaf[0]}. */
        private SingletonDocValuesLoader(int[] docIdsInLeaf, SortedDocValues singleton) {
            this.docIdsInLeaf = docIdsInLeaf;
            this.singleton = singleton;
        }

        /** Eager constructor: ords already collected; resolves uniqueOrds/converted immediately. */
        private SingletonDocValuesLoader(int[] docIdsInLeaf, int[] ords, SortedDocValues dv) throws IOException {
            this.docIdsInLeaf = docIdsInLeaf;
            this.singleton = null;
            this.ords = ords;
            resolveOrds(dv);
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            if (ords == null) {
                // Lazy bulk load: singleton is already positioned at docIdsInLeaf[0].
                ords = new int[docIdsInLeaf.length];
                ords[0] = singleton.ordValue();
                for (int d = 1; d < docIdsInLeaf.length; d++) {
                    ords[d] = singleton.advanceExact(docIdsInLeaf[d]) ? singleton.ordValue() : -1;
                }
                resolveOrds(singleton);
            }
            idx++;
            if (docIdsInLeaf[idx] != docId) {
                throw new IllegalArgumentException(
                    "expected to be called with [" + docIdsInLeaf[idx] + "] but was called with " + docId + " instead"
                );
            }
            return ords[idx] >= 0;
        }

        private void resolveOrds(SortedDocValues dv) throws IOException {
            int[] sortedOrds = ords.clone();
            Arrays.sort(sortedOrds);
            int unique = 0, prev = -1;
            for (int ord : sortedOrds) {
                if (ord != prev) {
                    prev = ord;
                    unique++;
                }
            }
            uniqueOrds = new int[unique];
            converted = new BytesRef[unique];
            unique = 0;
            prev = -1;
            for (int ord : sortedOrds) {
                if (ord != prev) {
                    prev = ord;
                    uniqueOrds[unique] = ord;
                    converted[unique] = preserve(convert(dv.lookupOrd(ord)));
                    unique++;
                }
            }
            logger.debug("loading [{}] on [{}] docs covering [{}] ords", name, docIdsInLeaf.length, uniqueOrds.length);
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
