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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Load {@code _source} fields from {@link SortedSetDocValues}.
 */
public abstract class SortedSetDocValuesSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private static final Logger logger = LogManager.getLogger(SortedSetDocValuesSyntheticFieldLoader.class);

    private final String name;
    private final String simpleName;
    private CheckedConsumer<XContentBuilder, IOException> writer = b -> {};

    public SortedSetDocValuesSyntheticFieldLoader(String name, String simpleName) {
        this.name = name;
        this.simpleName = simpleName;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        return Stream.of();
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        SortedSetDocValues dv = DocValues.getSortedSet(reader, name);
        if (dv.getValueCount() == 0) {
            return null;
        }
        if (docIdsInLeaf.length == 1) {
            /*
             * The singleton optimization is mostly about looking up ordinals
             * in sorted order and doesn't buy anything if there is only a single
             * document.
             */
            return new ImmediateDocValuesLoader(dv);
        }
        SortedDocValues singleton = DocValues.unwrapSingleton(dv);
        if (singleton != null) {
            return buildSingletonDocValuesLoader(singleton, docIdsInLeaf);
        }
        return new ImmediateDocValuesLoader(dv);
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        writer.accept(b);
    }

    /**
     * Load ordinals in line with populating the doc and immediately
     * convert from ordinals into {@link BytesRef}s.
     */
    private class ImmediateDocValuesLoader implements DocValuesLoader {
        private final SortedSetDocValues dv;
        private boolean hasValue;

        ImmediateDocValuesLoader(SortedSetDocValues dv) {
            this.dv = dv;
            SortedSetDocValuesSyntheticFieldLoader.this.writer = this::write;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            return hasValue = dv.advanceExact(docId);
        }

        public void write(XContentBuilder b) throws IOException {
            if (false == hasValue) {
                return;
            }
            long first = dv.nextOrd();
            long next = dv.nextOrd();
            if (next == SortedSetDocValues.NO_MORE_ORDS) {
                BytesRef c = convert(dv.lookupOrd(first));
                b.field(simpleName).utf8Value(c.bytes, c.offset, c.length);
                return;
            }
            b.startArray(simpleName);
            BytesRef c = convert(dv.lookupOrd(first));
            b.utf8Value(c.bytes, c.offset, c.length);
            c = convert(dv.lookupOrd(next));
            b.utf8Value(c.bytes, c.offset, c.length);
            while ((next = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                c = convert(dv.lookupOrd(next));
                b.utf8Value(c.bytes, c.offset, c.length);
            }
            b.endArray();
        }
    }

    /**
     * Load all ordinals for all docs up front and resolve to their string
     * values in order. This should be much more disk-friendly than
     * {@link ImmediateDocValuesLoader} because it resolves the ordinals in order and
     * marginally more cpu friendly because it resolves the ordinals one time.
     */
    private DocValuesLoader buildSingletonDocValuesLoader(SortedDocValues singleton, int[] docIdsInLeaf) throws IOException {
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

    private class SingletonDocValuesLoader implements DocValuesLoader {
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
            SortedSetDocValuesSyntheticFieldLoader.this.writer = this::write;
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

        private void write(XContentBuilder b) throws IOException {
            if (ords[idx] < 0) {
                return;
            }
            int convertedIdx = Arrays.binarySearch(uniqueOrds, ords[idx]);
            if (convertedIdx < 0) {
                throw new IllegalStateException("received unexpected ord [" + ords[idx] + "]. Expected " + Arrays.toString(uniqueOrds));
            }
            BytesRef c = converted[convertedIdx];
            b.field(simpleName).utf8Value(c.bytes, c.offset, c.length);
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
