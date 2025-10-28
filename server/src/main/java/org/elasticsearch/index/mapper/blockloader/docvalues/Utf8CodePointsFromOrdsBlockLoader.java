/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * A count of utf-8 code points for {@code keyword} style fields that are stored as a lookup table.
 */
public class Utf8CodePointsFromOrdsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    /**
     * When there are fewer than this many unique values we use much more efficient "low cardinality"
     * loaders. This must be fairly small because we build an untracked int[] with at most this many
     * entries for a cache.
     */
    static final int LOW_CARDINALITY = 1024;
    private final String fieldName;

    public Utf8CodePointsFromOrdsBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public IntBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.ints(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        SortedSetDocValues docValues = context.reader().getSortedSetDocValues(fieldName);
        if (docValues != null) {
            if (docValues.getValueCount() > LOW_CARDINALITY) {
                return new ImmediateOrdinals(docValues);
            }
            SortedDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonOrdinals(singleton);
            }
            return new Ordinals(docValues);
        }
        SortedDocValues singleton = context.reader().getSortedDocValues(fieldName);
        if (singleton != null) {
            if (singleton.getValueCount() > LOW_CARDINALITY) {
                return new ImmediateOrdinals(DocValues.singleton(singleton));
            }
            return new SingletonOrdinals(singleton);
        }
        return new ConstantNullsReader();
    }

    @Override
    public String toString() {
        return "Utf8CodePointsFromOrds[" + fieldName + "]";
    }

    /**
     * Loads low cardinality singleton ordinals in using a cache of code point counts.
     * <p>
     *     If we haven't cached the counts for all ordinals then the process looks like:
     * </p>
     * <ol>
     *     <li>Build an int[] containing the ordinals</li>
     *     <li>
     *         Sort a copy of the int[] and load the cache for each ordinal. The sorting
     *         is important here because ordinals are faster to resolved in ascending order.
     *     </li>
     *     <li>Walk the int[] in order, reading from the cache to build the page</li>
     * </ol>
     * <p>
     *     If we <strong>have</strong> cached the counts for all ordinals we load the
     *     ordinals and look them up in the cache immediately.
     * </p>
     */
    private static class SingletonOrdinals extends BlockDocValuesReader {
        private final SortedDocValues ordinals;
        private final int[] cache;

        private int cacheEntriesFilled;

        SingletonOrdinals(SortedDocValues ordinals) {
            this.ordinals = ordinals;

            // TODO track this memory. we can't yet because this isn't Closeable
            this.cache = new int[ordinals.getValueCount()];
            Arrays.fill(this.cache, -1);
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return blockForSingleDoc(factory, docs.get(offset));
            }

            if (cacheEntriesFilled == cache.length) {
                return buildFromFilledCache(factory, docs, offset);
            }

            int[] ords = readOrds(factory, docs, offset);
            try {
                fillCache(factory, ords);
                return buildFromCache(factory, cache, ords);
            } finally {
                factory.adjustBreaker(-RamUsageEstimator.sizeOf(ords));
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            if (ordinals.advanceExact(docId)) {
                ((IntBuilder) builder).appendInt(codePointsAtOrd(ordinals.ordValue()));
            } else {
                builder.appendNull();
            }
        }

        @Override
        public int docId() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "Utf8CodePointsFromOrds.SingletonOrdinals";
        }

        private Block blockForSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.advanceExact(docId)) {
                return factory.constantInt(codePointsAtOrd(ordinals.ordValue()), 1);
            } else {
                return factory.constantNulls(1);
            }
        }

        private int[] readOrds(BlockFactory factory, Docs docs, int offset) throws IOException {
            int count = docs.count() - offset;
            long size = sizeOfArray(count);
            factory.adjustBreaker(size);
            int[] ords = null;
            try {
                ords = new int[count];
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (ordinals.advanceExact(doc) == false) {
                        ords[i] = -1;
                        continue;
                    }
                    ords[i] = ordinals.ordValue();
                }
                int[] result = ords;
                ords = null;
                return result;
            } finally {
                if (ords != null) {
                    factory.adjustBreaker(-size);
                }
            }
        }

        private void fillCache(BlockFactory factory, int[] ords) throws IOException {
            factory.adjustBreaker(RamUsageEstimator.sizeOf(ords));
            try {
                int[] sortedOrds = ords.clone();
                Arrays.sort(sortedOrds);
                int i = 0;
                while (i < sortedOrds.length && sortedOrds[i] < 0) {
                    i++;
                }
                while (i < sortedOrds.length) {
                    // Fill the cache. Duplicates will noop quickly.
                    codePointsAtOrd(sortedOrds[i++]);
                }
            } finally {
                factory.adjustBreaker(-RamUsageEstimator.sizeOf(ords));
            }
        }

        private Block buildFromFilledCache(BlockFactory factory, Docs docs, int offset) throws IOException {
            int count = docs.count() - offset;
            try (IntBuilder builder = factory.ints(count)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (ordinals.advanceExact(doc) == false) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendInt(cache[ordinals.ordValue()]);
                }
                return builder.build();
            }
        }

        private int codePointsAtOrd(int ord) throws IOException {
            if (cache[ord] >= 0) {
                return cache[ord];
            }
            BytesRef v = ordinals.lookupOrd(ord);
            int count = UnicodeUtil.codePointCount(v);
            cache[ord] = count;
            cacheEntriesFilled++;
            return count;
        }
    }

    /**
     * Loads low cardinality non-singleton ordinals in using a cache of code point counts.
     * See {@link SingletonOrdinals} for the process
     */
    private static class Ordinals extends BlockDocValuesReader {
        private final SortedSetDocValues ordinals;
        private final int[] cache;

        private int cacheEntriesFilled;

        Ordinals(SortedSetDocValues ordinals) {
            this.ordinals = ordinals;

            // TODO track this memory. we can't yet because this isn't Closeable
            this.cache = new int[Math.toIntExact(ordinals.getValueCount())];
            Arrays.fill(this.cache, -1);
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return blockForSingleDoc(factory, docs.get(offset));
            }

            if (cacheEntriesFilled == cache.length) {
                return buildFromFilledCache(factory, docs, offset);
            }

            int[] ords = readOrds(factory, docs, offset);
            try {
                fillCache(factory, ords);
                return buildFromCache(factory, cache, ords);
            } finally {
                factory.adjustBreaker(-RamUsageEstimator.shallowSizeOf(ords));
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            if (ordinals.advanceExact(docId) == false) {
                builder.appendNull();
                return;
            }
            if (ordinals.docValueCount() != 1) {
                // TODO warning
                builder.appendNull();
                return;
            }
            ((IntBuilder) builder).appendInt(codePointsAtOrd(Math.toIntExact(ordinals.nextOrd())));
        }

        @Override
        public int docId() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "Utf8CodePointsFromOrds.Ordinals";
        }

        private Block blockForSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            if (ordinals.docValueCount() == 1) {
                return factory.constantInt(codePointsAtOrd(Math.toIntExact(ordinals.nextOrd())), 1);
            }
            // TODO warning!
            return factory.constantNulls(1);
        }

        private int[] readOrds(BlockFactory factory, Docs docs, int offset) throws IOException {
            int count = docs.count() - offset;
            long size = sizeOfArray(count);
            factory.adjustBreaker(size);
            int[] ords = null;
            try {
                ords = new int[docs.count() - offset];
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (ordinals.advanceExact(doc) == false) {
                        ords[i] = -1;
                        continue;
                    }
                    if (ordinals.docValueCount() != 1) {
                        // TODO warning
                        ords[i] = -1;
                        continue;
                    }
                    ords[i] = Math.toIntExact(ordinals.nextOrd());
                }
                int[] result = ords;
                ords = null;
                return result;
            } finally {
                if (ords != null) {
                    factory.adjustBreaker(-size);
                }
            }
        }

        private void fillCache(BlockFactory factory, int[] ords) throws IOException {
            factory.adjustBreaker(RamUsageEstimator.sizeOf(ords));
            try {
                int[] sortedOrds = ords.clone();
                Arrays.sort(sortedOrds);
                int i = 0;
                while (i < sortedOrds.length && sortedOrds[i] < 0) {
                    i++;
                }
                while (i < sortedOrds.length) {
                    // Fill the cache. Duplicates will noop quickly.
                    codePointsAtOrd(sortedOrds[i++]);
                }
            } finally {
                factory.adjustBreaker(-RamUsageEstimator.sizeOf(ords));
            }
        }

        private Block buildFromFilledCache(BlockFactory factory, Docs docs, int offset) throws IOException {
            int count = docs.count() - offset;
            try (IntBuilder builder = factory.ints(count)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (ordinals.advanceExact(doc) == false) {
                        builder.appendNull();
                        continue;
                    }
                    if (ordinals.docValueCount() != 1) {
                        // TODO warning
                        builder.appendNull();
                        continue;
                    }
                    builder.appendInt(cache[Math.toIntExact(ordinals.nextOrd())]);
                }
                return builder.build();
            }
        }

        private int codePointsAtOrd(int ord) throws IOException {
            if (cache[ord] >= 0) {
                return cache[ord];
            }
            BytesRef v = ordinals.lookupOrd(ord);
            int count = UnicodeUtil.codePointCount(v);
            cache[ord] = count;
            cacheEntriesFilled++;
            return count;
        }
    }

    /**
     * Loads a count of utf-8 code points for each ordinal doc by doc, without a cache. We use this when there
     * are many unique doc values and the cache hit rate is unlikely to be high.
     */
    private static class ImmediateOrdinals extends BlockDocValuesReader {
        private final SortedSetDocValues ordinals;

        ImmediateOrdinals(SortedSetDocValues ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return blockForSingleDoc(factory, docs.get(offset));
            }
            try (IntBuilder builder = factory.ints(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (IntBuilder) builder);
        }

        private void read(int docId, IntBuilder builder) throws IOException {
            if (ordinals.advanceExact(docId) == false) {
                builder.appendNull();
                return;
            }
            if (ordinals.docValueCount() != 1) {
                // TODO warning
                builder.appendNull();
                return;
            }
            builder.appendInt(codePointsAtOrd(ordinals.nextOrd()));
        }

        @Override
        public int docId() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "Utf8CodePointsFromOrds.Immediate";
        }

        private Block blockForSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            if (ordinals.docValueCount() == 1) {
                return factory.constantInt(codePointsAtOrd(ordinals.nextOrd()), 1);
            }
            // TODO warning!
            return factory.constantNulls(1);
        }

        private int codePointsAtOrd(long ord) throws IOException {
            return UnicodeUtil.codePointCount(ordinals.lookupOrd(ord));
        }
    }

    private static Block buildFromCache(BlockFactory factory, int[] cache, int[] ords) {
        try (IntBuilder builder = factory.ints(ords.length)) {
            for (int ord : ords) {
                if (ord >= 0) {
                    builder.appendInt(cache[ord]);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private static long sizeOfArray(int count) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * (long) count);
    }
}
