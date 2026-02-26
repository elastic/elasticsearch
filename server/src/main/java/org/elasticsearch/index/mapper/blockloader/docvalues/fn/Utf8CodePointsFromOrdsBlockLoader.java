/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.BinaryAndCounts;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.SortedDvSingletonOrSet;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedSetDocValues;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.ToIntFunction;

import static org.elasticsearch.index.mapper.blockloader.Warnings.registerSingleValueWarning;

/**
 * A count of utf-8 code points for {@code keyword} style fields that are stored as a lookup table.
 */
public class Utf8CodePointsFromOrdsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private static final FeatureFlag FAST_CODE_POINT_COUNT_FEATURE_FLAG = new FeatureFlag("fast_code_point_count");

    private static final ToIntFunction<BytesRef> codePointCountProvider = FAST_CODE_POINT_COUNT_FEATURE_FLAG.isEnabled()
        ? ESVectorUtil::codePointCount
        : UnicodeUtil::codePointCount;

    /**
     * When there are fewer than this many unique values we use much more efficient "low cardinality"
     * loaders. This must be fairly small because we build an untracked int[] with at most this many
     * entries for a cache.
     */
    static final int LOW_CARDINALITY = 1024;

    private final Warnings warnings;

    private final String fieldName;
    private final ByteSizeValue size;

    public Utf8CodePointsFromOrdsBlockLoader(Warnings warnings, String fieldName, ByteSizeValue size) {
        this.warnings = warnings;
        this.fieldName = fieldName;
        this.size = size;
    }

    @Override
    public IntBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.ints(expectedCount);
    }

    @Override
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        SortedDvSingletonOrSet dv = SortedDvSingletonOrSet.get(breaker, size, context, fieldName);
        if (dv != null) {
            if (dv.singleton() != null) {
                if (dv.singleton().docValues().getValueCount() > LOW_CARDINALITY) {
                    return new ImmediateOrdinals(warnings, dv.forceSet());
                }
                return new Singleton(dv.singleton());
            }
            if (dv.set().docValues().getValueCount() > LOW_CARDINALITY) {
                return new ImmediateOrdinals(warnings, dv.set());
            }
            return new SortedSet(warnings, dv.set());
        }
        BinaryAndCounts bc = BinaryAndCounts.get(breaker, context, fieldName, false);
        if (bc == null) {
            return ConstantNull.READER;
        }
        return new MultiValuedBinaryWithSeparateCounts(warnings, bc.counts(), bc.binary());
    }

    @Override
    public String toString() {
        return "Utf8CodePointsFromOrds[" + fieldName + "]";
    }

    /**
     * Loads low cardinality singleton ordinals in using a cache of code point counts.
     * <p>
     *     It's very important to look up ordinals in ascending order. So, if we haven't
     *     cached the counts for all ordinals, then the process looks like:
     * </p>
     * <ol>
     *     <li>Build an {@code int[]} containing the ordinals</li>
     *     <li>Sort a copy of the {@code int[]} and load the count into the cache for each ordinal.</li>
     *     <li>Walk the unsorted {@code int[]} reading from the cache to build the page</li>
     * </ol>
     * <p>
     *     If we <strong>have</strong> cached the counts for all ordinals we load the
     *     ordinals and look them up in the cache immediately.
     * </p>
     */
    private class Singleton extends BlockDocValuesReader {
        private final TrackingSortedDocValues ordinals;
        private final int[] cache;

        private int cacheEntriesFilled;

        Singleton(TrackingSortedDocValues ordinals) {
            super(null);
            this.ordinals = ordinals;

            int cacheSize = Math.toIntExact(ordinals.docValues().getValueCount());
            boolean success = false;
            try {
                ordinals.breaker().addEstimateBytesAndMaybeBreak(sizeOfArray(cacheSize), "load blocks");
                success = true;
            } finally {
                if (success == false) {
                    ordinals.close();
                }
            }
            this.cache = new int[cacheSize];
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
            if (ordinals.docValues().advanceExact(docId)) {
                ((IntBuilder) builder).appendInt(codePointsAtOrd(ordinals.docValues().ordValue()));
            } else {
                builder.appendNull();
            }
        }

        @Override
        public int docId() {
            return ordinals.docValues().docID();
        }

        @Override
        public String toString() {
            return "Utf8CodePointsFromOrds.Singleton";
        }

        private Block blockForSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.docValues().advanceExact(docId)) {
                return factory.constantInt(codePointsAtOrd(ordinals.docValues().ordValue()), 1);
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
                    if (ordinals.docValues().advanceExact(doc) == false) {
                        ords[i] = -1;
                        continue;
                    }
                    ords[i] = ordinals.docValues().ordValue();
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

        /**
         * Fill the cache for all ords. We skip values {@code -1} which represent "no data".
         */
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

        /**
         * Build the results for a list of documents directly from the cache. We use this
         * if we're sure that all ordinals we're going to load are already cached.
         */
        private Block buildFromFilledCache(BlockFactory factory, Docs docs, int offset) throws IOException {
            int count = docs.count() - offset;
            try (IntBuilder builder = factory.ints(count)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (ordinals.docValues().advanceExact(doc) == false) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendInt(cache[ordinals.docValues().ordValue()]);
                }
                return builder.build();
            }
        }

        /**
         * Get the count of code points at the ord, reading from the cache if possible.
         * The {@code ord} must be {@code >= 0} or this will fail.
         */
        private int codePointsAtOrd(int ord) throws IOException {
            if (cache[ord] >= 0) {
                return cache[ord];
            }
            BytesRef v = ordinals.docValues().lookupOrd(ord);
            int count = codePointCountProvider.applyAsInt(v);
            cache[ord] = count;
            cacheEntriesFilled++;
            return count;
        }

        @Override
        public void close() {
            Releasables.close(ordinals, () -> ordinals.breaker().addWithoutBreaking(-sizeOfArray(cache.length)));
        }
    }

    /**
     * Loads low cardinality non-singleton ordinals in using a cache of code point counts.
     * See {@link Singleton} for the process
     */
    private class SortedSet extends BlockDocValuesReader {
        private final Warnings warnings;
        private final TrackingSortedSetDocValues ordinals;
        private final int[] cache;

        private int cacheEntriesFilled;

        SortedSet(Warnings warnings, TrackingSortedSetDocValues ordinals) {
            super(null);
            this.warnings = warnings;
            this.ordinals = ordinals;

            int cacheSize = Math.toIntExact(ordinals.docValues().getValueCount());
            boolean success = false;
            try {
                ordinals.breaker().addEstimateBytesAndMaybeBreak(sizeOfArray(cacheSize), "load blocks");
                success = true;
            } finally {
                if (success == false) {
                    ordinals.close();
                }
            }
            this.cache = new int[cacheSize];
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

            int[] ords = readOrds(ordinals.docValues(), warnings, factory, docs, offset);
            try {
                fillCache(factory, ords);
                return buildFromCache(factory, cache, ords);
            } finally {
                factory.adjustBreaker(-RamUsageEstimator.shallowSizeOf(ords));
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            if (ordinals.docValues().advanceExact(docId) == false) {
                builder.appendNull();
                return;
            }
            if (ordinals.docValues().docValueCount() != 1) {
                registerSingleValueWarning(warnings);
                builder.appendNull();
                return;
            }
            ((IntBuilder) builder).appendInt(codePointsAtOrd(Math.toIntExact(ordinals.docValues().nextOrd())));
        }

        @Override
        public int docId() {
            return ordinals.docValues().docID();
        }

        @Override
        public String toString() {
            return "Utf8CodePointsFromOrds.SortedSet";
        }

        private Block blockForSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.docValues().advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            if (ordinals.docValues().docValueCount() == 1) {
                return factory.constantInt(codePointsAtOrd(Math.toIntExact(ordinals.docValues().nextOrd())), 1);
            }
            registerSingleValueWarning(warnings);
            return factory.constantNulls(1);
        }

        /**
         * Fill the cache for all ords. We skip values {@code -1} which represent "no data".
         */
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

        /**
         * Build the results for a list of documents directly from the cache. We use this
         * if we're sure that all ordinals we're going to load are already cached.
         */
        private Block buildFromFilledCache(BlockFactory factory, Docs docs, int offset) throws IOException {
            int count = docs.count() - offset;
            try (IntBuilder builder = factory.ints(count)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (ordinals.docValues().advanceExact(doc) == false) {
                        builder.appendNull();
                        continue;
                    }
                    if (ordinals.docValues().docValueCount() != 1) {
                        registerSingleValueWarning(warnings);
                        builder.appendNull();
                        continue;
                    }
                    builder.appendInt(cache[Math.toIntExact(ordinals.docValues().nextOrd())]);
                }
                return builder.build();
            }
        }

        /**
         * Get the count of code points at the ord, reading from the cache if possible.
         * The {@code ord} must be {@code >= 0} or this will fail.
         */
        private int codePointsAtOrd(int ord) throws IOException {
            if (cache[ord] >= 0) {
                return cache[ord];
            }
            BytesRef v = ordinals.docValues().lookupOrd(ord);
            int count = codePointCountProvider.applyAsInt(v);
            cache[ord] = count;
            cacheEntriesFilled++;
            return count;
        }

        @Override
        public void close() {
            Releasables.close(ordinals, () -> ordinals.breaker().addWithoutBreaking(-sizeOfArray(cache.length)));
        }
    }

    /**
     * Loads a count of utf-8 code points for each ordinal without a cache. We use this when there
     * are many unique doc values and the cache hit rate is unlikely to be high.
     * <p>
     *     It's very important to read values in sorted order so we:
     * </p>
     * <ul>
     *     <li>Load the ordinals into an {@code int[]}, using -1 for "empty" values</li>
     *     <li>Create a sorted copy of the {@code int[]}</li>
     *     <li>Compact the sorted {@code int[]}s into a sorted list of unique, non "empty" ordinals</li>
     *     <li>Count the code points for each of the sorted, compacted ordinals</li>
     *     <li>
     *         Walk the original ordinals {@code int[]} which are in doc order, building a {@link Block}
     *         of counts.
     *     </li>
     * </ul>
     */
    private class ImmediateOrdinals extends BlockDocValuesReader {
        private final Warnings warnings;
        private final TrackingSortedSetDocValues ordinals;

        ImmediateOrdinals(Warnings warnings, TrackingSortedSetDocValues ordinals) {
            super(null);
            this.ordinals = ordinals;
            this.warnings = warnings;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return blockForSingleDoc(factory, docs.get(offset));
            }

            int[] ords = readOrds(ordinals.docValues(), warnings, factory, docs, offset);
            int[] sortedOrds = null;
            int[] counts = null;
            try {
                sortedOrds = sortedOrds(factory, ords);
                int compactedLength = compactSorted(sortedOrds);
                counts = counts(factory, sortedOrds, compactedLength);
                try (IntBuilder builder = factory.ints(ords.length)) {
                    for (int ord : ords) {
                        if (ord >= 0) {
                            builder.appendInt(counts[Arrays.binarySearch(sortedOrds, 0, compactedLength, ord)]);
                        } else {
                            builder.appendNull();
                        }
                    }
                    return builder.build();
                }
            } finally {
                factory.adjustBreaker(-RamUsageEstimator.shallowSizeOf(ords));
                if (sortedOrds != null) {
                    factory.adjustBreaker(-RamUsageEstimator.shallowSizeOf(sortedOrds));
                }
                if (counts != null) {
                    factory.adjustBreaker(-RamUsageEstimator.shallowSizeOf(counts));
                }
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (IntBuilder) builder);
        }

        private void read(int docId, IntBuilder builder) throws IOException {
            if (ordinals.docValues().advanceExact(docId) == false) {
                builder.appendNull();
                return;
            }
            if (ordinals.docValues().docValueCount() != 1) {
                registerSingleValueWarning(warnings);
                builder.appendNull();
                return;
            }
            builder.appendInt(codePointsAtOrd(ordinals.docValues().nextOrd()));
        }

        @Override
        public int docId() {
            return ordinals.docValues().docID();
        }

        @Override
        public String toString() {
            return "Utf8CodePointsFromOrds.Immediate";
        }

        private Block blockForSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.docValues().advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            if (ordinals.docValues().docValueCount() == 1) {
                return factory.constantInt(codePointsAtOrd(ordinals.docValues().nextOrd()), 1);
            }
            registerSingleValueWarning(warnings);
            return factory.constantNulls(1);
        }

        /**
         * Builds a sorted copy of the loaded ordinals.
         */
        private int[] sortedOrds(BlockFactory factory, int[] ords) {
            factory.adjustBreaker(RamUsageEstimator.sizeOf(ords));
            int[] sortedOrds = ords.clone();
            Arrays.sort(sortedOrds);
            return sortedOrds;
        }

        /**
         * Compacts the array of sorted ordinals into an array of populated ({@code >= 0}), unique ordinals.
         * @return the length of the unique array
         */
        private int compactSorted(int[] sortedOrds) {
            int c = 0;
            int i = 0;
            while (i < sortedOrds.length && sortedOrds[i] < 0) {
                i++;
            }
            while (i < sortedOrds.length) {
                if (false == (i > 0 && sortedOrds[i - 1] == sortedOrds[i])) {
                    sortedOrds[c++] = sortedOrds[i];
                }
                i++;
            }
            return c;
        }

        private int[] counts(BlockFactory factory, int[] compactedSortedOrds, int compactedLength) throws IOException {
            long size = sizeOfArray(compactedLength);
            factory.adjustBreaker(size);
            int[] counts = new int[compactedLength];
            for (int i = 0; i < counts.length; i++) {
                counts[i] = codePointsAtOrd(compactedSortedOrds[i]);
            }
            return counts;
        }

        /**
         * Get the count of code points at the ord.
         * The {@code ord} must be {@code >= 0} or this will fail.
         */
        private int codePointsAtOrd(long ord) throws IOException {
            return codePointCountProvider.applyAsInt(ordinals.docValues().lookupOrd(ord));
        }

        @Override
        public void close() {
            ordinals.close();
        }
    }

    private static class MultiValuedBinaryWithSeparateCounts extends MultiValuedBinaryWithSeparateCountsLengthReader {
        MultiValuedBinaryWithSeparateCounts(Warnings warnings, TrackingNumericDocValues counts, TrackingBinaryDocValues values) {
            super(warnings, counts, values);
        }

        @Override
        int length(BytesRef bytesRef) {
            return codePointCountProvider.applyAsInt(bytesRef);
        }

        @Override
        public String toString() {
            return "Utf8CodePointsFromOrds.MultiValuedBinaryWithSeparateCounts";
        }
    }

    /**
     * Load an ordinal for each position. Three cases:
     * <ul>
     *     <li>There is a single ordinal at this position - load the ordinals value in to the array</li>
     *     <li>There are no values at this position - load a -1 - we'll skip loading that later</li>
     *     <li>There are <strong>many</strong> values at this position - load a -1 which we'll skip like above - and emit a warning</li>
     * </ul>
     */
    private static int[] readOrds(SortedSetDocValues ordinals, Warnings warnings, BlockFactory factory, Docs docs, int offset)
        throws IOException {
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
                    registerSingleValueWarning(warnings);
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
