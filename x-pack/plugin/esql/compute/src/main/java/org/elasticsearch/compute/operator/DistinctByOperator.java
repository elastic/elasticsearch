/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

/**
 * Base for operators that track the distinct values of a single key column across pages.
 * The behaviour is picked along two independent axes:
 * <ul>
 *   <li><b>key type</b> ({@code elementType}) selects the implementation (pimpl): a
 *       {@link BytesRefDistinctByOperator} for arbitrary keys (e.g. {@code _tsid}, backed by a
 *       hash table), or an {@link OrdinalDistinctByOperator} for a dense integer key (e.g. a join
 *       ordinal, backed by a {@link BitArray}).</li>
 *   <li><b>action</b> ({@code ignoreDuplicate}) selects what happens on a repeated key:
 *       {@code true} drops the row (deduplication), {@code false} throws (uniqueness enforcement).</li>
 * </ul>
 * Nulls are never treated as duplicates.
 */
public abstract class DistinctByOperator extends AbstractPageMappingOperator {

    /**
     * Builds specialized {@link DistinctByOperator} for a key {@code keyChannel} of type INT.
     */
    public record IntFactory(int keyChannel, boolean ignoreDuplicate) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new OrdinalDistinctByOperator(keyChannel, ignoreDuplicate, driverContext.bigArrays());
        }

        @Override
        public String describe() {
            return "DistinctByOperator[keyChannel=" + keyChannel + "]";
        }
    }

    /**
     * Builds generic {@link DistinctByOperator} for a key {@code keyChannel} of any type.
     */
    public record Factory(int keyChannel, boolean ignoreDuplicate) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new BytesRefDistinctByOperator(keyChannel, ignoreDuplicate, driverContext.blockFactory());
        }

        @Override
        public String describe() {
            return "DistinctByOperator[keyChannel=" + keyChannel + "]";
        }
    }

    protected final int channel;
    protected final boolean ignoreDuplicate;

    protected DistinctByOperator(int channel, boolean ignoreDuplicate) {
        this.channel = channel;
        this.ignoreDuplicate = ignoreDuplicate;
    }

    /**
     * The exception thrown when a duplicate key is seen while {@code ignoreDuplicate == false}.
     */
    protected final IllegalArgumentException duplicateKey() {
        return new IllegalArgumentException("found a duplicate key on channel [" + channel + "]");
    }

    /**
     * {@link DistinctByOperator} keyed on an arbitrary {@link BytesRefBlock} column (e.g. {@code _tsid}),
     * backed by a {@link BytesRefHashTable} seen-set. The hash table doubles as the seen-set: an
     * {@code add} that returns a negative id means the key was already present.
     */
    public static class BytesRefDistinctByOperator extends DistinctByOperator {

        private final BytesRefHashTable seenKeys;

        public BytesRefDistinctByOperator(int channel, boolean ignoreDuplicate, BlockFactory blockFactory) {
            super(channel, ignoreDuplicate);
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
        }

        @Override
        protected Page process(Page page) {
            // Release the input page only on success. On the duplicate-throw path we leave it for the
            // operator's close() to release the still-pending page, avoiding a double release.
            boolean success = false;
            try {
                Page result = doProcess(page);
                success = true;
                return result;
            } finally {
                if (success) {
                    page.releaseBlocks();
                }
            }
        }

        private Page doProcess(Page page) {
            BytesRefBlock keyBlock = page.getBlock(channel);
            BytesRef scratch = new BytesRef();

            BytesRefVector vector = keyBlock.asVector();
            if (vector != null && vector.isConstant()) {
                BytesRef key = vector.getBytesRef(0, scratch);
                long result = seenKeys.add(key);
                if (result >= 0) {
                    return page.filter(false, 0);
                }
                if (ignoreDuplicate == false) {
                    throw duplicateKey();
                }
                return null;
            }

            if (vector != null) {
                OrdinalBytesRefVector ordinals = vector.asOrdinals();
                if (ordinals != null) {
                    return processOrdinalsVector(page, ordinals);
                }
            } else {
                OrdinalBytesRefBlock ordinals = keyBlock.asOrdinals();
                if (ordinals != null) {
                    return processOrdinalsBlock(page, ordinals);
                }
            }

            int rowCount = 0;
            int[] positions = new int[page.getPositionCount()];
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (keyBlock.isNull(p)) {
                    continue;
                }
                BytesRef key = keyBlock.getBytesRef(p, scratch);
                long result = seenKeys.add(key);
                if (result >= 0) {
                    positions[rowCount++] = p;
                } else if (ignoreDuplicate == false) {
                    throw duplicateKey();
                }
            }
            return filteredPage(page, positions, rowCount);
        }

        /**
         * Fast path for ordinal vectors (no nulls): hash only the dictionary entries,
         * then filter positions using cheap integer ordinal lookups.
         */
        private Page processOrdinalsVector(Page page, OrdinalBytesRefVector ordinals) {
            boolean[] skipOrdinal = hashDictionary(ordinals.getDictionaryVector());
            IntVector ords = ordinals.getOrdinalsVector();
            int rowCount = 0;
            int[] positions = new int[page.getPositionCount()];
            for (int p = 0; p < ords.getPositionCount(); p++) {
                int ord = ords.getInt(p);
                if (skipOrdinal[ord] == false) {
                    positions[rowCount++] = p;
                    skipOrdinal[ord] = true;
                } else if (ignoreDuplicate == false) {
                    throw duplicateKey();
                }
            }
            return filteredPage(page, positions, rowCount);
        }

        /**
         * Fast path for ordinal blocks (may contain nulls): hash only the dictionary entries,
         * then filter positions using cheap integer ordinal lookups, skipping null positions.
         */
        private Page processOrdinalsBlock(Page page, OrdinalBytesRefBlock ordinals) {
            boolean[] skipOrdinal = hashDictionary(ordinals.getDictionaryVector());
            IntBlock ords = ordinals.getOrdinalsBlock();
            int rowCount = 0;
            int[] positions = new int[page.getPositionCount()];
            for (int p = 0; p < ords.getPositionCount(); p++) {
                if (ords.isNull(p)) {
                    continue;
                }
                int ord = ords.getInt(ords.getFirstValueIndex(p));
                if (skipOrdinal[ord] == false) {
                    positions[rowCount++] = p;
                    skipOrdinal[ord] = true;
                } else if (ignoreDuplicate == false) {
                    throw duplicateKey();
                }
            }
            return filteredPage(page, positions, rowCount);
        }

        /**
         * Adds all dictionary entries to {@link #seenKeys} and returns a boolean array
         * indexed by ordinal: {@code true} means the key was already present (skip it).
         */
        private boolean[] hashDictionary(BytesRefVector dictionary) {
            BytesRef scratch = new BytesRef();
            boolean[] skip = new boolean[dictionary.getPositionCount()];
            for (int d = 0; d < dictionary.getPositionCount(); d++) {
                skip[d] = seenKeys.add(dictionary.getBytesRef(d, scratch)) < 0;
            }
            return skip;
        }

        private static Page filteredPage(Page page, int[] positions, int rowCount) {
            if (rowCount == 0) {
                return null;
            }
            if (rowCount == page.getPositionCount()) {
                return page.shallowCopy();
            }
            return page.filter(false, positions, 0, rowCount);
        }

        @Override
        public String toString() {
            return "BytesRefDistinctByOperator[channel="
                + channel
                + ", ignoreDuplicate="
                + ignoreDuplicate
                + ", seenKeys="
                + seenKeys.size()
                + "]";
        }

        @Override
        public void close() {
            seenKeys.close();
            super.close();
        }
    }

    /**
     * {@link DistinctByOperator} keyed on a dense, non-negative integer ordinal column (e.g. a join
     * ordinal), backed by a {@link BitArray} seen-set. The ordinal is already dense, so membership is
     * a direct bit test - no hashing, no allocation - which is why this is preferred over feeding an
     * int through the {@link BytesRefDistinctByOperator} hash table.
     * <p>
     *   With {@code ignoreDuplicate == false} the operator is a pass-through guard used to enforce 1:1
     *   uniqueness: it throws on the second row that maps to an already-seen ordinal. With
     *   {@code ignoreDuplicate == true} it keeps the first row per ordinal and drops the rest. Null
     *   ordinals (e.g. join misses) are never duplicates.
     * </p>
     */
    public static class OrdinalDistinctByOperator extends DistinctByOperator {

        private final BitArray seen;

        public OrdinalDistinctByOperator(int channel, boolean ignoreDuplicate, BigArrays bigArrays) {
            super(channel, ignoreDuplicate);
            this.seen = new BitArray(1, bigArrays);
        }

        @Override
        protected Page process(Page page) {
            IntBlock ordinals = page.getBlock(channel);
            return ignoreDuplicate ? dedup(page, ordinals) : guard(page, ordinals);
        }

        /**
         * Pass-through guard: mark every ordinal and throw on the first repeat. The page is returned
         * unchanged, transferring ownership downstream; on the throw path we deliberately do NOT
         * release it, leaving the still-pending page for the operator's close() to release.
         */
        private Page guard(Page page, IntBlock ordinals) {
            IntVector vector = ordinals.asVector();
            if (vector != null) {
                for (int p = 0; p < vector.getPositionCount(); p++) {
                    if (seen.getAndSet(vector.getInt(p))) {
                        throw duplicateKey();
                    }
                }
            } else {
                for (int p = 0; p < ordinals.getPositionCount(); p++) {
                    if (ordinals.isNull(p)) {
                        continue;
                    }
                    if (seen.getAndSet(ordinals.getInt(ordinals.getFirstValueIndex(p)))) {
                        throw duplicateKey();
                    }
                }
            }
            return page;
        }

        /**
         * Keep the first row per ordinal, drop repeats and nulls. Builds a new page (which retains the
         * blocks it keeps), then releases the input.
         */
        private Page dedup(Page page, IntBlock ordinals) {
            int rowCount = 0;
            int[] positions = new int[page.getPositionCount()];
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (ordinals.isNull(p)) {
                    continue;
                }
                if (seen.getAndSet(ordinals.getInt(ordinals.getFirstValueIndex(p))) == false) {
                    positions[rowCount++] = p;
                }
            }
            Page out;
            if (rowCount == 0) {
                out = null;
            } else if (rowCount == page.getPositionCount()) {
                out = page.shallowCopy();
            } else {
                out = page.filter(false, positions, 0, rowCount);
            }
            page.releaseBlocks();
            return out;
        }

        @Override
        public String toString() {
            return "OrdinalDistinctByOperator[channel=" + channel + ", ignoreDuplicate=" + ignoreDuplicate + "]";
        }

        @Override
        public void close() {
            Releasables.close(seen, super::close);
        }
    }
}
