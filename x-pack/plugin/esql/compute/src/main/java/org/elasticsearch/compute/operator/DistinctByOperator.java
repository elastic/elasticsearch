/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
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

import java.util.Arrays;

/**
 * Operator that filters rows to keep only the first occurrence of each distinct key value.
 * This is useful for deduplication by a key column (e.g., _tsid).
 * <p>
 * The operator maintains a hash of seen key values and only passes through rows
 * where the key value hasn't been seen before.
 */
public class DistinctByOperator extends AbstractPageMappingOperator {

    public record Factory(int keyChannel) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new DistinctByOperator(keyChannel, driverContext.blockFactory());
        }

        @Override
        public String describe() {
            return "DistinctByOperator[keyChannel=" + keyChannel + "]";
        }
    }

    private final int keyChannel;
    private final BytesRefHashTable seenKeys;

    public DistinctByOperator(int keyChannel, BlockFactory blockFactory) {
        this.keyChannel = keyChannel;
        this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
    }

    @Override
    protected Page process(Page page) {
        BytesRefBlock keyBlock = page.getBlock(keyChannel);
        BytesRef scratch = new BytesRef();

        BytesRefVector vector = keyBlock.asVector();
        if (vector != null && vector.isConstant()) {
            BytesRef key = vector.getBytesRef(0, scratch);
            long result = seenKeys.add(key);
            if (result >= 0) {
                return page.filter(false, 0);
            } else {
                page.releaseBlocks();
                return null;
            }
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
            page.releaseBlocks();
            return null;
        }
        if (rowCount == page.getPositionCount()) {
            return page;
        }
        return page.filter(false, Arrays.copyOf(positions, rowCount));
    }

    @Override
    public String toString() {
        return "DistinctByOperator[keyChannel=" + keyChannel + ", seenKeys=" + seenKeys.size() + "]";
    }

    @Override
    public void close() {
        seenKeys.close();
        super.close();
    }
}
