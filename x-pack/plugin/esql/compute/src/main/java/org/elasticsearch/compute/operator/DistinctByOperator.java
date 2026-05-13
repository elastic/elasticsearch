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
    private final BlockFactory blockFactory;
    private final BytesRefHashTable seenKeys;

    public DistinctByOperator(int keyChannel, BlockFactory blockFactory) {
        this.keyChannel = keyChannel;
        this.blockFactory = blockFactory;
        this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
    }

    @Override
    protected Page process(Page page) {
        try {
            BytesRefBlock keyBlock = page.getBlock(keyChannel);
            BytesRef scratch = new BytesRef();

            BytesRefVector vector = keyBlock.asVector();
            if (vector != null && vector.isConstant()) {
                BytesRef key = vector.getBytesRef(0, scratch);
                long result = seenKeys.add(key);
                if (result >= 0) {
                    return page.filter(false, 0);
                } else {
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
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Fast path for ordinal vectors (no nulls): walk rows once, looking up only the dictionary
     * entries that are actually referenced. A per-page {@code seenInPage} boolean[] keeps
     * {@link #seenKeys} from being asked about the same ord twice within the page. Dictionary
     * entries that no row references are never read, so values left over by a shared dictionary
     * (after {@code filter}/{@code slice}/{@code keepMask}) do not pollute the cross-page hash.
     */
    private Page processOrdinalsVector(Page page, OrdinalBytesRefVector ordinals) {
        BytesRefVector dict = ordinals.getDictionaryVector();
        IntVector ords = ordinals.getOrdinalsVector();
        long acquiredBytes = (long) Byte.BYTES * dict.getPositionCount();
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "DistinctByOperator");
        try {
            boolean[] seenInPage = new boolean[dict.getPositionCount()];
            BytesRef scratch = new BytesRef();
            int rowCount = 0;
            int[] positions = new int[page.getPositionCount()];
            for (int p = 0; p < ords.getPositionCount(); p++) {
                int ord = ords.getInt(p);
                if (seenInPage[ord]) {
                    continue;
                }
                seenInPage[ord] = true;
                if (seenKeys.add(dict.getBytesRef(ord, scratch)) >= 0) {
                    positions[rowCount++] = p;
                }
            }
            return filteredPage(page, positions, rowCount);
        } finally {
            blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
        }
    }

    /**
     * Fast path for ordinal blocks (may contain nulls): like {@link #processOrdinalsVector}, but
     * skips null positions. Dictionary entries that no row references are never read.
     */
    private Page processOrdinalsBlock(Page page, OrdinalBytesRefBlock ordinals) {
        BytesRefVector dict = ordinals.getDictionaryVector();
        IntBlock ords = ordinals.getOrdinalsBlock();
        long acquiredBytes = (long) Byte.BYTES * dict.getPositionCount();
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "DistinctByOperator");
        try {
            boolean[] seenInPage = new boolean[dict.getPositionCount()];
            BytesRef scratch = new BytesRef();
            int rowCount = 0;
            int[] positions = new int[page.getPositionCount()];
            for (int p = 0; p < ords.getPositionCount(); p++) {
                if (ords.isNull(p)) {
                    continue;
                }
                int ord = ords.getInt(ords.getFirstValueIndex(p));
                if (seenInPage[ord]) {
                    continue;
                }
                seenInPage[ord] = true;
                if (seenKeys.add(dict.getBytesRef(ord, scratch)) >= 0) {
                    positions[rowCount++] = p;
                }
            }
            return filteredPage(page, positions, rowCount);
        } finally {
            blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
        }
    }

    private static Page filteredPage(Page page, int[] positions, int rowCount) {
        if (rowCount == 0) {
            return null;
        }
        if (rowCount == page.getPositionCount()) {
            return page.shallowCopy();
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
