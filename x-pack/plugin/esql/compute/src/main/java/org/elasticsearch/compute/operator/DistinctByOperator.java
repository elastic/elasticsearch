/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
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
            return new DistinctByOperator(keyChannel);
        }

        @Override
        public String describe() {
            return "DistinctByOperator[keyChannel=" + keyChannel + "]";
        }
    }

    private final int keyChannel;
    private final BytesRefHash seenKeys;

    public DistinctByOperator(int keyChannel) {
        this.keyChannel = keyChannel;
        this.seenKeys = new BytesRefHash();
    }

    @Override
    protected Page process(Page page) {
        BytesRefBlock keyBlock = page.getBlock(keyChannel);
        BytesRef scratch = new BytesRef();

        // If the block is a constant vector, all values are the same
        // We only need to check once instead of iterating through all positions
        BytesRefVector vector = keyBlock.asVector();
        if (vector != null && vector.isConstant()) {
            BytesRef key = vector.getBytesRef(0, scratch);
            long result = seenKeys.add(key);
            if (result >= 0) {
                return page.filter(0);
            } else {
                page.releaseBlocks();
                return null;
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

        if (rowCount == 0) {
            page.releaseBlocks();
            return null;
        }
        if (rowCount == page.getPositionCount()) {
            return page;
        }
        positions = Arrays.copyOf(positions, rowCount);
        return page.filter(positions);
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
