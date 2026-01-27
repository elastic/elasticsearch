/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;

/**
 * {@link RowInTableLookup} that models an increasing sequence of integers.
 */
public final class AscendingSequenceRowInTableLookup extends RowInTableLookup {
    private final BlockFactory blockFactory;
    private final int min;
    private final int max;

    public AscendingSequenceRowInTableLookup(BlockFactory blockFactory, int min, int max) {
        this.blockFactory = blockFactory;
        this.min = min;
        this.max = max;
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        IntBlock block = page.getBlock(0);
        IntVector vector = block.asVector();
        int target = Math.toIntExact(targetBlockSize.getBytes());
        if (vector != null && vector.getPositionCount() * Integer.BYTES < target) {
            return ReleasableIterator.single(lookupVector(vector));
        }
        return new Lookup(block, target);
    }

    private IntBlock lookupVector(IntVector vector) {
        if (vector.min() >= min && vector.max() < max) {
            if (min == 0) {
                vector.incRef();
                return vector.asBlock();
            }
            return lookupVectorInRange(vector).asBlock();
        }
        return lookupVectorMaybeInRange(vector);
    }

    private IntVector lookupVectorInRange(IntVector vector) {
        try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(vector.getPositionCount())) {
            for (int i = 0; i < vector.getPositionCount(); i++) {
                builder.appendInt(i, vector.getInt(i) - min);
            }
            return builder.build();
        }
    }

    private IntBlock lookupVectorMaybeInRange(IntVector vector) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(vector.getPositionCount())) {
            for (int i = 0; i < vector.getPositionCount(); i++) {
                int v = vector.getInt(i);
                if (v < min || v >= max) {
                    builder.appendNull();
                } else {
                    builder.appendInt(v - min);
                }
            }
            return builder.build();
        }
    }

    @Override
    public String toString() {
        return "AscendingSequence[" + min + "-" + max + "]";
    }

    private class Lookup implements ReleasableIterator<IntBlock> {
        private final IntBlock block;
        private final int target;

        int p;

        private Lookup(IntBlock block, int target) {
            this.block = block;
            this.target = target;
            block.incRef();
        }

        @Override
        public boolean hasNext() {
            return p < block.getPositionCount();
        }

        @Override
        public IntBlock next() {
            int initialEntries = Math.min(target / Integer.BYTES / 2, block.getPositionCount() - p);
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(initialEntries)) {
                if (builder.estimatedBytes() > target) {
                    throw new IllegalStateException(
                        "initial builder overshot target [" + builder.estimatedBytes() + "] vs [" + target + "]"
                    );
                }
                while (p < block.getPositionCount() && builder.estimatedBytes() < target) {
                    int start = block.getFirstValueIndex(p);
                    int end = start + block.getValueCount(p);
                    int first = -1;
                    boolean started = false;
                    for (int i = start; i < end; i++) {
                        int v = block.getInt(i);
                        if (v < min || v >= max) {
                            continue;
                        }
                        if (first < 0) {
                            first = v - min;
                            continue;
                        }
                        if (started == false) {
                            builder.beginPositionEntry();
                            builder.appendInt(first);
                            started = true;
                        }
                        builder.appendInt(v - min);
                    }
                    p++;
                    if (started) {
                        builder.endPositionEntry();
                        continue;
                    }
                    if (first < 0) {
                        builder.appendNull();
                        continue;
                    }
                    builder.appendInt(first);
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            block.decRef();
        }

        @Override
        public String toString() {
            return "AscendingSequence[" + p + "/" + block.getPositionCount() + "]";
        }
    }

    @Override
    public void close() {}
}
