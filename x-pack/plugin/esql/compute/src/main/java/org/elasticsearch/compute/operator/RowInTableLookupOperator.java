/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowInTableLookupOperator extends AbstractPageMappingToIteratorOperator {
    public record Key(String name, Block block) {
        @Override
        public String toString() {
            return "{name="
                + name
                + ", type="
                + block.elementType()
                + ", positions="
                + block.getPositionCount()
                + ", size="
                + ByteSizeValue.ofBytes(block.ramBytesUsed())
                + "}";
        }
    }

    /**
     * Factory for {@link RowInTableLookupOperator}. It's received {@link Block}s
     * are never closed, so we need to build them from a non-tracking factory.
     */
    public record Factory(Key[] keys, int[] blockMapping) implements Operator.OperatorFactory {
        public Factory {
            if (keys.length < 1) {
                throw new IllegalArgumentException("expected [keys] to be non-empty");
            }
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new RowInTableLookupOperator(driverContext.blockFactory(), keys, blockMapping);
        }

        @Override
        public String describe() {
            return "RowInTableLookup[keys=" + Arrays.toString(keys) + ", mapping=" + Arrays.toString(blockMapping) + "]";
        }
    }

    private final List<String> keys;
    private final RowInTableLookup lookup;
    private final int[] blockMapping;

    public RowInTableLookupOperator(BlockFactory blockFactory, Key[] keys, int[] blockMapping) {
        if (keys.length < 1) {
            throw new IllegalArgumentException("expected [keys] to be non-empty");
        }
        this.blockMapping = blockMapping;
        this.keys = new ArrayList<>(keys.length);
        Block[] blocks = new Block[keys.length];
        for (int k = 0; k < keys.length; k++) {
            this.keys.add(keys[k].name);
            blocks[k] = keys[k].block;
        }
        this.lookup = RowInTableLookup.build(blockFactory, blocks);
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        Page mapped = page.projectBlocks(blockMapping);
        try {
            // lookup increments any references we need to keep for the iterator
            return appendBlocks(page, lookup.lookup(mapped, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE));
        } finally {
            mapped.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "RowInTableLookup[" + lookup + ", keys=" + keys + ", mapping=" + Arrays.toString(blockMapping) + "]";
    }

    @Override
    public void close() {
        Releasables.close(super::close, lookup);
    }
}
