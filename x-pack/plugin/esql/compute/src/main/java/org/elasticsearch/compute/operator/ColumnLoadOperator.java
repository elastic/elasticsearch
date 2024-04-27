/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ColumnLoadOperator extends AbstractPageMappingToIteratorOperator {
    /**
     * Factory for {@link ColumnLoadOperator}. It's received {@link Block}s
     * are never closed, so we need to build them from a non-tracking factory.
     */
    public static class Factory implements OperatorFactory {
        private final Block values;

        public Factory(Block values) {
            this.values = values;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new ColumnLoadOperator(driverContext.blockFactory(), values);
        }

        @Override
        public String describe() {
            return "ColumnLoad[" + values.elementType() + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final Block values;

    public ColumnLoadOperator(BlockFactory blockFactory, Block values) {
        this.blockFactory = blockFactory;
        this.values = values;
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        Page mapped = page.projectBlocks(blockMapping);
        page.releaseBlocks();
        return appendBlocks(mapped, hash.lookup(mapped, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE));
    }

    @Override
    public String toString() {
        return "ColumnLoad[" + values.elementType() + "]";
    }

    @Override
    public void close() {
        Releasables.close(super::close, values);
    }

    class Ints implements ReleasableIterator<IntBlock> {
        private final IntBlock values;
        private int positionOffset;
        

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public IntBlock next() {
            return null;
        }

        @Override
        public void close() {

        }
    }
}
