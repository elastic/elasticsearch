/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * {@link Block#lookup Looks up} values from a provided {@link Block} and
 * merged them into each {@link Page}.
 */
public class ColumnLoadOperator extends AbstractPageMappingToIteratorOperator {
    public record Values(String name, Block block) {
        @Override
        public String toString() {
            return name + ":" + block.elementType();
        }
    }

    /**
     * Factory for {@link ColumnLoadOperator}. The {@link Block} inside {@link Values}
     * is never closed by this factory; each operator instance deep-copies it into its
     * own {@link BlockFactory} on construction.
     */
    public record Factory(Values values, int positionsOrd) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ColumnLoadOperator(new Values(values.name, values.block.deepCopy(driverContext.blockFactory())), positionsOrd);
        }

        @Override
        public String describe() {
            return "ColumnLoad[values=" + values + ", positions=" + positionsOrd + "]";
        }
    }

    private final Values values;
    private final int positionsOrd;

    public ColumnLoadOperator(Values values, int positionsOrd) {
        this.positionsOrd = positionsOrd;
        this.values = values;
    }

    static final long MIN_TARGET_BLOCK_BYTES = ByteSizeValue.ofKb(10).getBytes();

    /**
     * The target size of each loaded block. Each loaded block gets an equal share of the page
     * budget, so pages the source already sized to fit {@link Operator#TARGET_PAGE_SIZE} pass
     * through whole. The floor keeps pages with many columns from being cut into minimum-size pages.
     */
    static ByteSizeValue targetBlockSize(int inputBlockCount) {
        return ByteSizeValue.ofBytes(Math.max(MIN_TARGET_BLOCK_BYTES, Operator.TARGET_PAGE_SIZE / (inputBlockCount + 1)));
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        return appendBlocks(page, values.block.lookup(page.getBlock(positionsOrd), targetBlockSize(page.getBlockCount())));
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(values.block, super::close);
    }

    @Override
    public String toString() {
        return "ColumnLoad[values=" + values + ", positions=" + positionsOrd + "]";
    }
}
