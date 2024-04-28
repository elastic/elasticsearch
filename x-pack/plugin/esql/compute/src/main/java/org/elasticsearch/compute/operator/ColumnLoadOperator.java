/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

public class ColumnLoadOperator extends AbstractPageMappingToIteratorOperator {
    /**
     * Factory for {@link ColumnLoadOperator}. It's received {@link Block}s
     * are never closed, so we need to build them from a non-tracking factory.
     */
    public record Factory(Block values, int positionsOrd) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ColumnLoadOperator(values, positionsOrd);
        }

        @Override
        public String describe() {
            return "ColumnLoad[type=" + values.elementType() + ", positions=" + positionsOrd + "]";
        }
    }

    private final Block values;
    private final int positionsOrd;

    public ColumnLoadOperator(Block values, int positionsOrd) {
        this.values = values;
        this.positionsOrd = positionsOrd;
    }

    /**
     * The target size of each loaded block.
     * TODO target the size more intelligently
     */
    static final ByteSizeValue TARGET_BLOCK_SIZE = ByteSizeValue.ofKb(10);

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        return appendBlocks(page, values.lookup(page.getBlock(positionsOrd), TARGET_BLOCK_SIZE));
    }

    @Override
    public String toString() {
        return "ColumnLoad[" + values.elementType() + "]";
    }

    @Override
    public void close() {
        Releasables.close(super::close, values);
    }
}
