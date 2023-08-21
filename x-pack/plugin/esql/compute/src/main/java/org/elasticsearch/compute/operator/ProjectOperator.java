/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.Arrays;
import java.util.BitSet;

public class ProjectOperator extends AbstractPageMappingOperator {

    private final BitSet bs;
    private Block[] blocks;

    public record ProjectOperatorFactory(BitSet mask) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ProjectOperator(mask);
        }

        @Override
        public String describe() {
            return "ProjectOperator[mask = " + mask + "]";
        }
    }

    /**
     * Creates a project that applies the given mask (as a bitset).
     *
     * @param mask bitset mask for enabling/disabling blocks / columns inside a Page
     */
    public ProjectOperator(BitSet mask) {
        this.bs = mask;
    }

    @Override
    protected Page process(Page page) {
        if (page.getBlockCount() == 0) {
            return page;
        }
        if (blocks == null) {
            blocks = new Block[bs.cardinality()];
        }

        Arrays.fill(blocks, null);
        int b = 0;
        int positionCount = page.getPositionCount();
        for (int i = bs.nextSetBit(0); i >= 0 && i < page.getBlockCount(); i = bs.nextSetBit(i + 1)) {
            var block = page.getBlock(i);
            blocks[b++] = block;
        }
        return new Page(positionCount, blocks);
    }

    @Override
    public String toString() {
        return "ProjectOperator[mask = " + bs + ']';
    }
}
