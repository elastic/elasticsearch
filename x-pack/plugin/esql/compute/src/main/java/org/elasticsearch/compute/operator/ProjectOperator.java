/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

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
        List<Releasable> blocksToRelease = new ArrayList<>();
        for (int i = 0; i < page.getBlockCount(); i++) {
            var block = page.getBlock(i);
            if (bs.get(i)) {
                assertNotReleasing(blocksToRelease, block);
                blocks[b++] = block;
            } else {
                blocksToRelease.add(block);
            }
        }
        Releasables.close(blocksToRelease);
        return new Page(positionCount, blocks);
    }

    @Override
    public String toString() {
        return "ProjectOperator[mask = " + bs + ']';
    }

    static void assertNotReleasing(List<Releasable> toRelease, Block toKeep) {
        // verify by identity equality
        assert toRelease.stream().anyMatch(r -> r == toKeep) == false
            : "both releasing and keeping the same block: " + toRelease.stream().filter(r -> r == toKeep).toList();
    }
}
