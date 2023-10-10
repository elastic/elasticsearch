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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ProjectOperator extends AbstractPageMappingOperator {

    private final Set<Integer> pagesUsed;
    private final int[] projection;
    private Block[] blocks;

    public record ProjectOperatorFactory(List<Integer> projection) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ProjectOperator(projection);
        }

        @Override
        public String describe() {
            return "ProjectOperator[projection = " + projection + "]";
        }
    }

    /**
     * Creates an operator that applies the given projection, encoded as an integer list where
     * the ordinal indicates the output order and the value, the backing channel that to be used.
     * Given the input {a,b,c,d}, project {a,d,a} is encoded as {0,3,0}.
     *
     * @param projection list of blocks to keep and their order.
     */
    public ProjectOperator(List<Integer> projection) {
        this.pagesUsed = new HashSet<>(projection);
        this.projection = projection.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    protected Page process(Page page) {
        var blockCount = page.getBlockCount();
        if (blockCount == 0) {
            return page;
        }
        if (blocks == null) {
            blocks = new Block[projection.length];
        }

        Arrays.fill(blocks, null);
        int b = 0;
        for (int source : projection) {
            if (source >= blockCount) {
                throw new IllegalArgumentException(
                    "Cannot project block with index [" + source + "] from a page with size [" + blockCount + "]"
                );
            }
            var block = page.getBlock(source);
            blocks[b++] = block;
            block.incRef();
        }
        int positionCount = page.getPositionCount();
        page.releaseBlocks();
        // Use positionCount explicitly to avoid re-computing - also, if the projection is empty, there may be
        // no more blocks left to determine the positionCount from.
        return new Page(positionCount, blocks);
    }

    @Override
    public String toString() {
        return "ProjectOperator[projection = " + Arrays.toString(projection) + ']';
    }

    static void assertNotReleasing(List<Releasable> toRelease, Block toKeep) {
        // verify by identity equality
        assert toRelease.stream().anyMatch(r -> r == toKeep) == false
            : "both releasing and keeping the same block: " + toRelease.stream().filter(r -> r == toKeep).toList();
    }
}
