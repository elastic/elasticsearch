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
import java.util.List;

public class ProjectOperator extends AbstractPageMappingOperator {
    public record ProjectOperatorFactory(List<Integer> projection) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ProjectOperator(projection);
        }

        @Override
        public String describe() {
            if (projection.size() < 10) {
                return "ProjectOperator[projection = " + projection + "]";
            }
            return "ProjectOperator[projection = [" + projection.size() + " fields]]";
        }
    }

    private final int[] projection;
    private final Block[] blocks;

    /**
     * Creates an operator that applies the given projection, encoded as an integer list where
     * the ordinal indicates the output order and the value, the backing channel that to be used.
     * Given the input {a,b,c,d}, project {a,d,a} is encoded as {0,3,0}.
     *
     * @param projection list of blocks to keep and their order.
     */
    public ProjectOperator(List<Integer> projection) {
        this.projection = projection.stream().mapToInt(Integer::intValue).toArray();
        this.blocks = new Block[projection.size()];
    }

    @Override
    protected Page process(Page page) {
        var blockCount = page.getBlockCount();
        if (blockCount == 0) {
            return page;
        }
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
        Page output = new Page(positionCount, blocks);
        Arrays.fill(blocks, null);
        return output;
    }

    @Override
    public String toString() {
        if (projection.length < 10) {
            return "ProjectOperator[projection = " + Arrays.toString(projection) + "]";
        }
        return "ProjectOperator[projection = [" + projection.length + " fields]]";
    }
}
