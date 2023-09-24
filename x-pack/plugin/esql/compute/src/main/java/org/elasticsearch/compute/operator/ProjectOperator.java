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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ProjectOperator extends AbstractPageMappingOperator {

    private final List<Integer> projection;
    private Block[] blocks;

    public record ProjectOperatorFactory(List<Integer> projection) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ProjectOperator(projection);
        }

        @Override
        public String describe() {
            return "ProjectOperator[mask = " + projection + "]";
        }
    }

    /**
     * Creates a project that applies the given projection, encoded as an integer list where
     * the ordinal indicates the output order and the value, the backing channel that to be used.
     * Given the input {a,b,c,d}, project {a,d,a} is encoded as {0,3,0}.
     *
     * @param projection list of blocks to keep and their order.
     */
    public ProjectOperator(List<Integer> projection) {
        this.projection = projection;
    }

    @Override
    protected Page process(Page page) {
        if (page.getBlockCount() == 0) {
            return page;
        }
        if (blocks == null) {
            blocks = new Block[projection.size()];
        }

        Arrays.fill(blocks, null);
        int b = 0;
        int positionCount = page.getPositionCount();
        for (Integer source : projection) {
            if (source >= page.getBlockCount()) {
                throw new IllegalArgumentException("Cannot project blocks outside of a Page");
            }
            var block = page.getBlock(source);
            blocks[b++] = block;
        }
        // iterate the blocks to see which one isn't used
        List<Releasable> blocksToRelease = new ArrayList<>();

        Set<Integer> offsets = new HashSet<>(projection);
        for (int i = 0; i < page.getBlockCount(); i++) {
            if (offsets.contains(i) == false) {
                blocksToRelease.add(page.getBlock(i));
            }
        }
        Releasables.close(blocksToRelease);

        return new Page(positionCount, blocks);
    }

    @Override
    public String toString() {
        return "ProjectOperator[mask = " + projection + ']';
    }
}
