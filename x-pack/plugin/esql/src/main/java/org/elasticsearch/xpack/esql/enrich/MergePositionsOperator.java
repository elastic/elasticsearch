/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.Objects;

/**
 * Combines values at the given blocks with the same positions into a single position for the blocks at the given channels
 * Example, input pages consisting of three blocks:
 * positions    | field-1 | field-2 |
 * -----------------------------------
 * Page 1:
 * 1           |  a,b    |   2020  |
 * 1           |  c      |   2021  |
 * 2           |  a,e    |   2021  |
 * ---------------------------------
 * Page 2:
 * 4           |  d      |   null  |
 * ---------------------------------
 * Output:
 * |  field-1   | field-2    |
 * ---------------------------
 * |  null      | null       |
 * |  a,b,c     | 2020,2021  |
 * |  a,e       | 2021       |
 * |  null      | null       |
 * |  d         | 2023       |
 */
final class MergePositionsOperator implements Operator {
    private boolean finished = false;
    private final int totalPositions;
    private int filledPositions;
    private final int positionChannel;

    private final Block.Builder[] outputBuilders;
    private final int[] mergingChannels;

    private Page outputPage;

    MergePositionsOperator(
        int totalPositions,
        int positionChannel,
        int[] mergingChannels,
        ElementType[] mergingTypes,
        BlockFactory blockFactory
    ) {
        if (mergingChannels.length != mergingTypes.length) {
            throw new IllegalArgumentException(
                "Merging channels don't match merging types; channels="
                    + Arrays.toString(mergingChannels)
                    + ",types="
                    + Arrays.toString(mergingTypes)
            );
        }
        this.totalPositions = totalPositions;
        this.positionChannel = positionChannel;
        this.mergingChannels = mergingChannels;
        this.outputBuilders = new Block.Builder[mergingTypes.length];
        try {
            for (int i = 0; i < mergingTypes.length; i++) {
                outputBuilders[i] = mergingTypes[i].newBlockBuilder(totalPositions, blockFactory);
            }
        } finally {
            if (outputBuilders[outputBuilders.length - 1] == null) {
                Releasables.close(outputBuilders);
            }
        }
    }

    @Override
    public boolean needsInput() {
        return true;
    }

    @Override
    public void addInput(Page page) {
        try {
            var positions = Objects.requireNonNull(((IntBlock) page.getBlock(positionChannel)).asVector(), "positions vectors");
            int lastIndex = 0;
            for (int i = 1; i <= positions.getPositionCount(); i++) {
                int p0 = positions.getInt(i - 1);
                if (i == positions.getPositionCount() || positions.getInt(i) != p0) {
                    fillNullUpToPosition(p0);
                    for (int c = 0; c < mergingChannels.length; c++) {
                        Block block = page.getBlock(mergingChannels[c]);
                        copyFrom(block, outputBuilders[c], lastIndex, i);
                    }
                    filledPositions++;
                    lastIndex = i;
                }
            }
        } finally {
            Releasables.closeExpectNoException(page::releaseBlocks);
        }
    }

    void fillNullUpToPosition(int upToPosition) {
        while (filledPositions < upToPosition) {
            for (Block.Builder builder : outputBuilders) {
                builder.appendNull();
            }
            filledPositions++;
        }
    }

    @Override
    public void finish() {
        fillNullUpToPosition(totalPositions);
        final Block[] blocks = Block.Builder.buildAll(outputBuilders);
        outputPage = new Page(blocks);
        assert outputPage.getPositionCount() == totalPositions : outputPage.getPositionCount() + " != " + totalPositions;
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && outputPage == null;
    }

    @Override
    public Page getOutput() {
        Page page = this.outputPage;
        this.outputPage = null;
        return page;
    }

    @Override
    public void close() {
        Releasables.close(outputBuilders);
    }

    static void copyFrom(Block src, Block.Builder dst, int fromPosition, int toPosition) {
        int totalCount = 0;
        for (int i = fromPosition; i < toPosition; i++) {
            totalCount += src.getValueCount(i);
        }
        if (totalCount == 0) {
            dst.appendNull();
            return;
        }
        if (totalCount > 1) {
            dst.beginPositionEntry();
        }
        for (int i = fromPosition; i < toPosition; i++) {
            int firstValueIndex = src.getFirstValueIndex(i);
            int valueCount = src.getValueCount(i);
            for (int v = 0; v < valueCount; v++) {
                dst.appendFrom(src, firstValueIndex + v);
            }
        }
        if (totalCount > 1) {
            dst.endPositionEntry();
        }
    }
}
