/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;

import java.util.Arrays;

/**
 * Combines values at the given blocks with the same positions into a single position for the blocks at the given channels
 * Example, input pages consisting of three blocks:
 * positions    | field-1 | field-2 |
 * -----------------------------------
 * Page 1:
 * 1           |  a,b    |   2020  |
 * 1           |  c      |   2021  |
 * ---------------------------------
 * Page 2:
 * 2           |  a,e    |   2021  |
 * ---------------------------------
 * Page 3:
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
    private int filledPositions = 0;
    private final boolean singleMode;
    private final int positionCount;
    private final int positionChannel;

    private final Block.Builder[] builders;
    private final int[] mergingChannels;

    private Page outputPage;

    MergePositionsOperator(boolean singleMode, int positionCount, int positionChannel, int[] mergingChannels, ElementType[] mergingTypes) {
        if (mergingChannels.length != mergingTypes.length) {
            throw new IllegalArgumentException(
                "Merging channels don't match merging types; channels="
                    + Arrays.toString(mergingChannels)
                    + ",types="
                    + Arrays.toString(mergingTypes)
            );
        }
        if (singleMode == false) {
            throw new UnsupportedOperationException("Enrich indices should have single segment");
        }
        this.singleMode = singleMode;
        this.positionCount = positionCount;
        this.positionChannel = positionChannel;
        this.mergingChannels = mergingChannels;
        this.builders = new Block.Builder[mergingTypes.length];
        for (int i = 0; i < mergingTypes.length; i++) {
            builders[i] = mergingTypes[i].newBlockBuilder(positionCount);
        }
    }

    @Override
    public boolean needsInput() {
        return true;
    }

    @Override
    public void addInput(Page page) {
        if (singleMode) {
            mergePage(page);
            return;
        }
        throw new UnsupportedOperationException("Enrich indices should have single segment");
    }

    private void fillNullUpToPosition(int position) {
        while (filledPositions < position) {
            for (Block.Builder builder : builders) {
                builder.appendNull();
            }
            filledPositions++;
        }
    }

    private void mergePage(Page page) {
        IntBlock positions = page.getBlock(positionChannel);
        int currentPosition = positions.getInt(0);
        fillNullUpToPosition(currentPosition);
        for (int i = 0; i < mergingChannels.length; i++) {
            int channel = mergingChannels[i];
            builders[i].appendAllValuesToCurrentPosition(page.getBlock(channel));
        }
        filledPositions++;
    }

    @Override
    public void finish() {
        fillNullUpToPosition(positionCount);
        Block[] blocks = Arrays.stream(builders).map(Block.Builder::build).toArray(Block[]::new);
        outputPage = new Page(blocks);
        finished = true;
        assert outputPage.getPositionCount() == positionCount;
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

    }
}
