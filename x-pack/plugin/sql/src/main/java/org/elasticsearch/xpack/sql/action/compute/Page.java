/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import java.util.Arrays;
import java.util.Objects;

public class Page {

    private static final Block[] EMPTY_BLOCKS = new Block[0];

    private final Block[] blocks;
    private final int positionCount;

    public Page(Block... blocks) {
        this(true, determinePositionCount(blocks), blocks);
    }

    public Page(int positionCount) {
        this(false, positionCount, EMPTY_BLOCKS);
    }

    public Page(int positionCount, Block... blocks) {
        this(true, positionCount, blocks);
    }

    private Page(boolean blocksCopyRequired, int positionCount, Block[] blocks) {
        Objects.requireNonNull(blocks, "blocks is null");
        this.positionCount = positionCount;
        if (blocks.length == 0) {
            this.blocks = EMPTY_BLOCKS;
        } else {
            this.blocks = blocksCopyRequired ? blocks.clone() : blocks;
        }
    }

    private static int determinePositionCount(Block... blocks) {
        Objects.requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }

        return blocks[0].getPositionCount();
    }

    public Block getBlock(int channel) {
        return blocks[channel];
    }

    public Page appendColumn(Block block) {
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + 1);
        newBlocks[blocks.length] = block;
        return wrapBlocksWithoutCopy(positionCount, newBlocks);
    }

    static Page wrapBlocksWithoutCopy(int positionCount, Block[] blocks) {
        return new Page(false, positionCount, blocks);
    }

    @Override
    public String toString() {
        return "Page{" +
            "blocks=" + Arrays.toString(blocks) +
            '}';
    }

    public int getPositionCount() {
        return positionCount;
    }
}
