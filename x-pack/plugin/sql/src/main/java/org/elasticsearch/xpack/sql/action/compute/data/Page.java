/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.data;

import org.elasticsearch.xpack.sql.action.compute.operator.Operator;

import java.util.Arrays;
import java.util.Objects;

/**
 * A page is a column-oriented data abstraction that allows data to be passed
 * between {@link Operator}s in terms of small batches of data. Pages are immutable
 * and can be passed between threads.
 *
 * A page has a fixed number of positions (or rows), exposed via {@link #getPositionCount()}.
 * It is further composed of a number of {@link Block}s, which represent the columnar data.
 * The number of blocks can be retrieved via {@link #getBlockCount()}, and the respective
 * blocks can be retrieved via their index {@link #getBlock(int)}. The index of these
 * blocks in the page are referred to as channels.
 */
public class Page {

    private static final Block[] EMPTY_BLOCKS = new Block[0];

    private final Block[] blocks;
    private final int positionCount;

    /**
     * Creates a new page with the given blocks. Requires every block to have the same number of positions.
     */
    public Page(Block... blocks) {
        this(true, determinePositionCount(blocks), blocks);
    }

    /**
     * Creates a new page with the given positionCount and blocks. Assumes that every block has the same number of positions as the
     * positionCount that's passed in.
     */
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

    /**
     * Returns the block at the given channel position
     */
    public Block getBlock(int channel) {
        return blocks[channel];
    }

    /**
     * Creates a new page, appending the given block to the existing list of blocks
     */
    public Page appendColumn(Block block) {
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + 1);
        newBlocks[blocks.length] = block;
        return new Page(false, positionCount, newBlocks);
    }

    @Override
    public String toString() {
        return "Page{" + "blocks=" + Arrays.toString(blocks) + '}';
    }

    /**
     * Returns the number of positions (rows) in this page
     */
    public int getPositionCount() {
        return positionCount;
    }

    /**
     * Returns the number of blocks in this page. Blocks can then be retrieved via
     * {@link #getBlock(int)} where channel ranges from 0 to {@link #getBlockCount()}
     */
    public int getBlockCount() {
        return blocks.length;
    }
}
