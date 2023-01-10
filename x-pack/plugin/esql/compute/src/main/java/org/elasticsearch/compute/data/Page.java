/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.ann.Experimental;

import java.util.Arrays;
import java.util.Objects;

/**
 * A page is a column-oriented data abstraction that allows data to be passed between operators in
 * batches.
 *
 * <p> A page has a fixed number of positions (or rows), exposed via {@link #getPositionCount()}.
 * It is further composed of a number of {@link Block}s, which represent the columnar data.
 * The number of blocks can be retrieved via {@link #getBlockCount()}, and the respective
 * blocks can be retrieved via their index {@link #getBlock(int)}.
 *
 * <p> Pages are immutable and can be passed between threads.
 */
public final class Page {

    private final Block[] blocks;

    private final int positionCount;

    /**
     * Creates a new page with the given blocks. Every block has the same number of positions.
     *
     * @param blocks the blocks
     * @throws IllegalArgumentException if all blocks do not have the same number of positions
     */
    public Page(Block... blocks) {
        this(true, determinePositionCount(blocks), blocks);
    }

    /**
     * Creates a new page with the given positionCount and blocks. Assumes that every block has the
     * same number of positions as the positionCount that's passed in - there is no validation of
     * this.
     *
     * @param positionCount the block position count
     * @param blocks the blocks
     */
    public Page(int positionCount, Block... blocks) {
        this(true, positionCount, blocks);
    }

    private Page(boolean copyBlocks, int positionCount, Block[] blocks) {
        Objects.requireNonNull(blocks, "blocks is null");
        // assert assertPositionCount(blocks);
        this.positionCount = positionCount;
        this.blocks = copyBlocks ? blocks.clone() : blocks;
    }

    private static boolean assertPositionCount(Block... blocks) {
        int count = determinePositionCount(blocks);
        return Arrays.stream(blocks).map(Block::getPositionCount).allMatch(pc -> pc == count);
    }

    private static int determinePositionCount(Block... blocks) {
        Objects.requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }
        return blocks[0].getPositionCount();
    }

    /**
     * Returns the block at the given block index.
     *
     * @param blockIndex the block index
     * @return the block
     */
    public Block getBlock(int blockIndex) {
        return blocks[blockIndex];
    }

    /**
     * Creates a new page, appending the given block to the existing blocks in this Page.
     *
     * @param block the block to append
     * @return a new Page with the block appended
     * @throws IllegalArgumentException if the given block does not have the same number of
     *         positions as the blocks in this Page
     */
    public Page appendBlock(Block block) {
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + 1);
        newBlocks[blocks.length] = block;
        return new Page(false, positionCount, newBlocks);
    }

    /**
     * Creates a new page, replacing a block at the given index with a new block.
     *
     * @param blockIndex the index of the block to replace
     * @param block the replacement block
     * @return a new Page with the block replaced
     * @throws IllegalArgumentException if the given block does not have the same number of
     *         positions as the blocks in this Page
     */
    public Page replaceBlock(int blockIndex, Block block) {
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length);
        newBlocks[blockIndex] = block;
        return new Page(false, positionCount, newBlocks);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(positionCount);
        for (int i = 0; i < blocks.length; i++) {
            result = 31 * result + Objects.hashCode(blocks[i]);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Page page = (Page) o;
        return positionCount == page.positionCount && Arrays.equals(blocks, 0, positionCount, page.blocks, 0, positionCount);
    }

    @Override
    public String toString() {
        return "Page{" + "blocks=" + Arrays.toString(blocks) + '}';
    }

    /**
     * Returns the number of positions (rows) in this page.
     *
     * @return the number of positions
     */
    public int getPositionCount() {
        return positionCount;
    }

    /**
     * Returns the number of blocks in this page. Blocks can then be retrieved via
     * {@link #getBlock(int)} where channel ranges from 0 to {@code getBlockCount}.
     *
     * @return the number of blocks in this page
     */
    public int getBlockCount() {
        return blocks.length;
    }

    @Experimental
    public Page getRow(int position) {
        Block[] newBlocks = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            newBlocks[i] = blocks[i].getRow(position);
        }
        return new Page(false, 1, newBlocks);
    }
}
