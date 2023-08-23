/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Assertions;

import java.io.IOException;
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
public final class Page implements Writeable {

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
        if (Assertions.ENABLED) {
            for (Block b : blocks) {
                assert b.getPositionCount() == positionCount : "expected positionCount=" + positionCount + " but was " + b;
            }
        }
    }

    public Page(StreamInput in) throws IOException {
        int positionCount = in.readVInt();
        int blockPositions = in.readVInt();
        Block[] blocks = new Block[blockPositions];
        for (int blockIndex = 0; blockIndex < blockPositions; blockIndex++) {
            blocks[blockIndex] = in.readNamedWriteable(Block.class);
        }
        this.positionCount = positionCount;
        this.blocks = blocks;
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
    public <B extends Block> B getBlock(int blockIndex) {
        @SuppressWarnings("unchecked")
        B block = (B) blocks[blockIndex];
        return block;
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
     * Creates a new page, appending the given blocks to the existing blocks in this Page.
     *
     * @param toAdd the blocks to append
     * @return a new Page with the block appended
     * @throws IllegalArgumentException if one of the given blocks does not have the same number of
     *        positions as the blocks in this Page
     */
    public Page appendBlocks(Block[] toAdd) {
        for (Block block : toAdd) {
            if (positionCount != block.getPositionCount()) {
                throw new IllegalArgumentException("Block does not have same position count");
            }
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + toAdd.length);
        for (int i = 0; i < toAdd.length; i++) {
            newBlocks[blocks.length + i] = toAdd[i];
        }
        return new Page(false, positionCount, newBlocks);
    }

    /**
     * Creates a new page, appending the blocks of the given block to the existing blocks in this Page.
     *
     * @param toAdd the page to append
     * @return a new Page
     * @throws IllegalArgumentException if any blocks of the given page does not have the same number of
     *                                  positions as the blocks in this Page
     */
    public Page appendPage(Page toAdd) {
        return appendBlocks(toAdd.blocks);
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
        return positionCount == page.positionCount
            && (positionCount == 0 || Arrays.equals(blocks, 0, blocks.length, page.blocks, 0, page.blocks.length));
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(positionCount);
        out.writeVInt(getBlockCount());
        for (Block block : blocks) {
            out.writeNamedWriteable(block);
        }
    }

    public static class PageWriter implements Writeable.Writer<Page> {

        @Override
        public void write(StreamOutput out, Page value) throws IOException {
            value.writeTo(out);
        }
    }

    public static class PageReader implements Writeable.Reader<Page> {

        @Override
        public Page read(StreamInput in) throws IOException {
            return new Page(in);
        }
    }
}
