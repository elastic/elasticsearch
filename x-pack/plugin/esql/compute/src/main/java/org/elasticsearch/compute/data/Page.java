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
import org.elasticsearch.core.Releasables;

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
// TODO: Remove deprecated methods and ctors.
public final class Page implements Writeable {
    private final Block.Ref[] blocks;
    private final int positionCount;

    /**
     * True if we've called {@link #releaseBlocks()} which causes us to remove the
     * circuit breaker for the {@link Block}s. The {@link Page} reference should be
     * removed shortly after this and reading {@linkplain Block}s after release
     * will fail.
     */
    private boolean blocksReleased = false;

    /**
     * Creates a new page with the given blocks. Every block has the same number of positions.
     *
     * @param blocks the blocks
     * @throws IllegalArgumentException if all blocks do not have the same number of positions
     * @deprecated use {@link Page#Page(Block.Ref...)} instead
     */
    @Deprecated
    public Page(Block... blocks) {
        this(blockArrayToRefs(blocks));
    }

    /**
     * Creates a new page with the given positionCount and blocks. Assumes that every block has the
     * same number of positions as the positionCount that's passed in - there is no validation of
     * this.
     *
     * @param positionCount the block position count
     * @param blocks the blocks
     * @deprecated use {@link Page#Page(int, Block.Ref...)} instead
     */
    @Deprecated
    public Page(int positionCount, Block... blocks) {
        this(positionCount, blockArrayToRefs(blocks));
    }

    /**
     * Creates a new page with the given blocks. Every block has the same number of positions.
     *
     * @param blocks the blocks
     * @throws IllegalArgumentException if all blocks do not have the same number of positions
     */
    public Page(Block.Ref... blocks) {
        this(determinePositionCount(blocks), blocks);
    }

    /**
     * Creates a new page with the given positionCount and blocks. Assumes that every block has the
     * same number of positions as the positionCount that's passed in - there is no validation of
     * this.
     *
     * @param positionCount the block position count
     * @param blocks the blocks
     */
    public Page(int positionCount, Block.Ref... blocks) {
        Objects.requireNonNull(blocks, "blocks is null");
        // assert assertPositionCount(blocks);
        this.positionCount = positionCount;
        this.blocks = blocks.clone();
        for (Block.Ref bRef : blocks) {
            Block b = bRef.block();
            assert b.getPositionCount() == positionCount : "expected positionCount=" + positionCount + " but was " + b;
            if (b.isReleased()) {
                throw new IllegalArgumentException("can't build page out of released blocks but [" + b + "] was released");
            }
        }
    }

    /**
     * Appending ctor, see {@link #appendBlocks}.
     */
    private Page(Page prev, Block.Ref[] toAdd) {
        for (Block.Ref blockRef : toAdd) {
            if (prev.positionCount != blockRef.block().getPositionCount()) {
                throw new IllegalArgumentException("Block does not have same position count");
            }
        }
        this.positionCount = prev.positionCount;

        this.blocks = Arrays.copyOf(prev.blocks, prev.blocks.length + toAdd.length);
        for (int i = 0; i < toAdd.length; i++) {
            this.blocks[prev.blocks.length + i] = toAdd[i];
        }
    }

    public Page(StreamInput in) throws IOException {
        int positionCount = in.readVInt();
        int blockPositions = in.readVInt();
        Block.Ref[] blocks = new Block.Ref[blockPositions];
        boolean success = false;
        try {
            for (int blockIndex = 0; blockIndex < blockPositions; blockIndex++) {
                blocks[blockIndex] = new Block.Ref(in.readNamedWriteable(Block.class));
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
        this.positionCount = positionCount;
        this.blocks = blocks;
    }

    private static int determinePositionCount(Block.Ref... blocks) {
        Objects.requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }
        return blocks[0].block().getPositionCount();
    }

    /**
     * Returns the block at the given block index.
     *
     * @param blockIndex the block index
     * @return the block
     * @deprecated use {@link Page#getBlockRef} instead
     */
    @Deprecated
    public <B extends Block> B getBlock(int blockIndex) {
        if (blocksReleased) {
            throw new IllegalStateException("can't read released page");
        }
        return blocks[blockIndex].block();
    }

    /**
     * Returns a reference to the block at the given block index.
     *
     * @param blockIndex the block index
     * @return the block
     */
    public Block.Ref getBlockRef(int blockIndex) {
        if (blocksReleased) {
            throw new IllegalStateException("can't read released page");
        }
        return blocks[blockIndex];
    }

    /**
     * Creates a new page, appending the given block to the existing blocks in this Page.
     *
     * @param block the block to append
     * @return a new Page with the block appended
     * @throws IllegalArgumentException if the given block does not have the same number of
     *         positions as the blocks in this Page
     * @deprecated use {@link Page#appendBlock(Block.Ref)} instead
     */
    @Deprecated
    public Page appendBlock(Block block) {
        return new Page(this, new Block.Ref[] { block.asRef() });
    }

    /**
     * Creates a new page, appending the given blocks to the existing blocks in this Page.
     *
     * @param toAdd the blocks to append
     * @return a new Page with the block appended
     * @throws IllegalArgumentException if one of the given blocks does not have the same number of
     *        positions as the blocks in this Page
     * @deprecated use {@link Page#appendBlocks(Block.Ref[])} instead
     */
    @Deprecated
    public Page appendBlocks(Block[] toAdd) {
        return new Page(this, blockArrayToRefs(toAdd));
    }

    /**
     * Creates a new page, appending the given block to the existing blocks in this Page.
     *
     * @param block the block to append
     * @return a new Page with the block appended
     * @throws IllegalArgumentException if the given block does not have the same number of
     *         positions as the blocks in this Page
     */
    public Page appendBlock(Block.Ref block) {
        return new Page(this, new Block.Ref[] { block });
    }

    /**
     * Creates a new page, appending the given blocks to the existing blocks in this Page.
     *
     * @param toAdd the blocks to append
     * @return a new Page with the block appended
     * @throws IllegalArgumentException if one of the given blocks does not have the same number of
     *        positions as the blocks in this Page
     */
    public Page appendBlocks(Block.Ref[] toAdd) {
        return new Page(this, toAdd);
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
            result = 31 * result + Objects.hashCode(blocks[i].block());
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Page page = (Page) o;

        if ((positionCount == page.positionCount) == false || (blocks.length == page.blocks.length) == false) return false;
        if (positionCount == 0) return true;
        for (int i = 0; i < blocks.length; i++) {
            if (blocks[i].block().equals(page.blocks[i].block()) == false) return false;
        }
        return true;
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
        for (Block.Ref blockRef : blocks) {
            out.writeNamedWriteable(blockRef.block());
        }
    }

    /**
     * Release all blocks in this page, decrementing any breakers accounting for these blocks.
     */
    public void releaseBlocks() {
        blocksReleased = true;
        Releasables.closeExpectNoException(blocks);
    }

    private static Block.Ref[] blockArrayToRefs(Block[] blocks) {
        return Arrays.stream(blocks).map(Block::asRef).toArray(Block.Ref[]::new);
    }
}
