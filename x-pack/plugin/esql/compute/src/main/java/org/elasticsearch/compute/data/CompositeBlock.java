/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class CompositeBlock extends AbstractNonThreadSafeRefCounted implements Block {
    private final Block[] blocks;
    private final int positionCount;

    public CompositeBlock(Block[] blocks) {
        if (blocks == null || blocks.length == 0) {
            throw new IllegalArgumentException("must have at least one block; got " + Arrays.toString(blocks));
        }
        this.blocks = blocks;
        this.positionCount = blocks[0].getPositionCount();
        for (Block b : blocks) {
            assert b.getPositionCount() == positionCount : "expected positionCount=" + positionCount + " but was " + b;
            if (b.getPositionCount() != positionCount) {
                assert false : "expected positionCount=" + positionCount + " but was " + b;
                throw new IllegalArgumentException("expected positionCount=" + positionCount + " but was " + b);
            }
            if (b.isReleased()) {
                assert false : "can't build composite block out of released blocks but [" + b + "] was released";
                throw new IllegalArgumentException("can't build composite block out of released blocks but [" + b + "] was released");
            }
        }
    }

    @Override
    public Vector asVector() {
        return null;
    }

    /**
     * Returns the block at the given block index.
     */
    public <B extends Block> B getBlock(int blockIndex) {
        @SuppressWarnings("unchecked")
        B block = (B) blocks[blockIndex];
        return block;
    }

    public Page asPage() {
        return new Page(positionCount, blocks);
    }

    /**
     * Returns the number of blocks in this composite block.
     */
    public int getBlockCount() {
        return blocks.length;
    }

    @Override
    public boolean mvSortedAscending() {
        return Arrays.stream(blocks).allMatch(Block::mvSortedAscending);
    }

    @Override
    public boolean mvDeduplicated() {
        return Arrays.stream(blocks).allMatch(Block::mvDeduplicated);
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public int getTotalValueCount() {
        throw new UnsupportedOperationException("Composite block");
    }

    @Override
    public int getFirstValueIndex(int position) {
        throw new UnsupportedOperationException("Composite block");
    }

    @Override
    public int getValueCount(int position) {
        throw new UnsupportedOperationException("Composite block");
    }

    @Override
    public boolean isNull(int position) {
        for (Block block : blocks) {
            if (block.isNull(position) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ElementType elementType() {
        return ElementType.COMPOSITE;
    }

    @Override
    public BlockFactory blockFactory() {
        return blocks[0].blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        for (Block block : blocks) {
            block.allowPassingToDifferentDriver();
        }
    }

    @Override
    public boolean mayHaveNulls() {
        return Arrays.stream(blocks).anyMatch(Block::mayHaveNulls);
    }

    @Override
    public boolean areAllValuesNull() {
        return Arrays.stream(blocks).allMatch(Block::areAllValuesNull);
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return Arrays.stream(blocks).anyMatch(Block::mayHaveMultivaluedFields);
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        if (false == Arrays.stream(blocks).anyMatch(Block::mayHaveMultivaluedFields)) {
            return false;
        }
        return Arrays.stream(blocks).anyMatch(Block::doesHaveMultivaluedFields);
    }

    @Override
    public CompositeBlock filter(int... positions) {
        CompositeBlock result = null;
        final Block[] filteredBlocks = new Block[blocks.length];
        try {
            for (int i = 0; i < blocks.length; i++) {
                filteredBlocks[i] = blocks[i].filter(positions);
            }
            result = new CompositeBlock(filteredBlocks);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(filteredBlocks);
            }
        }
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        CompositeBlock result = null;
        final Block[] masked = new Block[blocks.length];
        try {
            for (int i = 0; i < blocks.length; i++) {
                masked[i] = blocks[i].keepMask(mask);
            }
            result = new CompositeBlock(masked);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(masked);
            }
        }
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        // TODO: support this
        throw new UnsupportedOperationException("can't lookup values from CompositeBlock");
    }

    @Override
    public MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    @Override
    public CompositeBlock expand() {
        throw new UnsupportedOperationException("CompositeBlock");
    }

    @Override
    public long ramBytesUsed() {
        return Arrays.stream(blocks).mapToLong(Accountable::ramBytesUsed).sum();
    }

    static Block readFrom(StreamInput in) throws IOException {
        final int numBlocks = in.readVInt();
        boolean success = false;
        final Block[] blocks = new Block[numBlocks];
        BlockStreamInput blockStreamInput = (BlockStreamInput) in;
        try {
            for (int b = 0; b < numBlocks; b++) {
                blocks[b] = Block.readTypedBlock(blockStreamInput);
            }
            CompositeBlock result = new CompositeBlock(blocks);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(blocks);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(blocks.length);
        for (Block block : blocks) {
            Block.writeTypedBlock(block, out);
        }
    }

    @Override
    protected void closeInternal() {
        Releasables.close(blocks);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeBlock that = (CompositeBlock) o;
        return positionCount == that.positionCount && Objects.deepEquals(blocks, that.blocks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(blocks), positionCount);
    }
}
