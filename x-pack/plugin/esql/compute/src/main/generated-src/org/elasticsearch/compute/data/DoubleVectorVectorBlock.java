/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public final class DoubleVectorVectorBlock extends AbstractNonThreadSafeRefCounted implements Block {
    private final DoubleVectorBlock[] blocks;
    private final int positionCount;

    public DoubleVectorVectorBlock(DoubleVectorBlock[] blocks) {
        if (blocks == null || blocks.length == 0) {
            throw new IllegalArgumentException("must have at least one block; got " + Arrays.toString(blocks));
        }
        this.blocks = blocks;
        this.positionCount = blocks[0] != null ? blocks[0].getPositionCount() : 0;
        for (DoubleVectorBlock b : blocks) {
            if (b == null) {
                continue;
            }
            assert b.getPositionCount() == positionCount : "expected positionCount=" + positionCount + " but was " + b;
            if (b.getPositionCount() != positionCount) {
                assert false : "expected positionCount=" + positionCount + " but was " + b;
                throw new IllegalArgumentException("expected positionCount=" + positionCount + " but was " + b);
            }
            if (b.isReleased()) {
                assert false : "can't build DoubleVectorVector block out of released blocks but [" + b + "] was released";
                throw new IllegalArgumentException(
                    "can't build DoubleVectorVector block out of released blocks but [" + b + "] was released"
                );
            }
        }
    }

    static NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "DoubleVectorVectorBlock",
        DoubleVectorVectorBlock::readFrom
    );

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

    /**
     * Returns the number of blocks in this DoubleVectorVectorBlock block.
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
        throw new UnsupportedOperationException("DoubleVectorVector block");
    }

    @Override
    public int getFirstValueIndex(int position) {
        return this.blocks.length > position && this.blocks[position] != null ? 0 : -1;
    }

    @Override
    public int getValueCount(int position) {
        return this.blocks.length;
    }

    @Override
    public boolean isNull(int position) {
        return this.blocks.length > position && this.blocks[position] != null;
    }

    @Override
    public ElementType elementType() {
        return ElementType.DENSE_VECTOR;
    }

    @Override
    public BlockFactory blockFactory() {
        return blocks.length > 0 ? blocks[0].blockFactory() : null;
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
    public DoubleVectorVectorBlock filter(int... positions) {
        DoubleVectorVectorBlock result = null;
        final DoubleVectorBlock[] filteredBlocks = new DoubleVectorBlock[positions.length];
        try {
            int j = 0;
            for (int i = 0; i < blocks.length; i++) {
                for (int p : positions) {
                    if (p == i) {
                        filteredBlocks[j] = blocks[p];
                        j++;
                    }
                }
            }
            result = new DoubleVectorVectorBlock(filteredBlocks);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(filteredBlocks);
            }
        }
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        // TODO: support this
        throw new UnsupportedOperationException("can't lookup values from DoubleVectorVectorBlock");
    }

    @Override
    public MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    @Override
    public DoubleVectorVectorBlock expand() {
        /*DoubleVectorBlock[] newBlocks = new DoubleVectorBlock[this.blocks.length * 2];
        System.arraycopy(this.blocks, 0, newBlocks, 0, blocks.length);
        this.blocks = newBlocks;*/
        throw new UnsupportedOperationException("could not expand for DoubleVectorVectorBlock");
    }

    @Override
    public long ramBytesUsed() {
        return Arrays.stream(blocks).mapToLong(Accountable::ramBytesUsed).sum();
    }

    @Override
    public String getWriteableName() {
        return "DoubleVectorVectorBlock";
    }

    static Block readFrom(StreamInput in) throws IOException {
        final int numBlocks = in.readVInt();
        boolean success = false;
        final DoubleVectorBlock[] blocks = new DoubleVectorBlock[numBlocks];
        try {
            for (int b = 0; b < numBlocks; b++) {
                blocks[b] = in.readNamedWriteable(DoubleVectorBlock.class);
            }
            DoubleVectorVectorBlock result = new DoubleVectorVectorBlock(blocks);
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
            out.writeNamedWriteable(block);
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
        DoubleVectorVectorBlock that = (DoubleVectorVectorBlock) o;
        return positionCount == that.positionCount && Objects.deepEquals(blocks, that.blocks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(blocks), positionCount);
    }

    public static DoubleVectorVectorBlock.Builder newBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
        return new DoubleVectorVectorBlock.Builder(blockFactory, estimatedSize);
    }

    public DoubleVectorBlock getDoubleVector(int offset) {
        return blocks[offset];
    }

    public static class Builder implements Block.Builder {

        private final BlockFactory blockFactory;
        private final DoubleVectorBlock[] doubleVectorBlocks;
        private final AtomicInteger index;

        public Builder(BlockFactory blockFactory, int estimatedSize) {
            this.blockFactory = blockFactory;
            this.doubleVectorBlocks = new DoubleVectorBlock[estimatedSize];
            this.index = new AtomicInteger();
        }

        @Override
        public void close() {
            for (DoubleVectorBlock block : doubleVectorBlocks) {
                if (block != null) {
                    block.close();
                }
            }
        }

        @Override
        public Block.Builder appendNull() {
            return this;
        }

        @Override
        public Block.Builder beginPositionEntry() {
            return this;
        }

        @Override
        public Block.Builder endPositionEntry() {
            return this;
        }

        @Override
        public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
            int positionCount = endExclusive - beginInclusive;
            for (int i = 0; i < positionCount; i++) {
                int valueCount = block.getValueCount(beginInclusive + i);
                if (valueCount > 0) {
                    if (block instanceof DoubleVectorVectorBlock doubleVectorBlock) {
                        DoubleVector vector = doubleVectorBlock.getDoubleVector(i).asVector();
                        doubleVectorBlocks[index.getAndIncrement()] = new DoubleVectorBlock(vector);
                    }
                } else {
                    double[] values = new double[valueCount];
                    DoubleVector vector = new DoubleArrayVector(values, positionCount, blockFactory);
                    doubleVectorBlocks[index.getAndIncrement()] = new DoubleVectorBlock(vector);
                }
            }
            return this;
        }

        @Override
        public Block.Builder mvOrdering(MvOrdering mvOrdering) {
            return this;
        }

        @Override
        public long estimatedBytes() {
            return (long) Double.BYTES * doubleVectorBlocks.length;
        }

        @Override
        public Block build() {
            return new DoubleVectorVectorBlock(doubleVectorBlocks);
        }
    }
}
