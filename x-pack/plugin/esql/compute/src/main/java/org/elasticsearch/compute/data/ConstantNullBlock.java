/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
import java.util.Objects;

/**
 * Block implementation representing a constant null value.
 */
public final class ConstantNullBlock extends AbstractNonThreadSafeRefCounted
    implements
        BooleanBlock,
        IntBlock,
        LongBlock,
        FloatBlock,
        DoubleBlock,
        BytesRefBlock {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantNullBlock.class);
    private final int positionCount;
    private BlockFactory blockFactory;

    ConstantNullBlock(int positionCount, BlockFactory blockFactory) {
        this.positionCount = positionCount;
        this.blockFactory = blockFactory;
    }

    @Override
    public ConstantNullVector asVector() {
        return null;
    }

    @Override
    public OrdinalBytesRefBlock asOrdinals() {
        return null;
    }

    @Override
    public ToMask toMask() {
        return new ToMask(blockFactory.newConstantBooleanVector(false, positionCount), false);
    }

    @Override
    public boolean isNull(int position) {
        return true;
    }

    @Override
    public boolean areAllValuesNull() {
        return true;
    }

    @Override
    public boolean mayHaveNulls() {
        return true;
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return false;
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return false;
    }

    @Override
    public ElementType elementType() {
        return ElementType.NULL;
    }

    @Override
    public ConstantNullBlock filter(int... positions) {
        return (ConstantNullBlock) blockFactory().newConstantNullBlock(positions.length);
    }

    @Override
    public ConstantNullBlock keepMask(BooleanVector mask) {
        return (ConstantNullBlock) blockFactory().newConstantNullBlock(getPositionCount());
    }

    @Override
    public ReleasableIterator<ConstantNullBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return ReleasableIterator.single((ConstantNullBlock) positions.blockFactory().newConstantNullBlock(positions.getPositionCount()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getPositionCount());
    }

    @Override
    public MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    @Override
    public ConstantNullBlock expand() {
        incRef();
        return this;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConstantNullBlock that) {
            return this.getPositionCount() == that.getPositionCount();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPositionCount());
    }

    @Override
    public String toString() {
        return "ConstantNullBlock[positions=" + getPositionCount() + "]";
    }

    @Override
    public void closeInternal() {
        blockFactory().adjustBreaker(-ramBytesUsed());
    }

    static class Builder implements Block.Builder {

        final BlockFactory blockFactory;

        Builder(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
        }

        private int positionCount;

        /**
         * Has this builder been closed already?
         */
        private boolean closed = false;

        @Override
        public Builder appendNull() {
            positionCount++;
            return this;
        }

        @Override
        public Builder beginPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Builder endPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
            for (int i = beginInclusive; i < endExclusive; i++) {
                if (false == block.isNull(i)) {
                    throw new UnsupportedOperationException("can't append non-null values to a null block");
                }
            }
            positionCount += endExclusive - beginInclusive;
            return this;
        }

        @Override
        public Block.Builder mvOrdering(MvOrdering mvOrdering) {
            /*
             * This is called when copying but otherwise doesn't do
             * anything because there aren't multivalue fields in a
             * block containing only nulls.
             */
            return this;
        }

        @Override
        public long estimatedBytes() {
            return BASE_RAM_BYTES_USED;
        }

        @Override
        public Block build() {
            if (closed) {
                throw new IllegalStateException("already closed");
            }
            close();
            return blockFactory.newConstantNullBlock(positionCount);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Override
    public boolean getBoolean(int valueIndex) {
        assert false : "null block";
        throw new UnsupportedOperationException("null block");
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        assert false : "null block";
        throw new UnsupportedOperationException("null block");
    }

    @Override
    public float getFloat(int valueIndex) {
        assert false : "null block";
        throw new UnsupportedOperationException("null block");
    }

    @Override
    public double getDouble(int valueIndex) {
        assert false : "null block";
        throw new UnsupportedOperationException("null block");
    }

    @Override
    public int getInt(int valueIndex) {
        assert false : "null block";
        throw new UnsupportedOperationException("null block");
    }

    @Override
    public long getLong(int valueIndex) {
        assert false : "null block";
        throw new UnsupportedOperationException("null block");
    }

    @Override
    public int getTotalValueCount() {
        return 0;
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public int getFirstValueIndex(int position) {
        return 0;
    }

    @Override
    public int getValueCount(int position) {
        return 0;
    }

    @Override
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    @Override
    public void allowPassingToDifferentDriver() {
        blockFactory = blockFactory.parent();
    }
}
