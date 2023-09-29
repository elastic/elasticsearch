/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Block implementation representing a constant null value.
 */
public final class ConstantNullBlock extends AbstractBlock {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantNullBlock.class);

    // Eventually, this should use the GLOBAL breaking instance
    ConstantNullBlock(int positionCount) {
        this(positionCount, BlockFactory.getNonBreakingInstance());
    }

    ConstantNullBlock(int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public boolean isNull(int position) {
        return true;
    }

    @Override
    public int nullValuesCount() {
        return getPositionCount();
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
    public ElementType elementType() {
        return ElementType.NULL;
    }

    @Override
    public Block filter(int... positions) {
        return new ConstantNullBlock(positions.length);
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "ConstantNullBlock",
        ConstantNullBlock::of
    );

    @Override
    public String getWriteableName() {
        return "ConstantNullBlock";
    }

    static ConstantNullBlock of(StreamInput in) throws IOException {
        return new ConstantNullBlock(in.readVInt());
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
    public Block expand() {
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
    public void close() {
        if (isReleased()) {
            throw new IllegalStateException("can't release already released block [" + this + "]");
        }
        released = true;
        blockFactory.adjustBreaker(-ramBytesUsed(), true);
    }

    static class Builder implements Block.Builder {
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
            return this;
        }

        @Override
        public Block.Builder appendAllValuesToCurrentPosition(Block block) {
            return appendNull();
        }

        @Override
        public Block.Builder mvOrdering(MvOrdering mvOrdering) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Block build() {
            if (closed) {
                throw new IllegalStateException("already closed");
            }
            close();
            return new ConstantNullBlock(positionCount);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
