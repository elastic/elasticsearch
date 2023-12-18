/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Block that stores Point values.
 * This class is generated. Do not edit it.
 */
public sealed interface PointBlock extends Block permits PointArrayBlock, PointVectorBlock, ConstantNullBlock {

    /**
     * Retrieves the double x, double y stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @return the data value (as a double)
     */
    double getX(int valueIndex);

    double getY(int valueIndex);

    default SpatialPoint getPoint(int valueIndex) {
        return new SpatialPoint(getX(valueIndex), getY(valueIndex));
    }

    @Override
    PointVector asVector();

    @Override
    PointBlock filter(int... positions);

    @Override
    default String getWriteableName() {
        return "PointBlock";
    }

    NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Block.class, "PointBlock", PointBlock::readFrom);

    private static PointBlock readFrom(StreamInput in) throws IOException {
        return readFrom((BlockStreamInput) in);
    }

    private static PointBlock readFrom(BlockStreamInput in) throws IOException {
        final boolean isVector = in.readBoolean();
        if (isVector) {
            return PointVector.readFrom(in.blockFactory(), in).asBlock();
        }
        final int positions = in.readVInt();
        try (PointBlock.Builder builder = in.blockFactory().newPointBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                if (in.readBoolean()) {
                    builder.appendNull();
                } else {
                    final int valueCount = in.readVInt();
                    builder.beginPositionEntry();
                    for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                        builder.appendPoint(in.readDouble(), in.readDouble());
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        PointVector vector = asVector();
        out.writeBoolean(vector != null);
        if (vector != null) {
            vector.writeTo(out);
        } else {
            final int positions = getPositionCount();
            out.writeVInt(positions);
            for (int pos = 0; pos < positions; pos++) {
                if (isNull(pos)) {
                    out.writeBoolean(true);
                } else {
                    out.writeBoolean(false);
                    final int valueCount = getValueCount(pos);
                    out.writeVInt(valueCount);
                    for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                        out.writeDouble(getX(getFirstValueIndex(pos) + valueIndex));
                        out.writeDouble(getY(getFirstValueIndex(pos) + valueIndex));
                    }
                }
            }
        }
    }

    /**
     * Compares the given object with this block for equality. Returns {@code true} if and only if the
     * given object is a PointBlock, and both blocks are {@link #equals(PointBlock, PointBlock) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this block, as defined by {@link #hash(PointBlock)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given blocks are equal to each other, otherwise {@code false}.
     * Two blocks are considered equal if they have the same position count, and contain the same
     * values (including absent null values) in the same order. This definition ensures that the
     * equals method works properly across different implementations of the PointBlock interface.
     */
    static boolean equals(PointBlock block1, PointBlock block2) {
        if (block1 == block2) {
            return true;
        }
        final int positions = block1.getPositionCount();
        if (positions != block2.getPositionCount()) {
            return false;
        }
        for (int pos = 0; pos < positions; pos++) {
            if (block1.isNull(pos) || block2.isNull(pos)) {
                if (block1.isNull(pos) != block2.isNull(pos)) {
                    return false;
                }
            } else {
                final int valueCount = block1.getValueCount(pos);
                if (valueCount != block2.getValueCount(pos)) {
                    return false;
                }
                final int b1ValueIdx = block1.getFirstValueIndex(pos);
                final int b2ValueIdx = block2.getFirstValueIndex(pos);
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    if (block1.getX(b1ValueIdx + valueIndex) != block2.getX(b2ValueIdx + valueIndex)
                        || block1.getY(b1ValueIdx + valueIndex) != block2.getY(b2ValueIdx + valueIndex)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Generates the hash code for the given block. The hash code is computed from the block's values.
     * This ensures that {@code block1.equals(block2)} implies that {@code block1.hashCode()==block2.hashCode()}
     * for any two blocks, {@code block1} and {@code block2}, as required by the general contract of
     * {@link Object#hashCode}.
     */
    static int hash(PointBlock block) {
        final int positions = block.getPositionCount();
        int result = 1;
        for (int pos = 0; pos < positions; pos++) {
            if (block.isNull(pos)) {
                result = 31 * result - 1;
            } else {
                final int valueCount = block.getValueCount(pos);
                result = 31 * result + valueCount;
                final int firstValueIdx = block.getFirstValueIndex(pos);
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    long element = Double.doubleToLongBits(block.getX(firstValueIdx + valueIndex));
                    result = 31 * result + (int) (element ^ (element >>> 32));
                    element = Double.doubleToLongBits(block.getY(firstValueIdx + valueIndex));
                    result = 31 * result + (int) (element ^ (element >>> 32));
                }
            }
        }
        return result;
    }

    /**
     * Returns a builder using the {@link BlockFactory#getNonBreakingInstance non-breaking block factory}.
     * @deprecated use {@link BlockFactory#newPointBlockBuilder}
     */
    // Eventually, we want to remove this entirely, always passing an explicit BlockFactory
    @Deprecated
    static Builder newBlockBuilder(int estimatedSize) {
        return newBlockBuilder(estimatedSize, BlockFactory.getNonBreakingInstance());
    }

    /**
     * Returns a builder.
     * @deprecated use {@link BlockFactory#newPointBlockBuilder}
     */
    @Deprecated
    static Builder newBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        return blockFactory.newPointBlockBuilder(estimatedSize);
    }

    /**
     * Returns a constant block built by the {@link BlockFactory#getNonBreakingInstance non-breaking block factory}.
     * @deprecated use {@link BlockFactory#newConstantPointBlockWith}
     */
    // Eventually, we want to remove this entirely, always passing an explicit BlockFactory
    @Deprecated
    static PointBlock newConstantBlockWith(double x, double y, int positions) {
        return newConstantBlockWith(x, y, positions, BlockFactory.getNonBreakingInstance());
    }

    /**
     * Returns a constant block.
     * @deprecated use {@link BlockFactory#newConstantPointBlockWith}
     */
    @Deprecated
    static PointBlock newConstantBlockWith(double x, double y, int positions, BlockFactory blockFactory) {
        return blockFactory.newConstantPointBlockWith(x, y, positions);
    }

    /**
     * Builder for {@link PointBlock}
     */
    sealed interface Builder extends Block.Builder, BlockLoader.PointBuilder permits PointBlockBuilder {
        /**
         * Appends a double to the current entry.
         */
        @Override
        Builder appendPoint(double x, double y);

        /**
         * Copy the values in {@code block} from {@code beginInclusive} to
         * {@code endExclusive} into this builder.
         */
        Builder copyFrom(PointBlock block, int beginInclusive, int endExclusive);

        @Override
        Builder appendNull();

        @Override
        Builder beginPositionEntry();

        @Override
        Builder endPositionEntry();

        @Override
        Builder copyFrom(Block block, int beginInclusive, int endExclusive);

        @Override
        Builder mvOrdering(Block.MvOrdering mvOrdering);

        /**
         * Appends the all values of the given block into the current position
         * in this builder.
         */
        @Override
        Builder appendAllValuesToCurrentPosition(Block block);

        /**
         * Appends the all values of the given block into the current position
         * in this builder.
         */
        Builder appendAllValuesToCurrentPosition(PointBlock block);

        @Override
        PointBlock build();
    }
}
