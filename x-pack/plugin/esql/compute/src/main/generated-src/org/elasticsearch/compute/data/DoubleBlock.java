/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Block that stores double values.
 * This class is generated. Do not edit it.
 */
public sealed interface DoubleBlock extends Block permits DoubleArrayBlock, DoubleVectorBlock, ConstantNullBlock, DoubleBigArrayBlock {

    /**
     * Retrieves the double value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @return the data value (as a double)
     */
    double getDouble(int valueIndex);

    @Override
    DoubleVector asVector();

    @Override
    DoubleBlock filter(int... positions);

    @Override
    ReleasableIterator<? extends DoubleBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    @Override
    DoubleBlock expand();

    @Override
    default String getWriteableName() {
        return "DoubleBlock";
    }

    NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Block.class, "DoubleBlock", DoubleBlock::readFrom);

    private static DoubleBlock readFrom(StreamInput in) throws IOException {
        return readFrom((BlockStreamInput) in);
    }

    static DoubleBlock readFrom(BlockStreamInput in) throws IOException {
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_BLOCK_VALUES -> DoubleBlock.readValues(in);
            case SERIALIZE_BLOCK_VECTOR -> DoubleVector.readFrom(in.blockFactory(), in).asBlock();
            case SERIALIZE_BLOCK_ARRAY -> DoubleArrayBlock.readArrayBlock(in.blockFactory(), in);
            case SERIALIZE_BLOCK_BIG_ARRAY -> DoubleBigArrayBlock.readArrayBlock(in.blockFactory(), in);
            default -> {
                assert false : "invalid block serialization type " + serializationType;
                throw new IllegalStateException("invalid serialization type " + serializationType);
            }
        };
    }

    private static DoubleBlock readValues(BlockStreamInput in) throws IOException {
        final int positions = in.readVInt();
        try (DoubleBlock.Builder builder = in.blockFactory().newDoubleBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                if (in.readBoolean()) {
                    builder.appendNull();
                } else {
                    final int valueCount = in.readVInt();
                    builder.beginPositionEntry();
                    for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                        builder.appendDouble(in.readDouble());
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        DoubleVector vector = asVector();
        final var version = out.getTransportVersion();
        if (vector != null) {
            out.writeByte(SERIALIZE_BLOCK_VECTOR);
            vector.writeTo(out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof DoubleArrayBlock b) {
            out.writeByte(SERIALIZE_BLOCK_ARRAY);
            b.writeArrayBlock(out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof DoubleBigArrayBlock b) {
            out.writeByte(SERIALIZE_BLOCK_BIG_ARRAY);
            b.writeArrayBlock(out);
        } else {
            out.writeByte(SERIALIZE_BLOCK_VALUES);
            DoubleBlock.writeValues(this, out);
        }
    }

    private static void writeValues(DoubleBlock block, StreamOutput out) throws IOException {
        final int positions = block.getPositionCount();
        out.writeVInt(positions);
        for (int pos = 0; pos < positions; pos++) {
            if (block.isNull(pos)) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                final int valueCount = block.getValueCount(pos);
                out.writeVInt(valueCount);
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    out.writeDouble(block.getDouble(block.getFirstValueIndex(pos) + valueIndex));
                }
            }
        }
    }

    /**
     * Compares the given object with this block for equality. Returns {@code true} if and only if the
     * given object is a DoubleBlock, and both blocks are {@link #equals(DoubleBlock, DoubleBlock) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this block, as defined by {@link #hash(DoubleBlock)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given blocks are equal to each other, otherwise {@code false}.
     * Two blocks are considered equal if they have the same position count, and contain the same
     * values (including absent null values) in the same order. This definition ensures that the
     * equals method works properly across different implementations of the DoubleBlock interface.
     */
    static boolean equals(DoubleBlock block1, DoubleBlock block2) {
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
                    if (block1.getDouble(b1ValueIdx + valueIndex) != block2.getDouble(b2ValueIdx + valueIndex)) {
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
    static int hash(DoubleBlock block) {
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
                    long element = Double.doubleToLongBits(block.getDouble(firstValueIdx + valueIndex));
                    result = 31 * result + (int) (element ^ (element >>> 32));
                }
            }
        }
        return result;
    }

    /**
     * Builder for {@link DoubleBlock}
     */
    sealed interface Builder extends Block.Builder, BlockLoader.DoubleBuilder permits DoubleBlockBuilder {
        /**
         * Appends a double to the current entry.
         */
        @Override
        Builder appendDouble(double value);

        /**
         * Copy the values in {@code block} from {@code beginInclusive} to
         * {@code endExclusive} into this builder.
         */
        Builder copyFrom(DoubleBlock block, int beginInclusive, int endExclusive);

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

        @Override
        DoubleBlock build();
    }
}
