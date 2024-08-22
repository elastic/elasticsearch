/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Block that stores BytesRef values.
 * This class is generated. Do not edit it.
 */
public sealed interface BytesRefBlock extends Block permits BytesRefArrayBlock, BytesRefVectorBlock, ConstantNullBlock,
    OrdinalBytesRefBlock {
    BytesRef NULL_VALUE = new BytesRef();

    /**
     * Retrieves the BytesRef value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @param dest the destination
     * @return the data value (as a BytesRef)
     */
    BytesRef getBytesRef(int valueIndex, BytesRef dest);

    @Override
    BytesRefVector asVector();

    /**
     * Returns an ordinal bytesref block if this block is backed by a dictionary and ordinals; otherwise,
     * returns null. Callers must not release the returned block as no extra reference is retained by this method.
     */
    OrdinalBytesRefBlock asOrdinals();

    @Override
    BytesRefBlock filter(int... positions);

    @Override
    ReleasableIterator<? extends BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    @Override
    BytesRefBlock expand();

    @Override
    default String getWriteableName() {
        return "BytesRefBlock";
    }

    NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Block.class, "BytesRefBlock", BytesRefBlock::readFrom);

    private static BytesRefBlock readFrom(StreamInput in) throws IOException {
        return readFrom((BlockStreamInput) in);
    }

    static BytesRefBlock readFrom(BlockStreamInput in) throws IOException {
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_BLOCK_VALUES -> BytesRefBlock.readValues(in);
            case SERIALIZE_BLOCK_VECTOR -> BytesRefVector.readFrom(in.blockFactory(), in).asBlock();
            case SERIALIZE_BLOCK_ARRAY -> BytesRefArrayBlock.readArrayBlock(in.blockFactory(), in);
            case SERIALIZE_BLOCK_ORDINAL -> OrdinalBytesRefBlock.readOrdinalBlock(in.blockFactory(), in);
            default -> {
                assert false : "invalid block serialization type " + serializationType;
                throw new IllegalStateException("invalid serialization type " + serializationType);
            }
        };
    }

    private static BytesRefBlock readValues(BlockStreamInput in) throws IOException {
        final int positions = in.readVInt();
        try (BytesRefBlock.Builder builder = in.blockFactory().newBytesRefBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                if (in.readBoolean()) {
                    builder.appendNull();
                } else {
                    final int valueCount = in.readVInt();
                    builder.beginPositionEntry();
                    for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                        builder.appendBytesRef(in.readBytesRef());
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        BytesRefVector vector = asVector();
        final var version = out.getTransportVersion();
        if (vector != null) {
            out.writeByte(SERIALIZE_BLOCK_VECTOR);
            vector.writeTo(out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof BytesRefArrayBlock b) {
            out.writeByte(SERIALIZE_BLOCK_ARRAY);
            b.writeArrayBlock(out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof OrdinalBytesRefBlock b && b.isDense()) {
            out.writeByte(SERIALIZE_BLOCK_ORDINAL);
            b.writeOrdinalBlock(out);
        } else {
            out.writeByte(SERIALIZE_BLOCK_VALUES);
            BytesRefBlock.writeValues(this, out);
        }
    }

    private static void writeValues(BytesRefBlock block, StreamOutput out) throws IOException {
        final int positions = block.getPositionCount();
        out.writeVInt(positions);
        for (int pos = 0; pos < positions; pos++) {
            if (block.isNull(pos)) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                final int valueCount = block.getValueCount(pos);
                out.writeVInt(valueCount);
                var scratch = new BytesRef();
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    out.writeBytesRef(block.getBytesRef(block.getFirstValueIndex(pos) + valueIndex, scratch));
                }
            }
        }
    }

    /**
     * Compares the given object with this block for equality. Returns {@code true} if and only if the
     * given object is a BytesRefBlock, and both blocks are {@link #equals(BytesRefBlock, BytesRefBlock) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this block, as defined by {@link #hash(BytesRefBlock)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given blocks are equal to each other, otherwise {@code false}.
     * Two blocks are considered equal if they have the same position count, and contain the same
     * values (including absent null values) in the same order. This definition ensures that the
     * equals method works properly across different implementations of the BytesRefBlock interface.
     */
    static boolean equals(BytesRefBlock block1, BytesRefBlock block2) {
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
                    if (block1.getBytesRef(b1ValueIdx + valueIndex, new BytesRef())
                        .equals(block2.getBytesRef(b2ValueIdx + valueIndex, new BytesRef())) == false) {
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
    static int hash(BytesRefBlock block) {
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
                    result = 31 * result + block.getBytesRef(firstValueIdx + valueIndex, new BytesRef()).hashCode();
                }
            }
        }
        return result;
    }

    /**
     * Builder for {@link BytesRefBlock}
     */
    sealed interface Builder extends Block.Builder, BlockLoader.BytesRefBuilder permits BytesRefBlockBuilder {
        /**
         * Appends a BytesRef to the current entry.
         */
        @Override
        Builder appendBytesRef(BytesRef value);

        /**
         * Copy the values in {@code block} from {@code beginInclusive} to
         * {@code endExclusive} into this builder.
         */
        Builder copyFrom(BytesRefBlock block, int beginInclusive, int endExclusive);

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
        BytesRefBlock build();
    }
}
