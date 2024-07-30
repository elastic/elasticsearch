/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Block that stores $type$ values.
 * This class is generated. Do not edit it.
 */
$if(BytesRef)$
public sealed interface BytesRefBlock extends Block permits BytesRefArrayBlock, BytesRefVectorBlock, ConstantNullBlock,
    OrdinalBytesRefBlock {
$else$
public sealed interface $Type$Block extends Block permits $Type$ArrayBlock, $Type$VectorBlock, ConstantNullBlock, $Type$BigArrayBlock {
$endif$

$if(BytesRef)$
    BytesRef NULL_VALUE = new BytesRef();

$endif$
    /**
     * Retrieves the $type$ value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
$if(BytesRef)$
     * @param dest the destination
$endif$
     * @return the data value (as a $type$)
     */
$if(BytesRef)$
    BytesRef getBytesRef(int valueIndex, BytesRef dest);

$else$
    $type$ get$Type$(int valueIndex);
$endif$

    @Override
    $Type$Vector asVector();

$if(BytesRef)$
    /**
     * Returns an ordinal bytesref block if this block is backed by a dictionary and ordinals; otherwise,
     * returns null. Callers must not release the returned block as no extra reference is retained by this method.
     */
    OrdinalBytesRefBlock asOrdinals();
$endif$

    @Override
    $Type$Block filter(int... positions);

    @Override
    ReleasableIterator<? extends $Type$Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    @Override
    $Type$Block expand();

    @Override
    default String getWriteableName() {
        return "$Type$Block";
    }

    NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Block.class, "$Type$Block", $Type$Block::readFrom);

    private static $Type$Block readFrom(StreamInput in) throws IOException {
        return readFrom((BlockStreamInput) in);
    }

    static $Type$Block readFrom(BlockStreamInput in) throws IOException {
        final byte serializationType = in.readByte();
        return switch (serializationType) {
            case SERIALIZE_BLOCK_VALUES -> $Type$Block.readValues(in);
            case SERIALIZE_BLOCK_VECTOR -> $Type$Vector.readFrom(in.blockFactory(), in).asBlock();
            case SERIALIZE_BLOCK_ARRAY -> $Type$ArrayBlock.readArrayBlock(in.blockFactory(), in);
$if(BytesRef)$
            case SERIALIZE_BLOCK_ORDINAL -> OrdinalBytesRefBlock.readOrdinalBlock(in.blockFactory(), in);
$else$
            case SERIALIZE_BLOCK_BIG_ARRAY -> $Type$BigArrayBlock.readArrayBlock(in.blockFactory(), in);
$endif$
            default -> {
                assert false : "invalid block serialization type " + serializationType;
                throw new IllegalStateException("invalid serialization type " + serializationType);
            }
        };
    }

    private static $Type$Block readValues(BlockStreamInput in) throws IOException {
        final int positions = in.readVInt();
        try ($Type$Block.Builder builder = in.blockFactory().new$Type$BlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                if (in.readBoolean()) {
                    builder.appendNull();
                } else {
                    final int valueCount = in.readVInt();
                    builder.beginPositionEntry();
                    for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                        builder.append$Type$(in.read$Type$());
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        $Type$Vector vector = asVector();
        final var version = out.getTransportVersion();
        if (vector != null) {
            out.writeByte(SERIALIZE_BLOCK_VECTOR);
            vector.writeTo(out);
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof $Type$ArrayBlock b) {
            out.writeByte(SERIALIZE_BLOCK_ARRAY);
            b.writeArrayBlock(out);
$if(BytesRef)$
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof OrdinalBytesRefBlock b && b.isDense()) {
            out.writeByte(SERIALIZE_BLOCK_ORDINAL);
            b.writeOrdinalBlock(out);
$else$
        } else if (version.onOrAfter(TransportVersions.V_8_14_0) && this instanceof $Type$BigArrayBlock b) {
            out.writeByte(SERIALIZE_BLOCK_BIG_ARRAY);
            b.writeArrayBlock(out);
$endif$
        } else {
            out.writeByte(SERIALIZE_BLOCK_VALUES);
            $Type$Block.writeValues(this, out);
        }
    }

    private static void writeValues($Type$Block block, StreamOutput out) throws IOException {
        final int positions = block.getPositionCount();
        out.writeVInt(positions);
        for (int pos = 0; pos < positions; pos++) {
            if (block.isNull(pos)) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                final int valueCount = block.getValueCount(pos);
                out.writeVInt(valueCount);
$if(BytesRef)$
                var scratch = new BytesRef();
$endif$
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
$if(BytesRef)$
                    out.write$Type$(block.get$Type$(block.getFirstValueIndex(pos) + valueIndex, scratch));
$else$
                    out.write$Type$(block.get$Type$(block.getFirstValueIndex(pos) + valueIndex));
$endif$
                }
            }
        }
    }

    /**
     * Compares the given object with this block for equality. Returns {@code true} if and only if the
     * given object is a $Type$Block, and both blocks are {@link #equals($Type$Block, $Type$Block) equal}.
     */
    @Override
    boolean equals(Object obj);

    /** Returns the hash code of this block, as defined by {@link #hash($Type$Block)}. */
    @Override
    int hashCode();

    /**
     * Returns {@code true} if the given blocks are equal to each other, otherwise {@code false}.
     * Two blocks are considered equal if they have the same position count, and contain the same
     * values (including absent null values) in the same order. This definition ensures that the
     * equals method works properly across different implementations of the $Type$Block interface.
     */
    static boolean equals($Type$Block block1, $Type$Block block2) {
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
$if(BytesRef)$
                    if (block1.getBytesRef(b1ValueIdx + valueIndex, new BytesRef())
                        .equals(block2.getBytesRef(b2ValueIdx + valueIndex, new BytesRef())) == false) {
$else$
                    if (block1.get$Type$(b1ValueIdx + valueIndex) != block2.get$Type$(b2ValueIdx + valueIndex)) {
$endif$
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
    static int hash($Type$Block block) {
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
$if(BytesRef)$
                    result = 31 * result + block.getBytesRef(firstValueIdx + valueIndex, new BytesRef()).hashCode();
$elseif(boolean)$
                    result = 31 * result + Boolean.hashCode(block.getBoolean(firstValueIdx + valueIndex));
$elseif(int)$
                    result = 31 * result + block.getInt(firstValueIdx + valueIndex);
$elseif(float)$
                    result = 31 * result + Float.floatToIntBits(block.getFloat(pos));
$elseif(long)$
                    long element = block.getLong(firstValueIdx + valueIndex);
                    result = 31 * result + (int) (element ^ (element >>> 32));
$elseif(double)$
                    long element = Double.doubleToLongBits(block.getDouble(firstValueIdx + valueIndex));
                    result = 31 * result + (int) (element ^ (element >>> 32));
$endif$
                }
            }
        }
        return result;
    }

    /**
     * Builder for {@link $Type$Block}
     */
    sealed interface Builder extends Block.Builder, BlockLoader.$Type$Builder permits $Type$BlockBuilder {
        /**
         * Appends a $type$ to the current entry.
         */
        @Override
        Builder append$Type$($type$ value);

        /**
         * Copy the values in {@code block} from {@code beginInclusive} to
         * {@code endExclusive} into this builder.
         */
        Builder copyFrom($Type$Block block, int beginInclusive, int endExclusive);

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
        $Type$Block build();
    }
}
