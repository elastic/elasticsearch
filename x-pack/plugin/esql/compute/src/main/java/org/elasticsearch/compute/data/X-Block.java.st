/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$else$
$endif$
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Block that stores $type$ values.
 * This class is generated. Do not edit it.
 */
public sealed interface $Type$Block extends Block permits Filter$Type$Block, $Type$ArrayBlock, $Type$VectorBlock {

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

    @Override
    $Type$Block filter(int... positions);

    NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Block.class, "$Type$Block", $Type$Block::of);

    @Override
    default String getWriteableName() {
        return "$Type$Block";
    }

    static $Type$Block of(StreamInput in) throws IOException {
        final int positions = in.readVInt();
        var builder = newBlockBuilder(positions);
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

    @Override
    default void writeTo(StreamOutput out) throws IOException {
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
$if(BytesRef)$
                    out.write$Type$(get$Type$(getFirstValueIndex(pos) + valueIndex, new BytesRef()));
$else$
                    out.write$Type$(get$Type$(getFirstValueIndex(pos) + valueIndex));
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

    static Builder newBlockBuilder(int estimatedSize) {
        return new $Type$BlockBuilder(estimatedSize);
    }

    static $Type$Block newConstantBlockWith($type$ value, int positions) {
        return new Constant$Type$Vector(value, positions).asBlock();
    }

    sealed interface Builder extends Block.Builder permits $Type$BlockBuilder {

        /**
         * Appends a $type$ to the current entry.
         */
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

        // TODO boolean containsMvDups();

        /**
         * Appends the all values of the given block into a the current position
         * in this builder.
         */
        Builder appendAllValuesToCurrentPosition(Block block);

        /**
         * Appends the all values of the given block into a the current position
         * in this builder.
         */
        Builder appendAllValuesToCurrentPosition($Type$Block block);

        @Override
        $Type$Block build();
    }
}
