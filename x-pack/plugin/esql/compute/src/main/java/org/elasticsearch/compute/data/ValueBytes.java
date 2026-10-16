/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * The bytes a block's values spell, as opposed to the memory the block holds.
 *
 * <p>A value of a fixed width counts as its width, one of bytes as its length. Nothing is copied: a block that
 * already holds the sum of its values is asked for it, a column read against a dictionary measures each term once,
 * and only a block that can answer for no more than one value at a time is walked value by value.
 */
public final class ValueBytes {

    private ValueBytes() {}

    /** The bytes the values of {@code block} spell, or 0 if it holds none. */
    public static long of(Block block) {
        final int positions = block.getPositionCount();
        if (positions == 0 || block.areAllValuesNull()) {
            return 0;
        }
        if (block instanceof BytesRefBlock bytesRefs) {
            final long total = totalOf(bytesRefs);
            return total >= 0 ? total : walk(bytesRefs);
        }
        final int width = widthOf(block.elementType());
        return width == 0 ? 0 : (long) block.getTotalValueCount() * width;
    }

    /**
     * The bytes a block holds when its shape can answer for all of its values at once, or {@code -1} when only
     * walking them can.
     */
    private static long totalOf(BytesRefBlock block) {
        if (block instanceof BytesRefArrayBlock arrayBlock) {
            return arrayBlock.totalValueBytes();
        }
        final OrdinalBytesRefBlock ordinals = block.asOrdinals();
        if (ordinals != null && ordinals.getDictionaryVector().getPositionCount() <= block.getTotalValueCount()) {
            // A dictionary holding more terms than the block holds values is read for fewer of them than it holds,
            // so measuring all of it would cost more than reading the values it is asked for.
            return ordinalsOf(ordinals);
        }
        final BytesRefVector vector = block.asVector();
        if (vector instanceof BytesRefArrayVector arrayVector) {
            return arrayVector.totalValueBytes();
        }
        if (vector != null && vector.isConstant()) {
            // One value, named by every position: its length is what each of them weighs.
            return (long) vector.getPositionCount() * vector.getBytesRef(0, new BytesRef()).length;
        }
        return -1;
    }

    /** The bytes a block of ordinals holds, as the length of the term each position names, measuring each term once. */
    private static long ordinalsOf(OrdinalBytesRefBlock block) {
        final BytesRefVector dictionary = block.getDictionaryVector();
        final BytesRef scratch = new BytesRef();
        final int[] lengths = new int[dictionary.getPositionCount()];
        for (int term = 0; term < lengths.length; term++) {
            lengths[term] = dictionary.getBytesRef(term, scratch).length;
        }
        final IntBlock ordinals = block.getOrdinalsBlock();
        final int positions = ordinals.getPositionCount();
        long bytes = 0;
        for (int position = 0; position < positions; position++) {
            final int count = ordinals.getValueCount(position);
            final int first = ordinals.getFirstValueIndex(position);
            for (int value = 0; value < count; value++) {
                bytes += lengths[ordinals.getInt(first + value)];
            }
        }
        return bytes;
    }

    /** The bytes a block holds, read value by value. */
    private static long walk(BytesRefBlock block) {
        final int positions = block.getPositionCount();
        final BytesRef scratch = new BytesRef();
        long bytes = 0;
        for (int position = 0; position < positions; position++) {
            final int count = block.getValueCount(position);
            final int first = block.getFirstValueIndex(position);
            for (int value = 0; value < count; value++) {
                bytes += block.getBytesRef(first + value, scratch).length;
            }
        }
        return bytes;
    }

    /** The bytes one value of {@code type} spells, or 0 if it has no fixed width. */
    private static int widthOf(ElementType type) {
        return switch (type) {
            case BOOLEAN -> Byte.BYTES;
            case INT, FLOAT -> Integer.BYTES;
            case LONG, DOUBLE -> Long.BYTES;
            default -> 0;
        };
    }
}
