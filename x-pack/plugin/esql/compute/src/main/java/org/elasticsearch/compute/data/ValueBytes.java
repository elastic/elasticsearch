/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * The bytes a block's values spell, as opposed to the memory the block holds.
 *
 * <p>A value of a fixed width counts as its width, one of bytes as its length. Nothing is read value by value: a
 * block of bytes answers with {@link BytesRefBlock#estimateTotalValueBytes()}.
 */
public final class ValueBytes {

    private ValueBytes() {}

    /** The bytes the values of {@code block} spell, or 0 if it holds none. */
    public static long of(Block block) {
        if (block.getPositionCount() == 0 || block.areAllValuesNull()) {
            return 0;
        }
        if (block instanceof BytesRefBlock bytesRefs) {
            return bytesRefs.estimateTotalValueBytes();
        }
        final int width = widthOf(block.elementType());
        return width == 0 ? 0 : (long) block.getTotalValueCount() * width;
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
