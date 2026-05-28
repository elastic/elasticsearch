/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;

/**
 * Raw-buffer uniformity checks shared by {@code *ArrowBufBlock.of()} factories to detect
 * constant-valued column batches before constructing a zero-copy block.
 * <p>
 * All checks short-circuit on the first differing value. Comparison is on raw buffer bits,
 * which is correct for primitive equality and avoids NaN-equality pitfalls (different NaN
 * bit patterns are correctly seen as different).
 */
public final class ArrowBufConstantDetection {

    private ArrowBufConstantDetection() {}

    /**
     * Returns {@code true} when every {@code byteSize}-wide element in {@code buf}'s first
     * {@code rowCount} positions has identical bytes to position 0. {@code byteSize} must be
     * 1, 2, 4, or 8 (the fixed widths used by Arrow primitive vectors).
     */
    public static boolean isUniform(ArrowBuf buf, int rowCount, int byteSize) {
        if (rowCount <= 1) {
            return true;
        }
        return switch (byteSize) {
            case 1 -> isUniform1(buf, rowCount);
            case 2 -> isUniform2(buf, rowCount);
            case 4 -> isUniform4(buf, rowCount);
            case 8 -> isUniform8(buf, rowCount);
            default -> throw new IllegalArgumentException("Unsupported byte size: " + byteSize);
        };
    }

    private static boolean isUniform1(ArrowBuf buf, int rowCount) {
        byte first = buf.getByte(0);
        for (int i = 1; i < rowCount; i++) {
            if (buf.getByte(i) != first) {
                return false;
            }
        }
        return true;
    }

    private static boolean isUniform2(ArrowBuf buf, int rowCount) {
        short first = buf.getShort(0);
        for (int i = 1; i < rowCount; i++) {
            if (buf.getShort((long) i * Short.BYTES) != first) {
                return false;
            }
        }
        return true;
    }

    private static boolean isUniform4(ArrowBuf buf, int rowCount) {
        int first = buf.getInt(0);
        for (int i = 1; i < rowCount; i++) {
            if (buf.getInt((long) i * Integer.BYTES) != first) {
                return false;
            }
        }
        return true;
    }

    private static boolean isUniform8(ArrowBuf buf, int rowCount) {
        long first = buf.getLong(0);
        for (int i = 1; i < rowCount; i++) {
            if (buf.getLong((long) i * Long.BYTES) != first) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns {@code true} when every bit in {@code buf}'s first {@code bitCount} positions
     * is identical. Compares whole bytes against a uniform mask; the partial last byte (when
     * {@code bitCount % 8 != 0}) is masked to ignore unused trailing bits.
     */
    public static boolean isUniformBits(ArrowBuf buf, int bitCount) {
        if (bitCount <= 1) {
            return true;
        }
        boolean first = (buf.getByte(0) & 0x01) != 0;
        byte fullByteMask = first ? (byte) 0xFF : (byte) 0x00;
        int fullBytes = bitCount / 8;
        int remainder = bitCount % 8;

        for (int b = 0; b < fullBytes; b++) {
            if (buf.getByte(b) != fullByteMask) {
                return false;
            }
        }
        if (remainder != 0) {
            int usedBitsMask = (1 << remainder) - 1;
            byte actual = (byte) (buf.getByte(fullBytes) & usedBitsMask);
            byte expected = (byte) (first ? usedBitsMask : 0);
            return actual == expected;
        }
        return true;
    }
}
