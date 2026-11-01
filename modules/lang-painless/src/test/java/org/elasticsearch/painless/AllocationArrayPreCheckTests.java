/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/** Tests for runtime-sized array allocation pre-checks ({@code new T[n]}, {@code new T[d0][d1]...}). */
public class AllocationArrayPreCheckTests extends AllocationTestCase {

    public void testOneDimConstantLengthCharged() {
        // new int[10] => pad8(16 + 4*10) = 56 bytes.
        assertEquals(AllocSizes.arraySize(int.class, 10), allocatedBytes("int[] a = new int[10]; return \"x\";"));
    }

    public void testOneDimVariableLengthCharged() {
        // new long[n] with n=4 => pad8(16 + 8*4) = 48 bytes.
        assertEquals(AllocSizes.arraySize(long.class, 4), allocatedBytes("int n = 4; long[] a = new long[n]; return \"x\";"));
    }

    public void testOneDimByteArray() {
        // new byte[10] => pad8(16 + 1*10) = 32 bytes; fieldSize(byte) = 1.
        assertEquals(AllocSizes.arraySize(byte.class, 10), allocatedBytes("byte[] b = new byte[10]; return \"x\";"));
    }

    public void testOneDimBooleanArray() {
        // new boolean[10] => pad8(16 + 1*10) = 32 bytes; fieldSize(boolean) = 1.
        assertEquals(AllocSizes.arraySize(boolean.class, 10), allocatedBytes("boolean[] b = new boolean[10]; return \"x\";"));
    }

    public void testOneDimShortArray() {
        // new short[10] => pad8(16 + 2*10) = 40 bytes; fieldSize(short) = 2.
        assertEquals(AllocSizes.arraySize(short.class, 10), allocatedBytes("short[] s = new short[10]; return \"x\";"));
    }

    public void testOneDimCharArray() {
        // new char[10] => pad8(16 + 2*10) = 40 bytes; fieldSize(char) = 2.
        assertEquals(AllocSizes.arraySize(char.class, 10), allocatedBytes("char[] c = new char[10]; return \"x\";"));
    }

    public void testOneDimZeroLength() {
        // new int[0] => pad8(16 + 4*0) = 16 bytes (just the array header).
        assertEquals(AllocSizes.arraySize(int.class, 0), allocatedBytes("int[] a = new int[0]; return \"x\";"));
    }

    public void testOneDimPreemptsHugeAllocation() {
        // The 2GB array would OOM if actually allocated; the pre-check trips first.
        assertTripsLimit("byte[] b = new byte[Integer.MAX_VALUE]; return \"x\";");
    }

    public void testTwoDimChargedExactly() {
        // new int[2][3] => pad8(16 + 4 * (2*3)) = pad8(40) = 40 bytes.
        assertEquals(AllocSizes.arraySize(int.class, 2 * 3), allocatedBytes("int[][] a = new int[2][3]; return \"x\";"));
    }

    public void testThreeDimChargedExactly() {
        assertEquals(AllocSizes.arraySize(int.class, 2 * 2 * 2), allocatedBytes("int[][][] a = new int[2][2][2]; return \"x\";"));
    }

    public void testMultiDimPreemptsHugeAllocation() {
        // new int[10][1_000_000_000]: product overflows int but not long; pre-check trips before MULTIANEWARRAY.
        assertTripsLimit("int[][] a = new int[10][1000000000]; return \"x\";");
    }

    public void testHighDimensionalArrayCharged() {
        // 100-dim new int[2][1]...[1]: product = 2; verifies the multiply loop handles many dims without overflow.
        int dims = 100;
        StringBuilder type = new StringBuilder("int");
        for (int i = 0; i < dims; i++) {
            type.append("[]");
        }
        StringBuilder src = new StringBuilder(type).append(" a = new int[2]");
        for (int i = 1; i < dims; i++) {
            src.append("[1]");
        }
        src.append("; return \"x\";");
        assertEquals(AllocSizes.arraySize(int.class, 2), allocatedBytes(src.toString()));
    }

    public void testHighDimensionalArrayPreemptsHugeAllocation() {
        // 100-dim array with a large outer dimension trips the limit before MULTIANEWARRAY.
        int dims = 100;
        StringBuilder type = new StringBuilder("int");
        for (int i = 0; i < dims; i++) {
            type.append("[]");
        }
        StringBuilder src = new StringBuilder(type).append(" a = new int[Integer.MAX_VALUE]");
        for (int i = 1; i < dims; i++) {
            src.append("[1]");
        }
        src.append("; return \"x\";");
        assertTripsLimit(src.toString());
    }

    public void testTwoDimVariableLengthsCharged() {
        assertEquals(
            AllocSizes.arraySize(int.class, 3 * 4),
            allocatedBytes("int x = 3; int y = 4; int[][] a = new int[x][y]; return \"x\";")
        );
    }

    public void testMultiDimExtentOverflowTripsLimit() {
        // 3000000^3 overflows long; without saturation the product wraps to a small/negative charge that slips past the
        // limit, letting MULTIANEWARRAY proceed and OOM the node. The saturating fold charges Long.MAX_VALUE and trips first.
        assertTripsLimit("int n = 3000000; int[][][] a = new int[n][n][n]; return \"x\";");
    }

    public void testMulSatExactBelowOverflow() {
        // No overflow: returns the true product.
        assertEquals(6L, AllocSizes.mulSat(2L, 3L));
        assertEquals(9_000_000_000_000L, AllocSizes.mulSat(3_000_000L, 3_000_000L));
    }

    public void testMulSatSaturatesHighOnOverflow() {
        // 3000000^3 overflows; the fold (mulSat applied left to right) saturates to Long.MAX_VALUE.
        assertEquals(Long.MAX_VALUE, AllocSizes.mulSat(AllocSizes.mulSat(3_000_000L, 3_000_000L), 3_000_000L));
        assertEquals(Long.MAX_VALUE, AllocSizes.mulSat(Long.MAX_VALUE, 2L));
    }

    public void testArrayBytesSaturatesOnOverflow() {
        // A huge element count saturates the whole pad8(ARRAY_HEADER + fieldSize * length) computation rather than wrapping;
        // the result is the largest 8-aligned long (pad8 keeps its multiple-of-8 invariant even when saturated).
        assertEquals(Long.MAX_VALUE & ~7L, AllocSizes.arrayBytes(Long.MAX_VALUE, 4));
        // Small inputs match the exact formula.
        assertEquals(56L, AllocSizes.arrayBytes(10L, 4)); // pad8(16 + 4*10)
    }
}
