/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

/**
 * Utility class providing vectorized comparison operations using the
 * Panama Vector API (Incubator).
 *
 * <p>This class offers methods to perform efficient byte-wise comparisons
 * on arrays using SIMD instructions. It can produce bitmasks indicating
 * which bytes in a loaded block match a given value, and can also report
 * the size of the vector block being processed.
 *
 * <p>Example usage:
 * <pre>{@code
 * byte[] data = ...;
 * byte target = 0x1F;
 *
 * final int len = vectorComparisonUtils.byteVectorLanes();
 * final int bound = loopBound(length, len);
 * int i = 0;
 * for (; i < bound; i += len) {
 *     long mask = vectorComparisonUtils.equalMask(data, i, target);
 *     // Process mask...
 * }
 * // tail
 * for (; i < data.length; i++) {
 *   // process mask
 * }
 * }</pre>
 */
public interface VectorComparisonUtils {

    /**
     * Compares a block of bytes in the given array to a specified value,
     * producing a bitmask of the positions that are equal.
     *
     * <p>This method loads a vector of bytes starting at the specified offset,
     * compares each element to {@code value}, and returns a {@code long} mask
     * where each bit represents the result of the comparison for one lane.
     * A bit is set to 1 if the corresponding byte is equal to {@code value},
     * and 0 otherwise.
     *
     * @param array  the source byte array containing the data to test
     * @param offset the starting index in the array (must be aligned for the vector species)
     * @param value  the byte value to compare each lane against
     * @return a bitmask representing which bytes in the vector are equal to {@code value}
     */
    long equalMask(byte[] array, int offset, byte value);

    /**
     * Returns the number of bytes processed by each {@link #equalMask(byte[],int,byte)}
     * invocation.
     *
     * <p>This corresponds to the number of lanes in the vector and represents
     * how many bytes are compared in each vectorized operation.
     *
     * @return the vector length
     */
    int byteVectorLanes();

    /**
     * Returns the number of longs processed by each {@link #equalMask(long[],int,long)}
     * invocation.
     *
     * <p>This corresponds to the number of lanes in the vector and represents
     * how many long values are compared in each vectorized operation.
     *
     * @return the vector length
     */
    int longVectorLanes();

    /**
     * Compares a block of longs in the given array to a specified value,
     * producing a bitmask of the positions that are equal.
     *
     * <p>This method loads a vector of longs starting at the specified offset,
     * compares each element to {@code value}, and returns a {@code long} mask
     * where each bit represents the result of the comparison for one lane.
     * A bit is set to 1 if the corresponding long is equal to {@code value},
     * and 0 otherwise.
     *
     * @param array  the source long array containing the data to test
     * @param offset the starting index in the array
     * @param value  the byte value to compare each lane against
     * @return a bitmask representing which longs in the vector are equal to {@code value}
     */
    long equalMask(long[] array, int offset, long value);

    // Convenience utility methods to help with the masks (as a long).

    /**
     * Returns whether any bits are set in the given mask.
     *
     * <p>This indicates whether any bytes in the compared vector matched
     * the comparison condition (e.g., {@link #equalMask(byte[], int, byte)}).
     *
     * @param mask the bitmask returned from a vector comparison
     * @return {@code true} if any bits are set, {@code false} otherwise
     */
    static boolean anyTrue(long mask) {
        return mask != 0L;
    }

    /**
     * Returns the index (lane position) of the first set bit in the given mask.
     *
     * <p>The least significant bit corresponds to lane index {@code 0}.
     * If no bits are set, this method returns {@code -1}.
     *
     * @param mask the bitmask returned from a vector comparison
     * @return the index of the first set bit, or {@code -1} if none are set
     */
    static int firstSet(long mask) {
        if (mask == 0L) {
            return -1;
        }
        return Long.numberOfTrailingZeros(mask);
    }

    /**
     * Returns the index (lane position) of the next set bit in the given mask,
     * starting from the bit position immediately after {@code fromIndex}.
     *
     * <p>The least significant bit corresponds to lane index {@code 0}.
     * If no further bits are set, this method returns {@code -1}.
     *
     * <p>Example usage, iterate over all bytes in the vector that matched the comparison:
     * <pre>{@code
     * long mask = VectorComparisonUtils.equalMask(data, offset, target);
     *
     * int pos = VectorComparisonUtils.firstSet(mask);
     * while (pos >= 0) {
     *     int absoluteIndex = offset + pos;
     *     System.out.println("Match found at byte index: " + absoluteIndex);
     *     pos = VectorComparisonUtils.nextSet(mask, pos);
     * }
     * }</pre>
     *
     * @param mask      the bitmask returned from a vector comparison
     * @param fromIndex the index after which to search for the next set bit
     * @return the index of the next set bit, or {@code -1} if none are set after {@code fromIndex}
     */
    static int nextSet(long mask, int fromIndex) {
        if (fromIndex >= Long.SIZE - 1) {
            return -1;
        }
        long remaining = mask >>> (fromIndex + 1);
        // Clear all bits up to and including 'fromIndex'
        return (remaining == 0L) ? -1 : fromIndex + 1 + Long.numberOfTrailingZeros(remaining);
    }

    /**
     * Loop control function which returns the largest multiple of laneCount that is less
     * than or equal to the given length value. In typical usage, length is the result of
     * an array.length, and laneCount is {@code xxxVectorLanes()}. This is analogous to
     * jdk.incubator.vector.ByteVector::loopBound.
     */
    static int loopBound(int length, int laneCount) {
        assert laneCount > 0 && (laneCount & (laneCount - 1)) == 0;
        return length & -laneCount;
    }
}
