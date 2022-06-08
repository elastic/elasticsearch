/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst;

import java.io.IOException;

/**
 * Static helper methods for {@link FST.Arc.BitTable}.
 *
 */
class BitTableUtil {

    /**
     * Returns whether the bit at given zero-based index is set. <br>
     * Example: bitIndex 10 means the third bit on the right of the second byte.
     *
     * @param bitIndex The bit zero-based index. It must be greater than or equal to 0, and strictly
     *     less than {@code number of bit-table bytes * Byte.SIZE}.
     * @param reader The {@link FST.BytesReader} to read. It must be positioned at the beginning of
     *     the bit-table.
     */
    static boolean isBitSet(int bitIndex, FST.BytesReader reader) throws IOException {
        assert bitIndex >= 0 : "bitIndex=" + bitIndex;
        reader.skipBytes(bitIndex >> 3);
        return (readByte(reader) & (1L << (bitIndex & (Byte.SIZE - 1)))) != 0;
    }

    /**
     * Counts all bits set in the bit-table.
     *
     * @param bitTableBytes The number of bytes in the bit-table.
     * @param reader The {@link FST.BytesReader} to read. It must be positioned at the beginning of
     *     the bit-table.
     */
    static int countBits(int bitTableBytes, FST.BytesReader reader) throws IOException {
        assert bitTableBytes >= 0 : "bitTableBytes=" + bitTableBytes;
        int bitCount = 0;
        for (int i = bitTableBytes >> 3; i > 0; i--) {
            // Count the bits set for all plain longs.
            bitCount += bitCount8Bytes(reader);
        }
        int numRemainingBytes;
        if ((numRemainingBytes = bitTableBytes & (Long.BYTES - 1)) != 0) {
            bitCount += Long.bitCount(readUpTo8Bytes(numRemainingBytes, reader));
        }
        return bitCount;
    }

    /**
     * Counts the bits set up to the given bit zero-based index, exclusive. <br>
     * In other words, how many 1s there are up to the bit at the given index excluded. <br>
     * Example: bitIndex 10 means the third bit on the right of the second byte.
     *
     * @param bitIndex The bit zero-based index, exclusive. It must be greater than or equal to 0, and
     *     less than or equal to {@code number of bit-table bytes * Byte.SIZE}.
     * @param reader The {@link FST.BytesReader} to read. It must be positioned at the beginning of
     *     the bit-table.
     */
    static int countBitsUpTo(int bitIndex, FST.BytesReader reader) throws IOException {
        assert bitIndex >= 0 : "bitIndex=" + bitIndex;
        int bitCount = 0;
        for (int i = bitIndex >> 6; i > 0; i--) {
            // Count the bits set for all plain longs.
            bitCount += bitCount8Bytes(reader);
        }
        int remainingBits;
        if ((remainingBits = bitIndex & (Long.SIZE - 1)) != 0) {
            int numRemainingBytes = (remainingBits + (Byte.SIZE - 1)) >> 3;
            // Prepare a mask with 1s on the right up to bitIndex exclusive.
            long mask = (1L << bitIndex) - 1L; // Shifts are mod 64.
            // Count the bits set only within the mask part, so up to bitIndex exclusive.
            bitCount += Long.bitCount(readUpTo8Bytes(numRemainingBytes, reader) & mask);
        }
        return bitCount;
    }

    /**
     * Returns the index of the next bit set following the given bit zero-based index. <br>
     * For example with bits 100011: the next bit set after index=-1 is at index=0; the next bit set
     * after index=0 is at index=1; the next bit set after index=1 is at index=5; there is no next bit
     * set after index=5.
     *
     * @param bitIndex The bit zero-based index. It must be greater than or equal to -1, and strictly
     *     less than {@code number of bit-table bytes * Byte.SIZE}.
     * @param bitTableBytes The number of bytes in the bit-table.
     * @param reader The {@link FST.BytesReader} to read. It must be positioned at the beginning of
     *     the bit-table.
     * @return The zero-based index of the next bit set after the provided {@code bitIndex}; or -1 if
     *     none.
     */
    static int nextBitSet(int bitIndex, int bitTableBytes, FST.BytesReader reader) throws IOException {
        assert bitIndex >= -1 && bitIndex < bitTableBytes * Byte.SIZE : "bitIndex=" + bitIndex + " bitTableBytes=" + bitTableBytes;
        int byteIndex = bitIndex / Byte.SIZE;
        int mask = -1 << ((bitIndex + 1) & (Byte.SIZE - 1));
        int i;
        if (mask == -1 && bitIndex != -1) {
            reader.skipBytes(byteIndex + 1);
            i = 0;
        } else {
            reader.skipBytes(byteIndex);
            i = (reader.readByte() & 0xFF) & mask;
        }
        while (i == 0) {
            if (++byteIndex == bitTableBytes) {
                return -1;
            }
            i = reader.readByte() & 0xFF;
        }
        return Integer.numberOfTrailingZeros(i) + (byteIndex << 3);
    }

    /**
     * Returns the index of the previous bit set preceding the given bit zero-based index. <br>
     * For example with bits 100011: there is no previous bit set before index=0. the previous bit set
     * before index=1 is at index=0; the previous bit set before index=5 is at index=1; the previous
     * bit set before index=64 is at index=5;
     *
     * @param bitIndex The bit zero-based index. It must be greater than or equal to 0, and less than
     *     or equal to {@code number of bit-table bytes * Byte.SIZE}.
     * @param reader The {@link FST.BytesReader} to read. It must be positioned at the beginning of
     *     the bit-table.
     * @return The zero-based index of the previous bit set before the provided {@code bitIndex}; or
     *     -1 if none.
     */
    static int previousBitSet(int bitIndex, FST.BytesReader reader) throws IOException {
        assert bitIndex >= 0 : "bitIndex=" + bitIndex;
        int byteIndex = bitIndex >> 3;
        reader.skipBytes(byteIndex);
        int mask = (1 << (bitIndex & (Byte.SIZE - 1))) - 1;
        int i = (reader.readByte() & 0xFF) & mask;
        while (i == 0) {
            if (byteIndex-- == 0) {
                return -1;
            }
            reader.skipBytes(-2); // FST.BytesReader implementations support negative skip.
            i = reader.readByte() & 0xFF;
        }
        return (Integer.SIZE - 1) - Integer.numberOfLeadingZeros(i) + (byteIndex << 3);
    }

    private static long readByte(FST.BytesReader reader) throws IOException {
        return reader.readByte() & 0xFFL;
    }

    private static long readUpTo8Bytes(int numBytes, FST.BytesReader reader) throws IOException {
        assert numBytes > 0 && numBytes <= 8 : "numBytes=" + numBytes;
        long l = readByte(reader);
        int shift = 0;
        while (--numBytes != 0) {
            l |= readByte(reader) << (shift += 8);
        }
        return l;
    }

    private static int bitCount8Bytes(FST.BytesReader reader) throws IOException {
        return Long.bitCount(reader.readLong());
    }
}
