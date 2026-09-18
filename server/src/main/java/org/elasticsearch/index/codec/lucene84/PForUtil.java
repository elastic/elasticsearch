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
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.lucene84;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.Arrays;

/** Utility class to encode sequences of 128 small positive integers. */
public final class PForUtil {

    static boolean allEqual(long[] l) {
        for (int i = 1; i < ForUtil.BLOCK_SIZE; ++i) {
            if (l[i] != l[0]) {
                return false;
            }
        }
        return true;
    }

    private final ForUtil forUtil;

    PForUtil(ForUtil forUtil) {
        this.forUtil = forUtil;
    }

    /** Encode 128 integers from {@code longs} into {@code out}. */
    void encode(long[] longs, DataOutput out) throws IOException {
        // At most 7 exceptions
        final long[] top8 = new long[8];
        Arrays.fill(top8, -1L);
        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
            if (longs[i] > top8[0]) {
                top8[0] = longs[i];
                Arrays.sort(top8); // For only 8 entries we just sort on every iteration instead of maintaining a PQ
            }
        }

        final int maxBitsRequired = PackedInts.bitsRequired(top8[7]);
        // We store the patch on a byte, so we can't decrease the number of bits required by more than 8
        final int patchedBitsRequired = Math.max(PackedInts.bitsRequired(top8[0]), maxBitsRequired - 8);
        int numExceptions = 0;
        final long maxUnpatchedValue = (1L << patchedBitsRequired) - 1;
        for (int i = 1; i < 8; ++i) {
            if (top8[i] > maxUnpatchedValue) {
                numExceptions++;
            }
        }
        final byte[] exceptions = new byte[numExceptions * 2];
        if (numExceptions > 0) {
            int exceptionCount = 0;
            for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
                if (longs[i] > maxUnpatchedValue) {
                    exceptions[exceptionCount * 2] = (byte) i;
                    exceptions[exceptionCount * 2 + 1] = (byte) (longs[i] >>> patchedBitsRequired);
                    longs[i] &= maxUnpatchedValue;
                    exceptionCount++;
                }
            }
            assert exceptionCount == numExceptions : exceptionCount + " " + numExceptions;
        }

        if (allEqual(longs) && maxBitsRequired <= 8) {
            for (int i = 0; i < numExceptions; ++i) {
                exceptions[2 * i + 1] = (byte) (Byte.toUnsignedLong(exceptions[2 * i + 1]) << patchedBitsRequired);
            }
            out.writeByte((byte) (numExceptions << 5));
            out.writeVLong(longs[0]);
        } else {
            final int token = (numExceptions << 5) | patchedBitsRequired;
            out.writeByte((byte) token);
            forUtil.encode(longs, patchedBitsRequired, out);
        }
        out.writeBytes(exceptions, exceptions.length);
    }

    /** Decode 128 integers into {@code longs}. */
    void decode(DataInput in, long[] longs) throws IOException {
        final int token = Byte.toUnsignedInt(in.readByte());
        final int bitsPerValue = token & 0x1f;
        final int numExceptions = token >>> 5;
        if (bitsPerValue == 0) {
            Arrays.fill(longs, 0, ForUtil.BLOCK_SIZE, in.readVLong());
        } else {
            forUtil.decode(bitsPerValue, in, longs);
        }
        for (int i = 0; i < numExceptions; ++i) {
            longs[Byte.toUnsignedInt(in.readByte())] |= Byte.toUnsignedLong(in.readByte()) << bitsPerValue;
        }
    }

    /** Skip 128 integers. */
    void skip(DataInput in) throws IOException {
        final int token = Byte.toUnsignedInt(in.readByte());
        final int bitsPerValue = token & 0x1f;
        final int numExceptions = token >>> 5;
        if (bitsPerValue == 0) {
            in.readVLong();
            in.skipBytes((numExceptions << 1));
        } else {
            in.skipBytes(forUtil.numBytes(bitsPerValue) + (numExceptions << 1));
        }
    }
}
