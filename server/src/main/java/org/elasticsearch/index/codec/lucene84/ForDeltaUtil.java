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

/** Utility class to encode/decode increasing sequences of 128 integers. */
public class ForDeltaUtil {

    // IDENTITY_PLUS_ONE[i] == i+1
    private static final long[] IDENTITY_PLUS_ONE = new long[ForUtil.BLOCK_SIZE];

    static {
        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
            IDENTITY_PLUS_ONE[i] = i + 1;
        }
    }

    private static void prefixSumOfOnes(long[] arr, long base) {
        System.arraycopy(IDENTITY_PLUS_ONE, 0, arr, 0, ForUtil.BLOCK_SIZE);
        // This loop gets auto-vectorized
        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
            arr[i] += base;
        }
    }

    private final ForUtil forUtil;

    ForDeltaUtil(ForUtil forUtil) {
        this.forUtil = forUtil;
    }

    /**
     * Encode deltas of a strictly monotonically increasing sequence of integers. The provided {@code
     * longs} are expected to be deltas between consecutive values.
     */
    void encodeDeltas(long[] longs, DataOutput out) throws IOException {
        if (longs[0] == 1 && PForUtil.allEqual(longs)) { // happens with very dense postings
            out.writeByte((byte) 0);
        } else {
            long or = 0;
            for (long l : longs) {
                or |= l;
            }
            assert or != 0;
            final int bitsPerValue = PackedInts.bitsRequired(or);
            out.writeByte((byte) bitsPerValue);
            forUtil.encode(longs, bitsPerValue, out);
        }
    }

    /** Decode deltas, compute the prefix sum and add {@code base} to all decoded longs. */
    void decodeAndPrefixSum(DataInput in, long base, long[] longs) throws IOException {
        final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
        if (bitsPerValue == 0) {
            prefixSumOfOnes(longs, base);
        } else {
            forUtil.decodeAndPrefixSum(bitsPerValue, in, base, longs);
        }
    }

    /** Skip a sequence of 128 longs. */
    void skip(DataInput in) throws IOException {
        final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
        if (bitsPerValue != 0) {
            in.skipBytes(forUtil.numBytes(bitsPerValue));
        }
    }
}
