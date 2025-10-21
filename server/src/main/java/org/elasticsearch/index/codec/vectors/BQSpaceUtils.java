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
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors;

import org.elasticsearch.simdvec.ESVectorUtil;

/** Utility class for quantization calculations */
public class BQSpaceUtils {

    public static final short B_QUERY = 4;

    /**
     * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
     * Transpose the query vector into a byte array allowing for efficient bitwise operations with the
     * index bit vectors. The idea here is to organize the query vector bits such that the first bit
     * of every dimension is in the first set dimensions bits, or (dimensions/8) bytes. The second,
     * third, and fourth bits are in the second, third, and fourth set of dimensions bits,
     * respectively. This allows for direct bitwise comparisons with the stored index vectors through
     * summing the bitwise results with the relative required bit shifts.
     *
     * @param q the query vector, assumed to be half-byte quantized with values between 0 and 15
     * @param quantQueryByte the byte array to store the transposed query vector
     */
    public static void transposeHalfByte(byte[] q, byte[] quantQueryByte) {
        int limit = q.length - 7;
        int i = 0;
        int index = 0;
        for (; i < limit; i += 8, index++) {
            assert q[i] >= 0 && q[i] <= 15;
            assert q[i + 1] >= 0 && q[i + 1] <= 15;
            assert q[i + 2] >= 0 && q[i + 2] <= 15;
            assert q[i + 3] >= 0 && q[i + 3] <= 15;
            assert q[i + 4] >= 0 && q[i + 4] <= 15;
            assert q[i + 5] >= 0 && q[i + 5] <= 15;
            assert q[i + 6] >= 0 && q[i + 6] <= 15;
            assert q[i + 7] >= 0 && q[i + 7] <= 15;
            int lowerByte = (q[i] & 1) << 7 | (q[i + 1] & 1) << 6 | (q[i + 2] & 1) << 5 | (q[i + 3] & 1) << 4 | (q[i + 4] & 1) << 3 | (q[i
                + 5] & 1) << 2 | (q[i + 6] & 1) << 1 | (q[i + 7] & 1);
            int lowerMiddleByte = ((q[i] >> 1) & 1) << 7 | ((q[i + 1] >> 1) & 1) << 6 | ((q[i + 2] >> 1) & 1) << 5 | ((q[i + 3] >> 1) & 1)
                << 4 | ((q[i + 4] >> 1) & 1) << 3 | ((q[i + 5] >> 1) & 1) << 2 | ((q[i + 6] >> 1) & 1) << 1 | ((q[i + 7] >> 1) & 1);
            int upperMiddleByte = ((q[i] >> 2) & 1) << 7 | ((q[i + 1] >> 2) & 1) << 6 | ((q[i + 2] >> 2) & 1) << 5 | ((q[i + 3] >> 2) & 1)
                << 4 | ((q[i + 4] >> 2) & 1) << 3 | ((q[i + 5] >> 2) & 1) << 2 | ((q[i + 6] >> 2) & 1) << 1 | ((q[i + 7] >> 2) & 1);
            int upperByte = ((q[i] >> 3) & 1) << 7 | ((q[i + 1] >> 3) & 1) << 6 | ((q[i + 2] >> 3) & 1) << 5 | ((q[i + 3] >> 3) & 1) << 4
                | ((q[i + 4] >> 3) & 1) << 3 | ((q[i + 5] >> 3) & 1) << 2 | ((q[i + 6] >> 3) & 1) << 1 | ((q[i + 7] >> 3) & 1);
            quantQueryByte[index] = (byte) lowerByte;
            quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
            quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
            quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
        }
        if (i == q.length) {
            return; // all done
        }
        int lowerByte = 0;
        int lowerMiddleByte = 0;
        int upperMiddleByte = 0;
        int upperByte = 0;
        for (int j = 7; i < q.length; j--, i++) {
            lowerByte |= (q[i] & 1) << j;
            lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
            upperMiddleByte |= ((q[i] >> 2) & 1) << j;
            upperByte |= ((q[i] >> 3) & 1) << j;
        }
        quantQueryByte[index] = (byte) lowerByte;
        quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
        quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
        quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
    }

    /**
     * Same as {@link #transposeHalfByte(byte[], byte[])} but with more readable but slower code.
     *
     * @param q the query vector, assumed to be half-byte quantized with values between 0 and 15
     * @param quantQueryByte the byte array to store the transposed query vector
     * */
    public static void transposeHalfByteLegacy(byte[] q, byte[] quantQueryByte) {
        for (int i = 0; i < q.length;) {
            assert q[i] >= 0 && q[i] <= 15;
            int lowerByte = 0;
            int lowerMiddleByte = 0;
            int upperMiddleByte = 0;
            int upperByte = 0;
            for (int j = 7; j >= 0 && i < q.length; j--) {
                lowerByte |= (q[i] & 1) << j;
                lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
                upperMiddleByte |= ((q[i] >> 2) & 1) << j;
                upperByte |= ((q[i] >> 3) & 1) << j;
                i++;
            }
            int index = ((i + 7) / 8) - 1;
            quantQueryByte[index] = (byte) lowerByte;
            quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
            quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
            quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
        }
    }

    /**
     * Same as {@link #transposeHalfByte(byte[], byte[])} but the input vector is provided as
     * an array of integers.
     *
     * @param q the query vector, assumed to be half-byte quantized with values between 0 and 15
     * @param quantQueryByte the byte array to store the transposed query vector
     * */
    public static void transposeHalfByte(int[] q, byte[] quantQueryByte) {
        ESVectorUtil.transposeHalfByte(q, quantQueryByte);
    }

    /**
     * Same as {@link #transposeHalfByte(int[], byte[])} but with more readable but slower code.
     *
     * @param q the query vector, assumed to be half-byte quantized with values between 0 and 15
     * @param quantQueryByte the byte array to store the transposed query vector
     * */
    public static void transposeHalfByteLegacy(int[] q, byte[] quantQueryByte) {
        for (int i = 0; i < q.length;) {
            assert q[i] >= 0 && q[i] <= 15;
            int lowerByte = 0;
            int lowerMiddleByte = 0;
            int upperMiddleByte = 0;
            int upperByte = 0;
            for (int j = 7; j >= 0 && i < q.length; j--) {
                lowerByte |= (q[i] & 1) << j;
                lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
                upperMiddleByte |= ((q[i] >> 2) & 1) << j;
                upperByte |= ((q[i] >> 3) & 1) << j;
                i++;
            }
            int index = ((i + 7) / 8) - 1;
            quantQueryByte[index] = (byte) lowerByte;
            quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
            quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
            quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
        }
    }
}
