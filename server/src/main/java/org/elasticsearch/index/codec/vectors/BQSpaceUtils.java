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

/** Utility class for quantization calculations */
public class BQSpaceUtils {

    public static final short B_QUERY = 4;
    // the first four bits masked
    private static final int B_QUERY_MASK = 15;

    /**
     * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
     * @param q the query vector, assumed to be half-byte quantized with values between 0 and 15
     * @param dimensions the number of dimensions in the query vector
     * @param quantQueryByte the byte array to store the transposed query vector
     */
    public static void transposeBin(byte[] q, int dimensions, byte[] quantQueryByte) {
        // TODO: rewrite this in Panama Vector API
        int qOffset = 0;
        final byte[] v1 = new byte[4];
        final byte[] v = new byte[32];
        for (int i = 0; i < dimensions; i += 32) {
            // for every four bytes we shift left (with remainder across those bytes)
            for (int j = 0; j < v.length; j += 4) {
                v[j] = (byte) (q[qOffset + j] << B_QUERY | ((q[qOffset + j] >>> B_QUERY) & B_QUERY_MASK));
                v[j + 1] = (byte) (q[qOffset + j + 1] << B_QUERY | ((q[qOffset + j + 1] >>> B_QUERY) & B_QUERY_MASK));
                v[j + 2] = (byte) (q[qOffset + j + 2] << B_QUERY | ((q[qOffset + j + 2] >>> B_QUERY) & B_QUERY_MASK));
                v[j + 3] = (byte) (q[qOffset + j + 3] << B_QUERY | ((q[qOffset + j + 3] >>> B_QUERY) & B_QUERY_MASK));
            }
            for (int j = 0; j < B_QUERY; j++) {
                moveMaskEpi8Byte(v, v1);
                for (int k = 0; k < 4; k++) {
                    quantQueryByte[(B_QUERY - j - 1) * (dimensions / 8) + i / 8 + k] = v1[k];
                    v1[k] = 0;
                }
                for (int k = 0; k < v.length; k += 4) {
                    v[k] = (byte) (v[k] + v[k]);
                    v[k + 1] = (byte) (v[k + 1] + v[k + 1]);
                    v[k + 2] = (byte) (v[k + 2] + v[k + 2]);
                    v[k + 3] = (byte) (v[k + 3] + v[k + 3]);
                }
            }
            qOffset += 32;
        }
    }

    private static void moveMaskEpi8Byte(byte[] v, byte[] v1b) {
        int m = 0;
        for (int k = 0; k < v.length; k++) {
            if ((v[k] & 0b10000000) == 0b10000000) {
                v1b[m] |= 0b00000001;
            }
            if (k % 8 == 7) {
                m++;
            } else {
                v1b[m] <<= 1;
            }
        }
    }
}
