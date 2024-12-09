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

    /**
     * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
     *
     * @param q              the query vector, assumed to be half-byte quantized with values between 0 and 15
     * @param quantQueryByte the byte array to store the transposed query vector
     */
    public static void transposeHalfByte(byte[] q, byte[] quantQueryByte) {
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
