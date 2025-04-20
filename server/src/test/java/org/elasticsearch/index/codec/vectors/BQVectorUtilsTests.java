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

import org.apache.lucene.tests.util.LuceneTestCase;

public class BQVectorUtilsTests extends LuceneTestCase {

    public static int popcount(byte[] a, int aOffset, byte[] b, int length) {
        int res = 0;
        for (int j = 0; j < length; j++) {
            int value = (a[aOffset + j] & b[j]) & 0xFF;
            for (int k = 0; k < Byte.SIZE; k++) {
                if ((value & (1 << k)) != 0) {
                    ++res;
                }
            }
        }
        return res;
    }

    private static float DELTA = Float.MIN_VALUE;

    public void testPackAsBinary() {
        // 5 bits
        byte[] toPack = new byte[] { 1, 1, 0, 0, 1 };
        byte[] packed = new byte[1];
        BQVectorUtils.packAsBinary(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001000 }, packed);

        // 8 bits
        toPack = new byte[] { 1, 1, 0, 0, 1, 0, 1, 0 };
        packed = new byte[1];
        BQVectorUtils.packAsBinary(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010 }, packed);

        // 10 bits
        toPack = new byte[] { 1, 1, 0, 0, 1, 0, 1, 0, 1, 1 };
        packed = new byte[2];
        BQVectorUtils.packAsBinary(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010, (byte) 0b11000000 }, packed);

        // 16 bits
        toPack = new byte[] { 1, 1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 0 };
        packed = new byte[2];
        BQVectorUtils.packAsBinary(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010, (byte) 0b11100110 }, packed);
    }

    public void testPadFloat() {
        assertArrayEquals(new float[] { 1, 2, 3, 4 }, BQVectorUtils.pad(new float[] { 1, 2, 3, 4 }, 4), DELTA);
        assertArrayEquals(new float[] { 1, 2, 3, 4 }, BQVectorUtils.pad(new float[] { 1, 2, 3, 4 }, 3), DELTA);
        assertArrayEquals(new float[] { 1, 2, 3, 4, 0 }, BQVectorUtils.pad(new float[] { 1, 2, 3, 4 }, 5), DELTA);
    }

    public void testPadByte() {
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, BQVectorUtils.pad(new byte[] { 1, 2, 3, 4 }, 4));
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, BQVectorUtils.pad(new byte[] { 1, 2, 3, 4 }, 3));
        assertArrayEquals(new byte[] { 1, 2, 3, 4, 0 }, BQVectorUtils.pad(new byte[] { 1, 2, 3, 4 }, 5));
    }

    public void testPopCount() {
        assertEquals(0, BQVectorUtils.popcount(new byte[] {}));
        assertEquals(1, BQVectorUtils.popcount(new byte[] { 1 }));
        assertEquals(2, BQVectorUtils.popcount(new byte[] { 2, 1 }));
        assertEquals(2, BQVectorUtils.popcount(new byte[] { 8, 0, 1 }));
        assertEquals(4, BQVectorUtils.popcount(new byte[] { 7, 1 }));

        int iterations = atLeast(50);
        for (int i = 0; i < iterations; i++) {
            int size = random().nextInt(5000);
            var a = new byte[size];
            random().nextBytes(a);
            assertEquals(popcount(a, 0, a, size), BQVectorUtils.popcount(a));
        }
    }

    public void testNorm() {
        assertEquals(3.0f, BQVectorUtils.norm(new float[] { 3 }), DELTA);
        assertEquals(5.0f, BQVectorUtils.norm(new float[] { 5 }), DELTA);
        assertEquals(4.0f, BQVectorUtils.norm(new float[] { 2, 2, 2, 2 }), DELTA);
        assertEquals(9.0f, BQVectorUtils.norm(new float[] { 3, 3, 3, 3, 3, 3, 3, 3, 3 }), DELTA);
    }

    public void testSubtract() {
        assertArrayEquals(new float[] { 1 }, BQVectorUtils.subtract(new float[] { 3 }, new float[] { 2 }), DELTA);
        assertArrayEquals(new float[] { 2, 1, 0 }, BQVectorUtils.subtract(new float[] { 3, 3, 3 }, new float[] { 1, 2, 3 }), DELTA);
    }

    public void testSubtractInPlace() {
        var a = new float[] { 3 };
        BQVectorUtils.subtractInPlace(a, new float[] { 2 });
        assertArrayEquals(new float[] { 1 }, a, DELTA);

        a = new float[] { 3, 3, 3 };
        BQVectorUtils.subtractInPlace(a, new float[] { 1, 2, 3 });
        assertArrayEquals(new float[] { 2, 1, 0 }, a, DELTA);
    }
}
