/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

public class BinaryRangeUtilTests extends ESTestCase {

    public void testBasics() {
        BytesRef encoded1 = new BytesRef(BinaryRangeUtil.encodeLong(Long.MIN_VALUE));
        BytesRef encoded2 = new BytesRef(BinaryRangeUtil.encodeLong(-1L));
        BytesRef encoded3 = new BytesRef(BinaryRangeUtil.encodeLong(0L));
        BytesRef encoded4 = new BytesRef(BinaryRangeUtil.encodeLong(1L));
        BytesRef encoded5 = new BytesRef(BinaryRangeUtil.encodeLong(Long.MAX_VALUE));

        assertTrue(encoded1.compareTo(encoded2) < 0);
        assertTrue(encoded2.compareTo(encoded1) > 0);
        assertTrue(encoded2.compareTo(encoded3) < 0);
        assertTrue(encoded3.compareTo(encoded2) > 0);
        assertTrue(encoded3.compareTo(encoded4) < 0);
        assertTrue(encoded4.compareTo(encoded3) > 0);
        assertTrue(encoded4.compareTo(encoded5) < 0);
        assertTrue(encoded5.compareTo(encoded4) > 0);

        encoded1 = new BytesRef(BinaryRangeUtil.encodeDouble(Double.NEGATIVE_INFINITY));
        encoded2 = new BytesRef(BinaryRangeUtil.encodeDouble(-1D));
        encoded3 = new BytesRef(BinaryRangeUtil.encodeDouble(-0D));
        encoded4 = new BytesRef(BinaryRangeUtil.encodeDouble(0D));
        encoded5 = new BytesRef(BinaryRangeUtil.encodeDouble(1D));
        BytesRef encoded6 = new BytesRef(BinaryRangeUtil.encodeDouble(Double.POSITIVE_INFINITY));

        assertTrue(encoded1.compareTo(encoded2) < 0);
        assertTrue(encoded2.compareTo(encoded1) > 0);
        assertTrue(encoded2.compareTo(encoded3) < 0);
        assertTrue(encoded3.compareTo(encoded2) > 0);
        assertTrue(encoded3.compareTo(encoded4) < 0);
        assertTrue(encoded4.compareTo(encoded3) > 0);
        assertTrue(encoded4.compareTo(encoded5) < 0);
        assertTrue(encoded5.compareTo(encoded4) > 0);
        assertTrue(encoded5.compareTo(encoded6) < 0);
        assertTrue(encoded6.compareTo(encoded5) > 0);

        encoded1 = new BytesRef(BinaryRangeUtil.encodeFloat(Float.NEGATIVE_INFINITY));
        encoded2 = new BytesRef(BinaryRangeUtil.encodeFloat(-1F));
        encoded3 = new BytesRef(BinaryRangeUtil.encodeFloat(-0F));
        encoded4 = new BytesRef(BinaryRangeUtil.encodeFloat(0F));
        encoded5 = new BytesRef(BinaryRangeUtil.encodeFloat(1F));
        encoded6 = new BytesRef(BinaryRangeUtil.encodeFloat(Float.POSITIVE_INFINITY));

        assertTrue(encoded1.compareTo(encoded2) < 0);
        assertTrue(encoded2.compareTo(encoded1) > 0);
        assertTrue(encoded2.compareTo(encoded3) < 0);
        assertTrue(encoded3.compareTo(encoded2) > 0);
        assertTrue(encoded3.compareTo(encoded4) < 0);
        assertTrue(encoded4.compareTo(encoded3) > 0);
        assertTrue(encoded4.compareTo(encoded5) < 0);
        assertTrue(encoded5.compareTo(encoded4) > 0);
        assertTrue(encoded5.compareTo(encoded6) < 0);
        assertTrue(encoded6.compareTo(encoded5) > 0);
    }

    public void testEncode_long() {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            long number1 = randomLong();
            BytesRef encodedNumber1 = new BytesRef(BinaryRangeUtil.encodeLong(number1));
            long number2 = randomBoolean() ? number1 + 1 : randomLong();
            BytesRef encodedNumber2 = new BytesRef(BinaryRangeUtil.encodeLong(number2));

            int cmp = normalize(Long.compare(number1, number2));
            assertEquals(cmp, normalize(encodedNumber1.compareTo(encodedNumber2)));
            cmp = normalize(Long.compare(number2, number1));
            assertEquals(cmp, normalize(encodedNumber2.compareTo(encodedNumber1)));
        }
    }

    public void testVariableLengthEncoding() {
        for (int i = -8; i <= 7; ++i) {
            assertEquals(1, BinaryRangeUtil.encodeLong(i).length);
        }
        for (int i = -2048; i <= 2047; ++i) {
            if (i < -8 ||i > 7) {
                assertEquals(2, BinaryRangeUtil.encodeLong(i).length);
            }
        }
        assertEquals(3, BinaryRangeUtil.encodeLong(-2049).length);
        assertEquals(3, BinaryRangeUtil.encodeLong(2048).length);
        assertEquals(9, BinaryRangeUtil.encodeLong(Long.MIN_VALUE).length);
        assertEquals(9, BinaryRangeUtil.encodeLong(Long.MAX_VALUE).length);
    }

    public void testEncode_double() {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            double number1 = randomDouble();
            BytesRef encodedNumber1 = new BytesRef(BinaryRangeUtil.encodeDouble(number1));
            double number2 = randomBoolean() ? Math.nextUp(number1) : randomDouble();
            BytesRef encodedNumber2 = new BytesRef(BinaryRangeUtil.encodeDouble(number2));

            assertEquals(8, encodedNumber1.length);
            assertEquals(8, encodedNumber2.length);
            int cmp = normalize(Double.compare(number1, number2));
            assertEquals(cmp, normalize(encodedNumber1.compareTo(encodedNumber2)));
            cmp = normalize(Double.compare(number2, number1));
            assertEquals(cmp, normalize(encodedNumber2.compareTo(encodedNumber1)));
        }
    }

    public void testEncode_Float() {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            float number1 = randomFloat();
            BytesRef encodedNumber1 = new BytesRef(BinaryRangeUtil.encodeFloat(number1));
            float number2 = randomBoolean() ? Math.nextUp(number1) : randomFloat();
            BytesRef encodedNumber2 = new BytesRef(BinaryRangeUtil.encodeFloat(number2));

            assertEquals(4, encodedNumber1.length);
            assertEquals(4, encodedNumber2.length);
            int cmp = normalize(Double.compare(number1, number2));
            assertEquals(cmp, normalize(encodedNumber1.compareTo(encodedNumber2)));
            cmp = normalize(Double.compare(number2, number1));
            assertEquals(cmp, normalize(encodedNumber2.compareTo(encodedNumber1)));
        }
    }

    private static int normalize(int cmp) {
        if (cmp < 0) {
            return -1;
        } else if (cmp > 0) {
            return 1;
        }
        return 0;
    }

}
