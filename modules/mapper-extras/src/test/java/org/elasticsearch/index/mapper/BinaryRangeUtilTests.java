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
        BytesRef encoded1 = new BytesRef(BinaryRangeUtil.encode(Long.MIN_VALUE));
        BytesRef encoded2 = new BytesRef(BinaryRangeUtil.encode(-1L));
        BytesRef encoded3 = new BytesRef(BinaryRangeUtil.encode(0L));
        BytesRef encoded4 = new BytesRef(BinaryRangeUtil.encode(1L));
        BytesRef encoded5 = new BytesRef(BinaryRangeUtil.encode(Long.MAX_VALUE));

        assertTrue(encoded1.compareTo(encoded2) < 0);
        assertTrue(encoded2.compareTo(encoded1) > 0);
        assertTrue(encoded2.compareTo(encoded3) < 0);
        assertTrue(encoded3.compareTo(encoded2) > 0);
        assertTrue(encoded3.compareTo(encoded4) < 0);
        assertTrue(encoded4.compareTo(encoded3) > 0);
        assertTrue(encoded4.compareTo(encoded5) < 0);
        assertTrue(encoded5.compareTo(encoded4) > 0);

        encoded1 = new BytesRef(BinaryRangeUtil.encode(Double.NEGATIVE_INFINITY));
        encoded2 = new BytesRef(BinaryRangeUtil.encode(-1D));
        encoded3 = new BytesRef(BinaryRangeUtil.encode(0D));
        encoded4 = new BytesRef(BinaryRangeUtil.encode(1D));
        encoded5 = new BytesRef(BinaryRangeUtil.encode(Double.POSITIVE_INFINITY));

        assertTrue(encoded1.compareTo(encoded2) < 0);
        assertTrue(encoded2.compareTo(encoded1) > 0);
        assertTrue(encoded2.compareTo(encoded3) < 0);
        assertTrue(encoded3.compareTo(encoded2) > 0);
        assertTrue(encoded3.compareTo(encoded4) < 0);
        assertTrue(encoded4.compareTo(encoded3) > 0);
        assertTrue(encoded4.compareTo(encoded5) < 0);
        assertTrue(encoded5.compareTo(encoded4) > 0);
    }

    public void testEncode_long() {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            long number1 = randomLong();
            BytesRef encodedNumber1 = new BytesRef(BinaryRangeUtil.encode(number1));
            long number2 = randomLong();
            BytesRef encodedNumber2 = new BytesRef(BinaryRangeUtil.encode(number2));

            int cmp = normalize(Long.compare(number1, number2));
            assertEquals(cmp, normalize(encodedNumber1.compareTo(encodedNumber2)));
            cmp = normalize(Long.compare(number2, number1));
            assertEquals(cmp, normalize(encodedNumber2.compareTo(encodedNumber1)));
        }
    }

    public void testEncode_double() {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            double number1 = randomDouble();
            BytesRef encodedNumber1 = new BytesRef(BinaryRangeUtil.encode(number1));
            double number2 = randomDouble();
            BytesRef encodedNumber2 = new BytesRef(BinaryRangeUtil.encode(number2));

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
