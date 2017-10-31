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

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.function.DoubleToLongFunction;
import java.util.function.LongToDoubleFunction;

public class CustomFloatTests extends ESTestCase {

    public void testIllegalInput() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> CustomFloat.getEncoder(0, -100));
        assertEquals("numSigBits must be >= 1, got 0", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                () -> CustomFloat.getEncoder(54, -100));
        assertEquals("numSigBits must be <= 53, got 54", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                () -> CustomFloat.getEncoder(10, -1024));
        assertEquals("zeroExponent must be >= -1023, got -1024", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                () -> CustomFloat.getEncoder(10, 1024));
        assertEquals("zeroExponent must be <= 1023, got 1024", e.getMessage());

        DoubleToLongFunction encoder = CustomFloat.getEncoder(randomIntBetween(1, 53), randomIntBetween(-1023, 0));
        e = expectThrows(IllegalArgumentException.class,
                () -> encoder.applyAsLong(Double.POSITIVE_INFINITY));
        assertEquals("Cannot encode non-finite value: Infinity", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                () -> encoder.applyAsLong(Double.NEGATIVE_INFINITY));
        assertEquals("Cannot encode non-finite value: -Infinity", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                () -> encoder.applyAsLong(Double.NaN));
        assertEquals("Cannot encode non-finite value: NaN", e.getMessage());

        DoubleToLongFunction encoder2 = CustomFloat.getEncoder(randomIntBetween(1, 51), randomIntBetween(-1023, 0));
        e = expectThrows(IllegalArgumentException.class,
                () -> encoder2.applyAsLong(Math.nextDown(Double.POSITIVE_INFINITY)));
        assertThat(e.getMessage(), Matchers.containsString("is too large and was rounded to +/-Infinity"));
    }

    public void testDuelFloat() {
        DoubleToLongFunction encoder = CustomFloat.getEncoder(24, -127);
        LongToDoubleFunction decoder = CustomFloat.getDecoder(24, -127);
        for (int i = 0; i < 100; ++i) {
            // Doubles that are likely normal
            double d = (randomDouble() - 0.3) * 10000;
            long encoded = encoder.applyAsLong(d);
            double decoded = decoder.applyAsDouble(encoded);
            assertEquals("" + (float) d + " != " + decoded,
                    Double.doubleToLongBits((float) d), Double.doubleToLongBits(decoded));
        }

        for (int i = 0; i < 100; ++i) {
            // Likely subnormal floats
            double d = (randomDouble() - 0.3) * Float.MIN_VALUE * 100000;
            long encoded = encoder.applyAsLong(d);
            double decoded = decoder.applyAsDouble(encoded);
            assertEquals("" + (double) (float) d + " != " + decoded,
                    Double.doubleToLongBits((float) d), Double.doubleToLongBits(decoded));
        }
    }

    public void testDuelDouble() {
        DoubleToLongFunction encoder = CustomFloat.getEncoder(53, -1023);
        LongToDoubleFunction decoder = CustomFloat.getDecoder(53, -1023);
        for (int i = 0; i < 100; ++i) {
            double d = (randomDouble() - 0.3) * 10000;
            long encoded = encoder.applyAsLong(d);
            double decoded = decoder.applyAsDouble(encoded);
            assertEquals("" + d + " != " + decoded,
                    Double.doubleToLongBits(d), Double.doubleToLongBits(decoded));
        }

        for (int i = 0; i < 100; ++i) {
            // Likely subnormal doubles
            double d = (randomDouble() - 0.3) * 100000 * Double.MIN_VALUE;
            long encoded = encoder.applyAsLong(d);
            double decoded = decoder.applyAsDouble(encoded);
            assertEquals("" + d + " != " + decoded,
                    Double.doubleToLongBits(d), Double.doubleToLongBits(decoded));
        }
    }

    public void testEncodeLongs() {
        for (int iter = 0; iter < 10; ++iter) {
            int numSigBits = randomIntBetween(1, 53);
            int zeroExp = numSigBits - 2;
            DoubleToLongFunction encoder = CustomFloat.getEncoder(numSigBits, zeroExp);
            LongToDoubleFunction decoder = CustomFloat.getDecoder(numSigBits, zeroExp);
            for (int i = 0; i <= (1 << numSigBits); ++i) {
                long encoded = encoder.applyAsLong(i);
                double decoded = decoder.applyAsDouble(encoded);
                assertEquals(i, decoded, 0d);
                assertEquals(i, encoded);
            }
        }
    }

    public void testMaintainsOrder() {
        int numSigBits = randomIntBetween(3, 10);
        int zeroExp = randomIntBetween(-1023, numSigBits - 2);
        DoubleToLongFunction encoder = CustomFloat.getEncoder(numSigBits, zeroExp);
        assertTrue(encoder.applyAsLong(-2) < encoder.applyAsLong(-1));
        assertTrue(encoder.applyAsLong(-1) < encoder.applyAsLong(0));
        assertTrue(encoder.applyAsLong(0) < encoder.applyAsLong(1));
        assertTrue(encoder.applyAsLong(1) < encoder.applyAsLong(2));
        assertTrue(encoder.applyAsLong(3) <= encoder.applyAsLong(Math.PI));
        assertTrue(encoder.applyAsLong(Math.PI) <= encoder.applyAsLong(4));
    }
}
