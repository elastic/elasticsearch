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

package org.elasticsearch.common.rounding;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RoundingTests extends ESTestCase {

    /**
     * simple test case to illustrate how Rounding.Interval works on readable input
     */
    @Test
    public void testInterval() {
        int interval = 10;
        Rounding.Interval rounding = new Rounding.Interval(interval);
        int value = 24;
        final long key = rounding.roundKey(24);
        final long r = rounding.round(24);
        String message = "round(" + value + ", interval=" + interval + ") = " + r;
        assertEquals(value/interval, key);
        assertEquals(value/interval * interval, r);
        assertEquals(message, 0, r % interval);
    }

    @Test
    public void testIntervalRandom() {
        final long interval = randomIntBetween(1, 100);
        Rounding.Interval rounding = new Rounding.Interval(interval);
        for (int i = 0; i < 1000; ++i) {
            long l = Math.max(randomLong(), Long.MIN_VALUE + interval);
            final long key = rounding.roundKey(l);
            final long r = rounding.round(l);
            String message = "round(" + l + ", interval=" + interval + ") = " + r;
            assertEquals(message, 0, r % interval);
            assertThat(message, r, lessThanOrEqualTo(l));
            assertThat(message, r + interval, greaterThan(l));
            assertEquals(message, r, key*interval);
        }
    }

    /**
     * Simple test case to illustrate how Rounding.Offset works on readable input.
     * offset shifts input value back before rounding (so here 6 - 7 -> -1)
     * then shifts rounded Value back  (here -10 -> -3)
     */
    @Test
    public void testOffsetRounding() {
        final long interval = 10;
        final long offset = 7;
        Rounding.OffsetRounding rounding = new Rounding.OffsetRounding(new Rounding.Interval(interval), offset);
        assertEquals(-1, rounding.roundKey(6));
        assertEquals(-3, rounding.round(6));
        assertEquals(7, rounding.nextRoundingValue(-3));
        assertEquals(0, rounding.roundKey(7));
        assertEquals(7, rounding.round(7));
        assertEquals(17, rounding.nextRoundingValue(7));
        assertEquals(0, rounding.roundKey(16));
        assertEquals(7, rounding.round(16));
        assertEquals(1, rounding.roundKey(17));
        assertEquals(17, rounding.round(17));
        assertEquals(27, rounding.nextRoundingValue(17));
    }

    /**
     * test OffsetRounding with an internal interval rounding on random inputs
     */
    @Test
    public void testOffsetRoundingRandom() {
        for (int i = 0; i < 1000; ++i) {
            final long interval = randomIntBetween(1, 100);
            Rounding.Interval internalRounding = new Rounding.Interval(interval);
            final long offset = randomIntBetween(-100, 100);
            Rounding.OffsetRounding rounding = new Rounding.OffsetRounding(internalRounding, offset);
            long safetyMargin = Math.abs(interval) + Math.abs(offset); // to prevent range overflow
            long value = Math.max(randomLong() - safetyMargin, Long.MIN_VALUE + safetyMargin);
            final long key = rounding.roundKey(value);
            final long key_next = rounding.roundKey(value + interval);
            final long r_value = rounding.round(value);
            final long nextRoundingValue = rounding.nextRoundingValue(r_value);
            assertThat("Rounding should be idempotent", r_value, equalTo(rounding.round(r_value)));
            assertThat("Rounded value smaller than unrounded, regardless of offset", r_value - offset, lessThanOrEqualTo(value - offset));
            assertThat("Key and next_key should differ by one", key_next - key, equalTo(1L));
            assertThat("Rounded value <= value < next interval start", r_value + interval, greaterThan(value));
            assertThat("NextRounding value should be interval from rounded value", r_value + interval, equalTo(nextRoundingValue));
        }
    }
}
