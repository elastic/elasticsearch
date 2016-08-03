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
import org.joda.time.DateTimeZone;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class OffsetRoundingTests extends ESTestCase {

    /**
     * Simple test case to illustrate how Rounding.Offset works on readable input.
     * offset shifts input value back before rounding (so here 6 - 7 -&gt; -1)
     * then shifts rounded Value back  (here -10 -&gt; -3)
     */
    public void testOffsetRounding() {
        final long interval = 10;
        final long offset = 7;
        Rounding.OffsetRounding rounding = new Rounding.OffsetRounding(
                new Rounding.TimeIntervalRounding(interval, DateTimeZone.UTC), offset);
        assertEquals(-3, rounding.round(6));
        assertEquals(7, rounding.nextRoundingValue(-3));
        assertEquals(7, rounding.round(7));
        assertEquals(17, rounding.nextRoundingValue(7));
        assertEquals(7, rounding.round(16));
        assertEquals(17, rounding.round(17));
        assertEquals(27, rounding.nextRoundingValue(17));
    }

    /**
     * test OffsetRounding with an internal interval rounding on random inputs
     */
    public void testOffsetRoundingRandom() {
        for (int i = 0; i < 1000; ++i) {
            final long interval = randomIntBetween(1, 100);
            Rounding internalRounding = new Rounding.TimeIntervalRounding(interval, DateTimeZone.UTC);
            final long offset = randomIntBetween(-100, 100);
            Rounding.OffsetRounding rounding = new Rounding.OffsetRounding(internalRounding, offset);
            long safetyMargin = Math.abs(interval) + Math.abs(offset); // to prevent range overflow
            long value = Math.max(randomLong() - safetyMargin, Long.MIN_VALUE + safetyMargin);
            final long r_value = rounding.round(value);
            final long nextRoundingValue = rounding.nextRoundingValue(r_value);
            assertThat("Rounding should be idempotent", r_value, equalTo(rounding.round(r_value)));
            assertThat("Rounded value smaller than unrounded, regardless of offset", r_value - offset, lessThanOrEqualTo(value - offset));
            assertThat("Rounded value <= value < next interval start", r_value + interval, greaterThan(value));
            assertThat("NextRounding value should be interval from rounded value", r_value + interval, equalTo(nextRoundingValue));
        }
    }
}
