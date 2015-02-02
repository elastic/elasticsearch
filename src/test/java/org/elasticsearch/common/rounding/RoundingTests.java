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

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RoundingTests extends ElasticsearchTestCase {

    /**
     * simple testcase to ilustrate how Rounding.Interval works on readable input
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
     * Simple testcase to ilustrate how Rounding.Pre works on readable input.
     * preOffset shifts input value before rounding (so here 24 -> 31)
     * postOffset shifts rounded Value after rounding (here 30 -> 35)
     */
    @Test
    public void testPrePostRounding() {
        int interval = 10;
        int value = 24;
        int preOffset = 7;
        int postOffset = 5;
        Rounding.PrePostRounding rounding = new Rounding.PrePostRounding(new Rounding.Interval(interval), preOffset, postOffset);
        final long key = rounding.roundKey(24);
        final long roundedValue = rounding.round(24);
        String message = "round(" + value + ", interval=" + interval + ") = " + roundedValue;
        assertEquals(3, key);
        assertEquals(35, roundedValue);
        assertEquals(message, postOffset, roundedValue % interval);
    }

    @Test
    public void testPrePostRoundingRandom() {
        final long interval = randomIntBetween(1, 100);
        Rounding.Interval internalRounding = new Rounding.Interval(interval);
        final long preRounding = randomIntBetween(-100, 100);
        final long postRounding = randomIntBetween(-100, 100);
        Rounding.PrePostRounding  prePost = new Rounding.PrePostRounding(new Rounding.Interval(interval), preRounding, postRounding);
        long safetyMargin = Math.abs(interval) + Math.abs(preRounding) + Math.abs(postRounding); // to prevent range overflow / underflow
        for (int i = 0; i < 1000; ++i) {
            long l = Math.max(randomLong() - safetyMargin, Long.MIN_VALUE + safetyMargin);
            final long key = prePost.roundKey(l);
            final long r = prePost.round(l);
            String message = "round(" + l + ", interval=" + interval + ") = "+ r;
            assertEquals(message, internalRounding.round(l+preRounding), r - postRounding);
            assertThat(message, r - postRounding, lessThanOrEqualTo(l + preRounding));
            assertThat(message, r + interval - postRounding, greaterThan(l + preRounding));
            assertEquals(message, r, key*interval + postRounding);
        }
    }
}
