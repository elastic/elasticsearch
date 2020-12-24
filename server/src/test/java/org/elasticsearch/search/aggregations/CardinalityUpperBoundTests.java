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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class CardinalityUpperBoundTests extends ESTestCase {
    public void testNoneMultiply() {
        assertThat(CardinalityUpperBound.NONE.multiply(randomInt()), sameInstance(CardinalityUpperBound.NONE));
    }

    public void testNoneMap() {
        assertThat(CardinalityUpperBound.NONE.map(i -> i), equalTo(0));
    }

    public void testOneMultiply() {
        assertThat(CardinalityUpperBound.ONE.multiply(0), sameInstance(CardinalityUpperBound.NONE));
        assertThat(CardinalityUpperBound.ONE.multiply(1), sameInstance(CardinalityUpperBound.ONE));
        assertThat(CardinalityUpperBound.ONE.multiply(Integer.MAX_VALUE), sameInstance(CardinalityUpperBound.MANY));
    }

    public void testOneMap() {
        assertThat(CardinalityUpperBound.ONE.map(i -> i), equalTo(1));
    }

    public void testLargerKnownValues() {
        int estimate = between(2, Short.MAX_VALUE);
        CardinalityUpperBound known = CardinalityUpperBound.ONE.multiply(estimate);
        assertThat(known.map(i -> i), equalTo(estimate));

        assertThat(known.multiply(0), sameInstance(CardinalityUpperBound.NONE));
        assertThat(known.multiply(1), sameInstance(known));
        int minOverflow = (int) Math.ceil((double) Integer.MAX_VALUE / estimate);
        assertThat(known.multiply(between(minOverflow, Integer.MAX_VALUE)), sameInstance(CardinalityUpperBound.MANY));

        int multiplier = between(2, Short.MAX_VALUE - 1);
        assertThat(known.multiply(multiplier).map(i -> i), equalTo(estimate * multiplier));
    }

    public void testManyMultiply() {
        assertThat(CardinalityUpperBound.MANY.multiply(0), sameInstance(CardinalityUpperBound.NONE));
        assertThat(CardinalityUpperBound.MANY.multiply(between(1, Integer.MAX_VALUE)), sameInstance(CardinalityUpperBound.MANY));
    }

    public void testManyMap() {
        assertThat(CardinalityUpperBound.MANY.map(i -> i), equalTo(Integer.MAX_VALUE));
    }
}
