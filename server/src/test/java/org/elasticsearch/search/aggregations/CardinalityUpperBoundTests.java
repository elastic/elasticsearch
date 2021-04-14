/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
