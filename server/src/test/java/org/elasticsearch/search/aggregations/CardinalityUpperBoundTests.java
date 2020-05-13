package org.elasticsearch.search.aggregations;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class CardinalityUpperBoundTests extends ESTestCase {
    public void testNoneMultiply() {
        assertThat(CardinalityUpperBound.NONE.multiply(randomInt()), equalTo(CardinalityUpperBound.NONE));
    }

    public void testOneMultiply() {
        assertThat(CardinalityUpperBound.ONE.multiply(0), equalTo(CardinalityUpperBound.NONE));
        assertThat(CardinalityUpperBound.ONE.multiply(1), equalTo(CardinalityUpperBound.ONE));
        assertThat(CardinalityUpperBound.ONE.multiply(between(2, Integer.MAX_VALUE)), equalTo(CardinalityUpperBound.MANY));
    }

    public void testManyMultiply() {
        assertThat(CardinalityUpperBound.MANY.multiply(0), equalTo(CardinalityUpperBound.NONE));
        assertThat(CardinalityUpperBound.MANY.multiply(between(1, Integer.MAX_VALUE)), equalTo(CardinalityUpperBound.MANY));
    }
}
