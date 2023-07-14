/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class IntRangeVectorTests extends ESTestCase {

    public void testBasic() {
        for (int i = 0; i < 100; i++) {
            int startInclusive = randomIntBetween(1, 100);
            int endExclusive = randomIntBetween(101, 1000);
            int positions = endExclusive - startInclusive;
            var vector = new IntRangeVector(startInclusive, endExclusive);
            assertThat(vector.getPositionCount(), is(positions));
            assertRangeValues(vector);
            assertThat(vector.ascending(), is(true));
            assertThat(IntRangeVector.isRangeFromMToN(vector, startInclusive, endExclusive), is(true));
        }
    }

    public void testEmpty() {
        var vector = new IntRangeVector(0, 0);
        assertThat(vector.getPositionCount(), is(0));
        assertThat(vector.ascending(), is(true));
        assertThat(IntRangeVector.isRangeFromMToN(vector, 0, 0), is(true));
    }

    static void assertRangeValues(IntVector vector) {
        int v = vector.getInt(0);
        for (int i = 0; i < vector.getPositionCount(); i++) {
            assertThat(vector.getInt(i), is(v + i));
        }
    }
}
