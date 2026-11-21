/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MinMaxDoubleNaNTests extends ESTestCase {

    public void testMinSkipsNaN() {
        double acc = MinDoubleAggregator.init();
        for (double v : new double[] { Double.NaN, 3.0, Double.NaN, 1.0, Double.NaN }) {
            acc = MinDoubleAggregator.combine(acc, v);
        }
        assertThat(acc, equalTo(1.0));
    }

    public void testMaxSkipsNaN() {
        double acc = MaxDoubleAggregator.init();
        for (double v : new double[] { Double.NaN, 3.0, Double.NaN, 1.0, Double.NaN }) {
            acc = MaxDoubleAggregator.combine(acc, v);
        }
        assertThat(acc, equalTo(3.0));
    }

    public void testAllNaNIsNaN() {
        double min = MinDoubleAggregator.init();
        double max = MaxDoubleAggregator.init();
        for (int i = 0; i < 3; i++) {
            min = MinDoubleAggregator.combine(min, Double.NaN);
            max = MaxDoubleAggregator.combine(max, Double.NaN);
        }
        assertTrue(Double.isNaN(min));
        assertTrue(Double.isNaN(max));
    }

    public void testSignedZeroOrdering() {
        assertThat(MinDoubleAggregator.combine(MinDoubleAggregator.combine(MinDoubleAggregator.init(), 0.0), -0.0), equalTo(-0.0));
        assertThat(MaxDoubleAggregator.combine(MaxDoubleAggregator.combine(MaxDoubleAggregator.init(), -0.0), 0.0), equalTo(0.0));
    }

    public void testInfinitiesAreOrdinaryValues() {
        assertThat(MinDoubleAggregator.combine(MinDoubleAggregator.init(), Double.POSITIVE_INFINITY), equalTo(Double.POSITIVE_INFINITY));
        assertThat(MaxDoubleAggregator.combine(MaxDoubleAggregator.init(), Double.NEGATIVE_INFINITY), equalTo(Double.NEGATIVE_INFINITY));
    }
}
