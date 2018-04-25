/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class StatsAccumulatorTests extends ESTestCase {

    public void testGivenNoValues() {
        StatsAccumulator accumulator = new StatsAccumulator();
        assertThat(accumulator.getMin(), equalTo(0.0));
        assertThat(accumulator.getMax(), equalTo(0.0));
        assertThat(accumulator.getTotal(), equalTo(0.0));
        assertThat(accumulator.getAvg(), equalTo(0.0));
    }

    public void testGivenPositiveValues() {
        StatsAccumulator accumulator = new StatsAccumulator();

        for (int i = 1; i <= 10; i++) {
            accumulator.add(i);
        }

        assertThat(accumulator.getMin(), equalTo(1.0));
        assertThat(accumulator.getMax(), equalTo(10.0));
        assertThat(accumulator.getTotal(), equalTo(55.0));
        assertThat(accumulator.getAvg(), equalTo(5.5));
    }

    public void testGivenNegativeValues() {
        StatsAccumulator accumulator = new StatsAccumulator();

        for (int i = 1; i <= 10; i++) {
            accumulator.add(-1 * i);
        }

        assertThat(accumulator.getMin(), equalTo(-10.0));
        assertThat(accumulator.getMax(), equalTo(-1.0));
        assertThat(accumulator.getTotal(), equalTo(-55.0));
        assertThat(accumulator.getAvg(), equalTo(-5.5));
    }

    public void testAsMap() {
        StatsAccumulator accumulator = new StatsAccumulator();
        accumulator.add(5.0);
        accumulator.add(10.0);

        Map<String, Double> expectedMap = new HashMap<>();
        expectedMap.put("min", 5.0);
        expectedMap.put("max", 10.0);
        expectedMap.put("avg", 7.5);
        expectedMap.put("total", 15.0);
        assertThat(accumulator.asMap(), equalTo(expectedMap));
    }
}