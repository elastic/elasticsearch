/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

public class CounterResetDataPointsTests extends ESTestCase {

    public void testEmptyByDefault() {
        CounterResetDataPoints dataPoints = new CounterResetDataPoints();
        assertThat(dataPoints.isEmpty(), equalTo(true));
        assertThat(dataPoints.countResetDocuments(), equalTo(0));
    }

    public void testAddSingleDataPoint() {
        CounterResetDataPoints dataPoints = new CounterResetDataPoints();
        dataPoints.addDataPoint(
            "counter_a",
            new CounterResetDataPoints.ResetPoint(randomLongBetween(100, 10000), randomIntBetween(0, 10000))
        );

        assertThat(dataPoints.isEmpty(), equalTo(false));
        assertThat(dataPoints.countResetDocuments(), equalTo(1));
    }

    public void testAddMultipleDataPointsAtDifferentTimestamps() {
        CounterResetDataPoints dataPoints = new CounterResetDataPoints();
        dataPoints.addDataPoint("counter_a", new CounterResetDataPoints.ResetPoint(100L, randomIntBetween(0, 10000)));
        dataPoints.addDataPoint("counter_a", new CounterResetDataPoints.ResetPoint(200L, randomIntBetween(0, 10000)));
        dataPoints.addDataPoint("counter_a", new CounterResetDataPoints.ResetPoint(300L, randomIntBetween(0, 10000)));

        assertThat(dataPoints.countResetDocuments(), equalTo(3));
    }

    public void testAddMultipleCountersAtSameTimestamp() {
        CounterResetDataPoints dataPoints = new CounterResetDataPoints();
        long timestamp = randomLongBetween(100, 10000);
        dataPoints.addDataPoint("counter_a", new CounterResetDataPoints.ResetPoint(timestamp, 10.0));
        dataPoints.addDataPoint("counter_b", new CounterResetDataPoints.ResetPoint(timestamp, 20.0));

        assertThat(dataPoints.countResetDocuments(), equalTo(1));

        dataPoints.processDataPoints((t, values) -> {
            assertThat(t, equalTo(timestamp));
            assertThat(values, hasItem(Tuple.tuple("counter_a", 10.0)));
            assertThat(values, hasItem(Tuple.tuple("counter_b", 20.0)));
        });
    }

    public void testProcessDataPointsOnEmpty() {
        CounterResetDataPoints dataPoints = new CounterResetDataPoints();
        List<Long> visited = new ArrayList<>();
        dataPoints.processDataPoints((timestamp, values) -> visited.add(timestamp));

        assertThat(visited, hasSize(0));
    }

    public void testProcessDataPointsVisitsAllTimestamps() {
        CounterResetDataPoints dataPoints = new CounterResetDataPoints();
        dataPoints.addDataPoint("c1", new CounterResetDataPoints.ResetPoint(10L, 1.0));
        dataPoints.addDataPoint("c2", new CounterResetDataPoints.ResetPoint(20L, 2.0));
        dataPoints.addDataPoint("c3", new CounterResetDataPoints.ResetPoint(30L, 3.0));

        List<Long> timestamps = new ArrayList<>();
        dataPoints.processDataPoints((timestamp, values) -> timestamps.add(timestamp));

        assertThat(timestamps, hasSize(3));
        assertTrue(timestamps.contains(10L));
        assertTrue(timestamps.contains(20L));
        assertTrue(timestamps.contains(30L));
    }

    public void testProcessDataPointsWithMixedCountersAndTimestamps() {
        CounterResetDataPoints dataPoints = new CounterResetDataPoints();
        dataPoints.addDataPoint("cpu", new CounterResetDataPoints.ResetPoint(100L, 50.0));
        dataPoints.addDataPoint("mem", new CounterResetDataPoints.ResetPoint(100L, 80.0));
        dataPoints.addDataPoint("cpu", new CounterResetDataPoints.ResetPoint(200L, 5.0));

        assertThat(dataPoints.countResetDocuments(), equalTo(2));

        dataPoints.processDataPoints((timestamp, values) -> {
            if (timestamp == 100L) {
                assertThat(values, hasItems(Tuple.tuple("cpu", 50.0), Tuple.tuple("mem", 80.0)));
            } else if (timestamp == 200L) {
                assertThat(values, equalTo(List.of(Tuple.tuple("cpu", 5.0))));
            } else {
                fail("unexpected timestamp: " + timestamp);
            }
        });
    }
}
