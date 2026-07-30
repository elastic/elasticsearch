/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

public class ResetDataPointsTests extends ESTestCase {

    public void testEmptyByDefault() {
        ResetDataPoints dataPoints = new ResetDataPoints();
        assertThat(dataPoints.isEmpty(), equalTo(true));
        assertThat(dataPoints.countResetDocuments(), equalTo(0));
    }

    public void testAddSingleDataPoint() {
        ResetDataPoints dataPoints = new ResetDataPoints();
        dataPoints.addDataPoint(
            "counter_a",
            new ResetDataPoints.ResetPoint(randomLongBetween(100, 10000), (double) randomIntBetween(0, 10000))
        );

        assertThat(dataPoints.isEmpty(), equalTo(false));
        assertThat(dataPoints.countResetDocuments(), equalTo(1));
    }

    public void testAddMultipleDataPointsAtDifferentTimestamps() {
        ResetDataPoints dataPoints = new ResetDataPoints();
        dataPoints.addDataPoint("counter_a", new ResetDataPoints.ResetPoint(100L, (double) randomIntBetween(0, 10000)));
        dataPoints.addDataPoint("counter_a", new ResetDataPoints.ResetPoint(200L, (double) randomIntBetween(0, 10000)));
        dataPoints.addDataPoint("counter_a", new ResetDataPoints.ResetPoint(300L, (double) randomIntBetween(0, 10000)));

        assertThat(dataPoints.countResetDocuments(), equalTo(3));
    }

    public void testAddMultipleCountersAtSameTimestamp() throws IOException {
        ResetDataPoints dataPoints = new ResetDataPoints();
        long timestamp = randomLongBetween(100, 10000);
        dataPoints.addDataPoint("counter_a", new ResetDataPoints.ResetPoint(timestamp, 10.0));
        dataPoints.addDataPoint("counter_b", new ResetDataPoints.ResetPoint(timestamp, 20.0));

        assertThat(dataPoints.countResetDocuments(), equalTo(1));

        dataPoints.processDataPoints((t, values) -> {
            assertThat(t, equalTo(timestamp));
            assertThat(values, hasItem(Tuple.tuple("counter_a", new ResetDataPoints.CounterResetValue(10.0))));
            assertThat(values, hasItem(Tuple.tuple("counter_b", new ResetDataPoints.CounterResetValue(20.0))));
        });
    }

    public void testProcessDataPointsOnEmpty() throws IOException {
        ResetDataPoints dataPoints = new ResetDataPoints();
        List<Long> visited = new ArrayList<>();
        dataPoints.processDataPoints((timestamp, values) -> visited.add(timestamp));

        assertThat(visited, hasSize(0));
    }

    public void testProcessDataPointsVisitsAllTimestamps() throws IOException {
        ResetDataPoints dataPoints = new ResetDataPoints();
        dataPoints.addDataPoint("c1", new ResetDataPoints.ResetPoint(10L, new ResetDataPoints.CounterResetValue(1.0)));
        dataPoints.addDataPoint("c2", new ResetDataPoints.ResetPoint(20L, new ResetDataPoints.CounterResetValue(2.0)));
        dataPoints.addDataPoint("c3", new ResetDataPoints.ResetPoint(30L, new ResetDataPoints.CounterResetValue(3.0)));

        List<Long> timestamps = new ArrayList<>();
        dataPoints.processDataPoints((timestamp, values) -> timestamps.add(timestamp));

        assertThat(timestamps, hasSize(3));
        assertTrue(timestamps.contains(10L));
        assertTrue(timestamps.contains(20L));
        assertTrue(timestamps.contains(30L));
    }

    public void testProcessDataPointsWithMixedCountersAndTimestamps() throws IOException {
        ResetDataPoints dataPoints = new ResetDataPoints();
        dataPoints.addDataPoint("cpu", new ResetDataPoints.ResetPoint(100L, new ResetDataPoints.CounterResetValue(50.0)));
        dataPoints.addDataPoint("mem", new ResetDataPoints.ResetPoint(100L, new ResetDataPoints.CounterResetValue(80.0)));
        dataPoints.addDataPoint("cpu", new ResetDataPoints.ResetPoint(200L, new ResetDataPoints.CounterResetValue(5.0)));

        assertThat(dataPoints.countResetDocuments(), equalTo(2));

        dataPoints.processDataPoints((timestamp, values) -> {
            if (timestamp == 100L) {
                assertThat(
                    values,
                    hasItems(
                        Tuple.tuple("cpu", new ResetDataPoints.CounterResetValue(50.0)),
                        Tuple.tuple("mem", new ResetDataPoints.CounterResetValue(80.0))
                    )
                );
            } else if (timestamp == 200L) {
                assertThat(values, equalTo(List.of(Tuple.tuple("cpu", new ResetDataPoints.CounterResetValue(5.0)))));
            } else {
                fail("unexpected timestamp: " + timestamp);
            }
        });
    }

    public void testAddSingleHistogramDataPoint() {
        ResetDataPoints dataPoints = new ResetDataPoints();
        ExponentialHistogram h = histogram(1.0, 2.0, 3.0);
        dataPoints.addDataPoint("latency", new ResetDataPoints.ResetPoint(100L, h));

        assertThat(dataPoints.isEmpty(), equalTo(false));
        assertThat(dataPoints.countResetDocuments(), equalTo(1));
    }

    public void testAddMultipleHistogramsAtDifferentTimestamps() {
        ResetDataPoints dataPoints = new ResetDataPoints();
        dataPoints.addDataPoint("latency", new ResetDataPoints.ResetPoint(100L, histogram(1.0)));
        dataPoints.addDataPoint("latency", new ResetDataPoints.ResetPoint(200L, histogram(2.0)));
        dataPoints.addDataPoint("latency", new ResetDataPoints.ResetPoint(300L, histogram(3.0)));

        assertThat(dataPoints.countResetDocuments(), equalTo(3));
    }

    public void testAddMultipleHistogramsAtSameTimestamp() throws IOException {
        ResetDataPoints dataPoints = new ResetDataPoints();
        long timestamp = randomLongBetween(100, 10000);
        ExponentialHistogram h1 = histogram(1.0, 2.0);
        ExponentialHistogram h2 = histogram(10.0, 20.0, 30.0);

        dataPoints.addDataPoint("latency", new ResetDataPoints.ResetPoint(timestamp, h1));
        dataPoints.addDataPoint("throughput", new ResetDataPoints.ResetPoint(timestamp, h2));

        assertThat(dataPoints.countResetDocuments(), equalTo(1));

        dataPoints.processDataPoints((t, values) -> {
            assertThat(t, equalTo(timestamp));
            assertThat(values, hasSize(2));
            assertThat(values, hasItem(Tuple.tuple("latency", new ResetDataPoints.HistogramResetValue(h1))));
            assertThat(values, hasItem(Tuple.tuple("throughput", new ResetDataPoints.HistogramResetValue(h2))));
        });
    }

    public void testMixedCountersAndHistograms() throws IOException {
        ResetDataPoints dataPoints = new ResetDataPoints();
        ExponentialHistogram h = histogram(5.0, 10.0);

        dataPoints.addDataPoint("requests", new ResetDataPoints.ResetPoint(100L, 42.0));
        dataPoints.addDataPoint("latency", new ResetDataPoints.ResetPoint(100L, h));

        assertThat(dataPoints.countResetDocuments(), equalTo(1));

        dataPoints.processDataPoints((timestamp, values) -> {
            assertThat(timestamp, equalTo(100L));
            assertThat(values, hasSize(2));
            assertThat(values, hasItem(Tuple.tuple("requests", new ResetDataPoints.CounterResetValue(42.0))));
            assertThat(values, hasItem(Tuple.tuple("latency", new ResetDataPoints.HistogramResetValue(h))));
        });
    }

    public void testMixedCountersAndHistogramsAcrossTimestamps() throws IOException {
        ResetDataPoints dataPoints = new ResetDataPoints();
        ExponentialHistogram h1 = histogram(1.0, 2.0, 3.0);
        ExponentialHistogram h2 = histogram(10.0);

        dataPoints.addDataPoint("requests", new ResetDataPoints.ResetPoint(100L, 50.0));
        dataPoints.addDataPoint("latency", new ResetDataPoints.ResetPoint(100L, h1));
        dataPoints.addDataPoint("latency", new ResetDataPoints.ResetPoint(200L, h2));
        dataPoints.addDataPoint("requests", new ResetDataPoints.ResetPoint(200L, 5.0));

        assertThat(dataPoints.countResetDocuments(), equalTo(2));

        dataPoints.processDataPoints((timestamp, values) -> {
            if (timestamp == 100L) {
                assertThat(
                    values,
                    hasItems(
                        Tuple.tuple("requests", new ResetDataPoints.CounterResetValue(50.0)),
                        Tuple.tuple("latency", new ResetDataPoints.HistogramResetValue(h1))
                    )
                );
            } else if (timestamp == 200L) {
                assertThat(
                    values,
                    hasItems(
                        Tuple.tuple("latency", new ResetDataPoints.HistogramResetValue(h2)),
                        Tuple.tuple("requests", new ResetDataPoints.CounterResetValue(5.0))
                    )
                );
            } else {
                fail("unexpected timestamp: " + timestamp);
            }
        });
    }

    public void testResetPointConvenienceConstructors() {
        var counterPoint = new ResetDataPoints.ResetPoint(100L, 42.0);
        assertThat(counterPoint.value(), equalTo(new ResetDataPoints.CounterResetValue(42.0)));

        ExponentialHistogram h = histogram(1.0, 2.0);
        var histogramPoint = new ResetDataPoints.ResetPoint(100L, h);
        assertThat(histogramPoint.value(), equalTo(new ResetDataPoints.HistogramResetValue(h)));
    }

    private static ExponentialHistogram histogram(double... values) {
        return ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), values);
    }
}
