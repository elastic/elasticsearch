/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class HistogramConverterTests extends ESTestCase {

    @SuppressWarnings("unused")
    private final String name;
    private final HistogramDataPoint dataPoint;
    private final List<Long> expectedCounts;
    private final List<Double> expectedValues;
    private final boolean valid;

    public HistogramConverterTests(
        String name,
        HistogramDataPoint dataPoint,
        List<Long> expectedCounts,
        List<Double> expectedValues,
        boolean valid
    ) {
        this.name = name;
        this.dataPoint = dataPoint;
        this.expectedCounts = expectedCounts;
        this.expectedValues = expectedValues;
        this.valid = valid;
    }

    public void testHistograms() throws Exception {
        DataPoint.Histogram histogram = new DataPoint.Histogram(
            dataPoint,
            Metric.newBuilder()
                .setHistogram(
                    Histogram.newBuilder()
                        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
                        .addDataPoints(dataPoint)
                )
                .build()
        );
        assertThat(histogram.isValid(new HashSet<>()), equalTo(valid));
        if (valid == false) {
            return;
        }
        List<Long> actualCounts = new ArrayList<>();
        HistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        HistogramConverter.centroidValues(dataPoint, actualValues::add);

        assertEquals(expectedCounts, actualCounts);
        assertEquals(expectedValues.size(), actualValues.size());
        for (int i = 0; i < expectedValues.size(); i++) {
            assertEquals(expectedValues.get(i), actualValues.get(i), 1e-10);
        }
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> testCases() {
        return List.of(
            new Object[] { "empty", HistogramDataPoint.newBuilder().build(), List.of(), List.of(), true },
            new Object[] {
                "single bucket",
                HistogramDataPoint.newBuilder().addBucketCounts(10L).addExplicitBounds(5.0).build(),
                List.of(10L),
                List.of(2.5),
                true },
            new Object[] { "single count", HistogramDataPoint.newBuilder().addBucketCounts(10L).build(), List.of(10L), List.of(), false },
            new Object[] {
                "two buckets",
                HistogramDataPoint.newBuilder().addAllBucketCounts(List.of(5L, 10L)).addExplicitBounds(5.0).build(),
                List.of(5L, 10L),
                List.of(2.5, 5.0),
                true },
            new Object[] {
                "three buckets",
                HistogramDataPoint.newBuilder().addAllBucketCounts(List.of(5L, 10L, 15L)).addAllExplicitBounds(List.of(5.0, 10.0)).build(),
                List.of(5L, 10L, 15L),
                List.of(2.5, 7.5, 10.0),
                true },
            new Object[] {
                "zero count buckets",
                HistogramDataPoint.newBuilder().addAllBucketCounts(List.of(5L, 0L, 15L)).addAllExplicitBounds(List.of(5.0, 10.0)).build(),
                List.of(5L, 15L),
                List.of(2.5, 10.0),
                true },
            new Object[] {
                "negative bounds",
                HistogramDataPoint.newBuilder()
                    .addAllBucketCounts(List.of(5L, 10L, 15L))
                    .addAllExplicitBounds(List.of(-10.0, 10.0))
                    .build(),
                List.of(5L, 10L, 15L),
                List.of(-10.0, 0.0, 10.0),
                true },
            new Object[] {
                "all negative bounds",
                HistogramDataPoint.newBuilder().addAllBucketCounts(List.of(5L, 10L)).addExplicitBounds(-5.0).build(),
                List.of(5L, 10L),
                List.of(-5.0, -5.0),
                true },
            new Object[] {
                "multiple buckets with varying distances",
                HistogramDataPoint.newBuilder()
                    .addAllBucketCounts(List.of(5L, 10L, 15L, 20L))
                    .addAllExplicitBounds(List.of(1.0, 5.0, 20.0))
                    .build(),
                List.of(5L, 10L, 15L, 20L),
                List.of(0.5, 3.0, 12.5, 20.0),
                true }
        );
    }
}
