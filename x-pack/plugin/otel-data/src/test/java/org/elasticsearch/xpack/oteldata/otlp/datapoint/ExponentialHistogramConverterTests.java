/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.Buckets;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.newBuilder;

/**
 * @see <a href="https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.132.0/exporter/elasticsearchexporter/internal/exphistogram/exphistogram_test.go">
 *     OpenTelemetry Collector Exponential Histogram Tests
 * </a>
 */
public class ExponentialHistogramConverterTests extends ESTestCase {

    private final ExponentialHistogramDataPoint dataPoint;
    private final List<Long> expectedCounts;
    private final List<Double> expectedValues;

    public ExponentialHistogramConverterTests(
        String name,
        ExponentialHistogramDataPoint.Builder builder,
        List<Long> expectedCounts,
        List<Double> expectedValues
    ) {
        this.dataPoint = builder.build();
        this.expectedCounts = expectedCounts;
        this.expectedValues = expectedValues;
    }

    public void testExponentialHistograms() {
        List<Long> actualCounts = new ArrayList<>();
        HistogramConverter.counts(dataPoint, actualCounts::add);
        assertEquals(expectedCounts, actualCounts);

        List<Double> actualValues = new ArrayList<>();
        HistogramConverter.centroidValues(dataPoint, actualValues::add);
        assertEquals(expectedValues.size(), actualValues.size());
        for (int i = 0; i < expectedValues.size(); i++) {
            assertEquals(expectedValues.get(i), actualValues.get(i), 1e-10);
        }
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> testCases() {
        return List.of(
            new Object[] { "empty", newBuilder(), List.of(), List.of() },
            new Object[] { "empty, scale=1", newBuilder().setScale(1), List.of(), List.of() },
            new Object[] { "empty, scale=-1", newBuilder().setScale(-1), List.of(), List.of() },
            new Object[] { "zeros", newBuilder().setZeroCount(1), List.of(1L), List.of(0.0) },
            new Object[] {
                "scale=0",
                newBuilder().setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L))),
                List.of(1L, 1L, 1L, 1L, 1L),
                List.of(-3.0, -1.5, 0.0, 1.5, 3.0) },
            new Object[] {
                "scale=0, no zeros",
                newBuilder().setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L))),
                List.of(1L, 1L, 1L, 1L),
                List.of(-3.0, -1.5, 1.5, 3.0) },
            new Object[] {
                "scale=0, offset=1",
                newBuilder().setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(1L, 1L))),
                List.of(1L, 1L, 1L, 1L, 1L),
                List.of(-6.0, -3.0, 0.0, 3.0, 6.0) },
            new Object[] {
                "scale=0, offset=-1",
                newBuilder().setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(-1).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(-1).addAllBucketCounts(List.of(1L, 1L))),
                List.of(1L, 1L, 1L, 1L, 1L),
                List.of(-1.5, -0.75, 0.0, 0.75, 1.5) },
            new Object[] {
                "scale=0, different offsets",
                newBuilder().setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(-1).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(1L, 1L))),
                List.of(1L, 1L, 1L, 1L, 1L),
                List.of(-6.0, -3.0, 0.0, 0.75, 1.5) },
            new Object[] {
                "scale=-1",
                newBuilder().setScale(-1)
                    .setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L))),
                List.of(1L, 1L, 1L, 1L, 1L),
                List.of(-10.0, -2.5, 0.0, 2.5, 10.0) },
            new Object[] {
                "scale=1",
                newBuilder().setScale(1)
                    .setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L))),
                List.of(1L, 1L, 1L, 1L, 1L),
                List.of(-1.7071067811865475, -1.2071067811865475, 0.0, 1.2071067811865475, 1.7071067811865475) }
        );
    }
}
