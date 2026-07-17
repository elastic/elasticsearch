/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class RawHistogramConverterTests extends ESTestCase {

    public void testExplicitBoundsHistogram() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder()
            .addAllBucketCounts(List.of(1L, 3L, 0L, 2L))
            .addAllExplicitBounds(List.of(10.0, 20.0, 30.0))
            .build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(1L, 5L), actualCounts);
        assertEquals(List.of(10.0, 20.0), actualValues);
    }

    public void testExplicitBoundsHistogramPassesThroughBounds() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder()
            .addAllBucketCounts(List.of(1L, 3L, 5L, 0L))
            .addAllExplicitBounds(List.of(10.0, 20.0, 30.0))
            .build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(1L, 3L, 5L), actualCounts);
        assertEquals(List.of(10.0, 20.0, 30.0), actualValues);
    }

    public void testExplicitBoundsHistogramSingleBucket() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder().addAllBucketCounts(List.of(3L, 0L)).addExplicitBounds(5.0).build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(3L), actualCounts);
        assertEquals(List.of(5.0), actualValues);
    }

    public void testExplicitBoundsHistogramSkipsZeroCountBuckets() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder()
            .addAllBucketCounts(List.of(0L, 3L, 0L, 0L))
            .addAllExplicitBounds(List.of(10.0, 20.0, 30.0))
            .build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(3L), actualCounts);
        assertEquals(List.of(20.0), actualValues);
    }

    public void testExplicitBoundsHistogramMergesOverflowBucket() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder()
            .addAllBucketCounts(List.of(1L, 2L, 3L))
            .addAllExplicitBounds(List.of(10.0, 20.0))
            .build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(1L, 5L), actualCounts);
        assertEquals(List.of(10.0, 20.0), actualValues);
    }

    public void testExplicitBoundsHistogramIgnoresEmptyOverflowBucket() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder()
            .addAllBucketCounts(List.of(1L, 2L, 0L))
            .addAllExplicitBounds(List.of(10.0, 20.0))
            .build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(1L, 2L), actualCounts);
        assertEquals(List.of(10.0, 20.0), actualValues);
    }

    public void testExplicitBoundsHistogramWithoutExplicitBounds() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder().setCount(10L).setSum(100.0).build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(10L), actualCounts);
        assertEquals(List.of(10.0), actualValues);
    }

    public void testExplicitBoundsHistogramWithSingleNoBoundsBucket() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder().addBucketCounts(10L).setCount(10L).setSum(100.0).build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(10L), actualCounts);
        assertEquals(List.of(10.0), actualValues);
    }

    public void testExplicitBoundsHistogramWithoutExplicitBoundsAndZeroCount() throws Exception {
        HistogramDataPoint dataPoint = HistogramDataPoint.newBuilder().build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(), actualCounts);
        assertEquals(List.of(), actualValues);
    }

    public void testExponentialHistogram() throws Exception {
        ExponentialHistogramDataPoint dataPoint = ExponentialHistogramDataPoint.newBuilder()
            .setZeroCount(1)
            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
            .setNegative(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
            .build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(1L, 1L, 1L, 1L, 1L), actualCounts);
        assertEquals(List.of(-2.0, -1.0, 0.0, 2.0, 4.0), actualValues);
    }

    public void testExponentialHistogramEmpty() throws Exception {
        ExponentialHistogramDataPoint dataPoint = ExponentialHistogramDataPoint.newBuilder().build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(), actualCounts);
        assertEquals(List.of(), actualValues);
    }

    public void testExponentialHistogramZeroOnly() throws Exception {
        ExponentialHistogramDataPoint dataPoint = ExponentialHistogramDataPoint.newBuilder().setZeroCount(1).build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(1L), actualCounts);
        assertEquals(List.of(0.0), actualValues);
    }

    public void testExponentialHistogramWithOffset() throws Exception {
        ExponentialHistogramDataPoint dataPoint = ExponentialHistogramDataPoint.newBuilder()
            .setZeroCount(1)
            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(1L, 1L)))
            .setNegative(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(1L, 1L)))
            .build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(1L, 1L, 1L, 1L, 1L), actualCounts);
        assertEquals(5, actualValues.size());
        assertEquals(-4.0, actualValues.get(0), 1e-10);
        assertEquals(-2.0, actualValues.get(1), 1e-10);
        assertEquals(0.0, actualValues.get(2), 1e-10);
        assertEquals(4.0, actualValues.get(3), 1e-10);
        assertEquals(8.0, actualValues.get(4), 1e-10);
    }

    public void testExponentialHistogramSkipsZeroCountBuckets() throws Exception {
        ExponentialHistogramDataPoint dataPoint = ExponentialHistogramDataPoint.newBuilder()
            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(0L, 1L)))
            .build();

        List<Long> actualCounts = new ArrayList<>();
        RawHistogramConverter.counts(dataPoint, actualCounts::add);
        List<Double> actualValues = new ArrayList<>();
        RawHistogramConverter.values(dataPoint, actualValues::add);

        assertEquals(List.of(1L), actualCounts);
        assertEquals(List.of(4.0), actualValues);
    }
}
