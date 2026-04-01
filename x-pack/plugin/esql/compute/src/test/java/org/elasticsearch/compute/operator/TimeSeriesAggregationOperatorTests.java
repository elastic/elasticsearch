/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Instant;
import java.util.List;
import java.util.function.BiFunction;

public class TimeSeriesAggregationOperatorTests extends ComputeTestCase {

    public void testValuesAggregator() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null, "test");
        List<BiFunction<List<Integer>, DriverContext, GroupingAggregatorFunction>> functions = List.of(
            (channels, ctx) -> new ValuesBooleanAggregatorFunctionSupplier().groupingAggregator(ctx, channels),
            (channels, ctx) -> new ValuesIntAggregatorFunctionSupplier().groupingAggregator(ctx, channels),
            (channels, ctx) -> new ValuesLongAggregatorFunctionSupplier().groupingAggregator(ctx, channels),
            (channels, ctx) -> new ValuesBytesRefAggregatorFunctionSupplier().groupingAggregator(ctx, channels),
            DimensionValuesByteRefGroupingAggregatorFunction::new
        );
        for (var fn : functions) {
            try (GroupingAggregatorFunction aggregator = fn.apply(List.of(randomNonNegativeInt()), driverContext)) {
                assertTrue(TimeSeriesAggregationOperator.isValuesAggregator(aggregator));
            }
        }
    }

    public void testRangeForStartAlignedBucketKey() {
        Rounding.Prepared timeBucket = Rounding.builder(TimeValue.timeValueMinutes(10)).build().prepareForUnknown();
        long bucketTs = Instant.parse("2024-01-01T12:10:00Z").toEpochMilli();

        long roundedBucketTsMillis = TimeSeriesAggregationOperator.roundedBucketKeyInMillis(
            bucketTs,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        assertEquals(bucketTs, roundedBucketTsMillis);
        long roundedBucketTsMillis1 = TimeSeriesAggregationOperator.roundedBucketKeyInMillis(
            bucketTs,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        assertEquals(
            Instant.parse("2024-01-01T12:20:00Z").toEpochMilli(),
            TimeSeriesAggregationOperator.nextBucketKey(roundedBucketTsMillis1, timeBucket)
        );
    }

    public void testRangeForEndAlignedBucketKey() {
        Rounding.Prepared timeBucket = Rounding.builder(TimeValue.timeValueMinutes(10)).build().prepareForUnknown();
        long bucketTs = Instant.parse("2024-01-01T12:10:00Z").toEpochMilli();

        long roundedBucketTsMillis = TimeSeriesAggregationOperator.roundedBucketKeyInMillis(
            bucketTs,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        assertEquals(
            Instant.parse("2024-01-01T12:00:00Z").toEpochMilli(),
            TimeSeriesAggregationOperator.previousBucketKey(roundedBucketTsMillis, timeBucket)
        );
        long roundedBucketTsMillis1 = TimeSeriesAggregationOperator.roundedBucketKeyInMillis(
            bucketTs,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        assertEquals(bucketTs, roundedBucketTsMillis1);
    }
}
