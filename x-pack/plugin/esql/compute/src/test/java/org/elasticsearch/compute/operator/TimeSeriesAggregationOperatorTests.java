/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesBooleanGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesBytesRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesIntGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesLongGroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;
import java.util.function.BiFunction;

public class TimeSeriesAggregationOperatorTests extends ComputeTestCase {

    public void testValuesAggregator() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, "test");
        List<BiFunction<List<Integer>, DriverContext, GroupingAggregatorFunction>> functions = List.of(
            ValuesBooleanGroupingAggregatorFunction::create,
            ValuesIntGroupingAggregatorFunction::create,
            ValuesLongGroupingAggregatorFunction::create,
            ValuesBytesRefGroupingAggregatorFunction::create,
            DimensionValuesByteRefGroupingAggregatorFunction::new
        );
        for (var fn : functions) {
            try (GroupingAggregatorFunction aggregator = fn.apply(List.of(randomNonNegativeInt()), driverContext)) {
                assertTrue(TimeSeriesAggregationOperator.isValuesAggregator(aggregator));
            }
        }
    }
}
