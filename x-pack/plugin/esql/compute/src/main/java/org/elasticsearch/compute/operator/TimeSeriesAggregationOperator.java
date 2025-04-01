/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;

import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

/**
 * A specialized version of {@link HashAggregationOperator} that aggregates time-series aggregations from time-series sources.
 */
public class TimeSeriesAggregationOperator extends HashAggregationOperator {

    public record Factory(
        BlockHash.GroupSpec tsidGroup,
        BlockHash.GroupSpec timestampGroup,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            // TODO: use TimeSeriesBlockHash when possible
            return new HashAggregationOperator(
                aggregators,
                () -> BlockHash.build(
                    List.of(tsidGroup, timestampGroup),
                    driverContext.blockFactory(),
                    maxPageSize,
                    true // we can enable optimizations as the inputs are vectors
                ),
                driverContext
            );
        }

        @Override
        public String describe() {
            return "MetricsAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    public TimeSeriesAggregationOperator(
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        super(aggregators, blockHash, driverContext);
    }
}
