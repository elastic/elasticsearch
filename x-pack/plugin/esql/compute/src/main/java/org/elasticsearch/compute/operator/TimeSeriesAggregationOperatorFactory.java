/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.core.TimeValue;

import java.util.List;

public record TimeSeriesAggregationOperatorFactory(
    AggregatorMode mode,
    int tsHashChannel,
    int timestampIntervalChannel,
    TimeValue timeSeriesPeriod,
    List<GroupingAggregator.Factory> aggregators,
    int maxPageSize
) implements Operator.OperatorFactory {

    @Override
    public String describe() {
        return "TimeSeriesAggregationOperator[mode="
            + mode
            + ", tsHashChannel = "
            + tsHashChannel
            + ", timestampIntervalChannel = "
            + timestampIntervalChannel
            + ", timeSeriesPeriod = "
            + timeSeriesPeriod
            + ", maxPageSize = "
            + maxPageSize
            + "]";
    }

    @Override
    public Operator get(DriverContext driverContext) {
        BlockHash blockHash = new TimeSeriesBlockHash(tsHashChannel, timestampIntervalChannel, driverContext);
        return new HashAggregationOperator(aggregators, () -> blockHash, driverContext);
    }

}
