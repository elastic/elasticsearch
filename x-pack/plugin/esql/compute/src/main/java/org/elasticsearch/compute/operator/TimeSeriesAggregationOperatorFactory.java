/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.core.TimeValue;

import java.util.List;

public record TimeSeriesAggregationOperatorFactory(
    AggregatorMode mode,
    int tsHashChannel,
    int timestampChannel,
    TimeValue timeSeriesPeriod,
    List<GroupingAggregator.Factory> aggregators,
    int maxPageSize
) implements Operator.OperatorFactory {

    @Override
    public String describe() {
        return null;
    }

    @Override
    public Operator get(DriverContext driverContext) {
        var rounding = timeSeriesPeriod.equals(TimeValue.ZERO) == false ? Rounding.builder(timeSeriesPeriod).build() : null;
        BlockHash blockHash = new TimeSeriesBlockHash(tsHashChannel, timestampChannel, rounding, driverContext);
        return new HashAggregationOperator(aggregators, () -> blockHash, driverContext);
    }

}
