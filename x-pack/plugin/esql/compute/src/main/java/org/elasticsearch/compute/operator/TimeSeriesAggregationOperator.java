/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.TimeSeriesGroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.BytesRefLongBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

/**
 * A specialized version of {@link HashAggregationOperator} that aggregates time-series aggregations from time-series sources.
 */
public class TimeSeriesAggregationOperator extends HashAggregationOperator {

    public record Factory(
        Rounding.Prepared timeBucket,
        boolean dateNanos,
        List<BlockHash.GroupSpec> groups,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            // TODO: use TimeSeriesBlockHash when possible
            return new TimeSeriesAggregationOperator(
                timeBucket,
                dateNanos ? DateFieldMapper.Resolution.NANOSECONDS : DateFieldMapper.Resolution.MILLISECONDS,
                aggregators,
                () -> BlockHash.build(
                    groups,
                    driverContext.blockFactory(),
                    maxPageSize,
                    true // we can enable optimizations as the inputs are vectors
                ),
                driverContext
            );
        }

        @Override
        public String describe() {
            return "TimeSeriesAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    private final Rounding.Prepared timeBucket;
    private final DateFieldMapper.Resolution timeResolution;

    public TimeSeriesAggregationOperator(
        Rounding.Prepared timeBucket,
        DateFieldMapper.Resolution timeResolution,
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        super(aggregators, blockHash, driverContext);
        this.timeBucket = timeBucket;
        this.timeResolution = timeResolution;
    }

    @Override
    protected GroupingAggregatorEvaluationContext evaluationContext(BlockHash blockHash, Block[] keys) {
        if (keys.length < 2) {
            return super.evaluationContext(blockHash, keys);
        }
        final BytesRefLongBlockHash hash = (BytesRefLongBlockHash) blockHash;

        final LongBlock timestamps = keys[0].elementType() == ElementType.LONG ? (LongBlock) keys[0] : (LongBlock) keys[1];
        // block hash so that we can look key
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            @Override
            public long rangeStartInMillis(int groupId) {
                return timeResolution.roundDownToMillis(timestamps.getLong(groupId));
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return timeResolution.roundDownToMillis(timeBucket.nextRoundingValue(timestamps.getLong(groupId)));
            }

            @Override
            public List<Integer> groupIdsFromWindow(int startingGroupId, Duration window) {
                long ordinal = hash.getBytesRefKeyFromGroup(startingGroupId);
                long startTimestamp = hash.getLongKeyFromGroup(startingGroupId);
                List<Integer> results = new ArrayList<>();
                results.add(startingGroupId);
                long endTimestamp = startTimestamp + timeResolution.convert(window.toMillis());
                long nextTimestamp = startTimestamp;
                while ((nextTimestamp = timeBucket.nextRoundingValue(nextTimestamp)) < endTimestamp) {
                    long nextGroupId = hash.getGroupId(ordinal, nextTimestamp);
                    if (nextGroupId != -1) {
                        results.add(Math.toIntExact(nextGroupId));
                    }
                }
                assert nextTimestamp == endTimestamp : "expected to end at the original timestamp bucket";
                return results;
            }
        };
    }
}
