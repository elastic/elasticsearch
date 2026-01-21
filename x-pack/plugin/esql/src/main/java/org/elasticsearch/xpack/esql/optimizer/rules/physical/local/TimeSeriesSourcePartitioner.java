/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * Partitions time-series data source queries into multiple queries based on time intervals.
 * The ideal number of slices should be close to the number of available processors.
 * Too many slices may lead to high overhead; too few slices may cause under-utilization of CPUs
 * and require high memory to buffer data points for rate calculations.
 * Prefer partitioning the query interval into small, fixed, and predefined intervals to improve cache hit rate.
 */
final class TimeSeriesSourcePartitioner {

    static EsQueryExec partitionTimeSeriesSource(
        LocalPhysicalOptimizerContext ctx,
        Expression timestampField,
        LucenePushdownPredicates pushdownPredicates,
        EsQueryExec queryExec,
        List<Expression> pushable
    ) {
        // TODO: support date_nanos
        if (timestampField == null || timestampField.dataType() != DataType.DATETIME) {
            return null;
        }
        if (pushdownPredicates.isPushableFieldAttribute(timestampField) == false) {
            return null;
        }
        Object minTimestampFromData = ctx.searchStats().min(new FieldAttribute.FieldName(MetadataAttribute.TIMESTAMP_FIELD));
        Object maxTimestampFromData = ctx.searchStats().max(new FieldAttribute.FieldName(MetadataAttribute.TIMESTAMP_FIELD));
        if (minTimestampFromData instanceof Long minTs && maxTimestampFromData instanceof Long maxTs) {
            maxTs++; // maxTs from data is inclusive, make it exclusive
            long dataInterval = maxTs - minTs;
            Tuple<Long, Long> minMaxFromPredicates = minMaxTimestampFromQuery(timestampField, pushable);
            minTs = Math.max(minTs, minMaxFromPredicates.v1());
            maxTs = Math.min(maxTs, minMaxFromPredicates.v2());
            var filters = partitionFiltersByTimeIntervals(ctx, timestampField, dataInterval, minTs, maxTs);
            if (filters.isEmpty() || filters.size() == 1) {
                return null;
            }
            return pushDownFiltersAndPartitionFilters(
                pushdownPredicates,
                queryExec,
                removeTimestampFilters(timestampField, pushable),
                filters
            );
        }
        return null;
    }

    private static EsQueryExec pushDownFiltersAndPartitionFilters(
        LucenePushdownPredicates pushdownPredicates,
        EsQueryExec queryExec,
        List<Expression> pushable,
        List<Range> partitionFilters
    ) {
        List<EsQueryExec.QueryBuilderAndTags> queryAndTags = new ArrayList<>();
        QueryBuilder mainQuery = queryExec.query();
        for (Range partition : partitionFilters) {
            var newPushable = PushFiltersToSource.combineEligiblePushableToRange(CollectionUtils.appendToCopy(pushable, partition));
            Query queryDSL = TRANSLATOR_HANDLER.asQuery(pushdownPredicates, Predicates.combineAnd(newPushable));
            QueryBuilder planQuery = queryDSL.toQueryBuilder();
            QueryBuilder partitionQuery = Queries.combine(Queries.Clause.FILTER, asList(mainQuery, planQuery));
            queryAndTags.add(new EsQueryExec.QueryBuilderAndTags(partitionQuery, List.of()));
        }
        return new EsQueryExec(
            queryExec.source(),
            queryExec.indexPattern(),
            queryExec.indexMode(),
            queryExec.output(),
            queryExec.limit(),
            queryExec.sorts(),
            queryExec.estimatedRowSize(),
            queryAndTags
        );
    }

    private static List<Expression> removeTimestampFilters(Expression timestampField, List<Expression> pushable) {
        List<Expression> removed = new ArrayList<>(pushable.size());
        for (Expression expr : pushable) {
            if (expr instanceof EsqlBinaryComparison bin
                && bin.right().foldable()
                && bin.right() instanceof Literal l
                && l.value() instanceof Long
                && bin.left().dataType() == timestampField.dataType()
                && bin.left().semanticEquals(timestampField)) {
                continue;
            }
            if (expr instanceof Range r && r.value().dataType() == timestampField.dataType() && r.value().semanticEquals(timestampField)) {
                continue;
            }
            removed.add(expr);
        }
        return removed;
    }

    private static List<Range> partitionFiltersByTimeIntervals(
        LocalPhysicalOptimizerContext ctx,
        Expression timestampField,
        long dataInterval,
        long queryStartTs,
        long queryEndTs
    ) {
        long maxRateBufferBytes = ctx.plannerSettings().getRateBufferSize().getBytes();
        int taskConcurrency = ctx.configuration().pragmas().taskConcurrency();
        long queryInterval = queryEndTs - queryStartTs;
        long queryDocs = ctx.searchStats().count() * queryInterval / dataInterval;
        long selectedInterval = -1;
        long selectedSlices = -1;
        // TODO: retrieve this from planner settings instead
        final int availableProcessors = Math.ceilDiv(taskConcurrency * 2, 3);
        for (int i = TIME_SERIES_PARTITION_INTERVALS.length - 1; i >= 0; i--) {
            long sliceInterval = TIME_SERIES_PARTITION_INTERVALS[i];
            long numSlices = Math.ceilDiv(queryInterval, sliceInterval);
            long docPerSlice = queryDocs / numSlices;
            long requiredBytes = docPerSlice * 2 * Long.BYTES * Math.min(availableProcessors, numSlices);
            // The number of data points should be based on the number of docs with the counter field,
            // not just the timestamp field or total docs.
            // Assume the counter field appears in 50% of documents on average to avoid expensive over-partitioning.
            if (requiredBytes <= maxRateBufferBytes * 2.0) {
                if (selectedInterval == -1
                    || numSlices <= availableProcessors
                    || (selectedSlices < availableProcessors && numSlices <= taskConcurrency)) {
                    selectedInterval = sliceInterval;
                    selectedSlices = numSlices;
                } else {
                    break;
                }
            }
        }
        if (selectedInterval == -1) {
            return List.of();
        }
        List<Range> filters = new ArrayList<>();
        // round the intervals to improve caching
        var pt = (queryStartTs + selectedInterval - 1) / selectedInterval * selectedInterval;
        if (queryStartTs < pt) {
            filters.add(
                new Range(
                    timestampField.source(),
                    timestampField,
                    new Literal(timestampField.source(), queryStartTs, timestampField.dataType()),
                    true,
                    new Literal(timestampField.source(), pt, timestampField.dataType()),
                    false,
                    ctx.configuration().zoneId()
                )
            );
        }
        while (pt < queryEndTs) {
            long nextPt = Math.min(pt + selectedInterval, queryEndTs);
            filters.add(
                new Range(
                    timestampField.source(),
                    timestampField,
                    new Literal(timestampField.source(), pt, timestampField.dataType()),
                    true,
                    new Literal(timestampField.source(), nextPt, timestampField.dataType()),
                    false,
                    ctx.configuration().zoneId()
                )
            );
            pt = nextPt;
        }
        return filters;
    }

    private static Tuple<Long, Long> minMaxTimestampFromQuery(Expression timestampField, List<Expression> expressions) {
        long minTimestamp = Long.MIN_VALUE;
        long maxTimestamp = Long.MAX_VALUE;
        for (Expression expr : expressions) {
            if (expr instanceof EsqlBinaryComparison bin
                && bin.right().foldable()
                && bin.right() instanceof Literal l
                && l.value() instanceof Long v
                && bin.left().dataType() == timestampField.dataType()
                && bin.left().semanticEquals(timestampField)) {
                switch (expr) {
                    case Equals unused -> {
                        minTimestamp = Math.max(minTimestamp, v);
                        maxTimestamp = Math.min(maxTimestamp, v + 1);
                    }
                    case GreaterThan unused -> minTimestamp = Math.max(minTimestamp, v + 1);
                    case GreaterThanOrEqual unused -> minTimestamp = Math.max(minTimestamp, v);
                    case LessThan unused -> maxTimestamp = Math.min(maxTimestamp, v);
                    case LessThanOrEqual unused -> maxTimestamp = Math.min(maxTimestamp, v + 1);
                    default -> {
                    }
                }
            }
            if (expr instanceof Range r && r.value().dataType() == timestampField.dataType() && r.value().semanticEquals(timestampField)) {
                if (r.lower() instanceof Literal lowerLit && lowerLit.value() instanceof Long lowerVal) {
                    if (r.includeLower()) {
                        minTimestamp = Math.max(minTimestamp, lowerVal);
                    } else {
                        minTimestamp = Math.max(minTimestamp, lowerVal + 1);
                    }
                }
                if (r.upper() instanceof Literal upperLit && upperLit.value() instanceof Long upperVal) {
                    if (r.includeUpper()) {
                        maxTimestamp = Math.min(maxTimestamp, upperVal + 1);
                    } else {
                        maxTimestamp = Math.min(maxTimestamp, upperVal);
                    }
                }
            }
        }
        return Tuple.tuple(minTimestamp, maxTimestamp);
    }

    static final long[] TIME_SERIES_PARTITION_INTERVALS = new long[] {
        TimeValue.timeValueSeconds(1).millis(),
        TimeValue.timeValueSeconds(2).millis(),
        TimeValue.timeValueSeconds(5).millis(),
        TimeValue.timeValueSeconds(10).millis(),
        TimeValue.timeValueSeconds(20).millis(),
        TimeValue.timeValueSeconds(30).millis(),

        TimeValue.timeValueMinutes(1).millis(),
        TimeValue.timeValueMinutes(2).millis(),
        TimeValue.timeValueMinutes(3).millis(),
        TimeValue.timeValueMinutes(5).millis(),
        TimeValue.timeValueMinutes(10).millis(),
        TimeValue.timeValueMinutes(15).millis(),
        TimeValue.timeValueMinutes(20).millis(),
        TimeValue.timeValueMinutes(30).millis(),

        TimeValue.timeValueHours(1).millis(),
        TimeValue.timeValueHours(2).millis(),
        TimeValue.timeValueHours(3).millis(),
        TimeValue.timeValueHours(5).millis(),
        TimeValue.timeValueHours(12).millis(),

        TimeValue.timeValueDays(1).millis(),
        TimeValue.timeValueDays(7).millis() };
}
