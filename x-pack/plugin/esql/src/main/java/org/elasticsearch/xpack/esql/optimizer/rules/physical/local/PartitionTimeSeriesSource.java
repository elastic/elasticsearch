/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceDateTruncBucketWithRoundTo;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceRoundToWithQueryAndTags.buildCombinedQueryAndTags;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceRoundToWithQueryAndTags.createRangeExpression;

public class PartitionTimeSeriesSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    TimeSeriesAggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(TimeSeriesAggregateExec plan, LocalPhysicalOptimizerContext ctx) {
        if (plan.anyMatch(p -> p instanceof EsQueryExec q && q.indexMode() == IndexMode.TIME_SERIES)) {
            return partition(plan, ctx);
        } else {
            return plan;
        }
    }

    static Expression timestampField(TimeSeriesAggregateExec tsAggExec) {
        for (NamedExpression agg : tsAggExec.aggregates()) {
            if (Alias.unwrap(agg) instanceof TimestampAware timestampAware) {
                if (timestampAware.timestamp() != null) {
                    return timestampAware.timestamp();
                }
            }
        }
        return null;
    }

    PhysicalPlan partition(TimeSeriesAggregateExec plan, LocalPhysicalOptimizerContext ctx) {
        Expression timestampField = timestampField(plan);
        // TODO: support date_nanos
        if (timestampField == null || timestampField.dataType() != DataType.DATETIME) {
            return plan;
        }
        Object minFromData = ctx.searchStats().min(new FieldAttribute.FieldName(MetadataAttribute.TIMESTAMP_FIELD));
        Object maxFromData = ctx.searchStats().max(new FieldAttribute.FieldName(MetadataAttribute.TIMESTAMP_FIELD));
        if (minFromData instanceof Long minTs && maxFromData instanceof Long maxTs) {
            List<EsqlBinaryComparison> predicates = predicates(plan, timestampField);
            Tuple<Long, Long> minMaxFromPredicates = ReplaceDateTruncBucketWithRoundTo.minMaxFromPredicates(predicates);
            long dataInterval = maxTs - minTs;
            if (minMaxFromPredicates.v1() != null && minMaxFromPredicates.v1() > minTs) {
                minTs = minMaxFromPredicates.v1();
            }
            if (minMaxFromPredicates.v2() != null && minMaxFromPredicates.v2() < maxTs) {
                maxTs = minMaxFromPredicates.v2();
            }
            if (maxTs < Long.MAX_VALUE) {
                // make the maxTs inclusive
                maxTs += 1;
            }
            return partition(plan, ctx, timestampField, dataInterval, minTs, maxTs);
        }
        return plan;
    }

    PhysicalPlan partition(
        PhysicalPlan plan,
        LocalPhysicalOptimizerContext ctx,
        Expression timestampField,
        long dataInterval,
        long queryStart,
        long queryEnd
    ) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        if (pushdownPredicates.isPushableFieldAttribute(timestampField) == false) {
            return null;
        }
        DataType dataType = timestampField.dataType();
        long maxRateBufferBytes = ctx.plannerSettings().getRateBufferSize().getBytes();
        long taskConcurrency = ctx.configuration().pragmas().taskConcurrency();
        long queryInterval = queryEnd - queryStart - 1;
        long queryDocs = ctx.searchStats().count() * queryInterval / dataInterval;
        long selectedInterval = -1;
        long selectedSlices = -1;
        for (int i = TIME_SERIES_PARTITION_INTERVALS.length - 1; i >= 0; i--) {
            long interval = TIME_SERIES_PARTITION_INTERVALS[i];
            long numSlices = Math.ceilDiv(queryInterval, interval);
            long docPerSlice = queryDocs / numSlices;
            long requiredBytes = docPerSlice * 2 * Long.BYTES * Math.min(taskConcurrency, numSlices);
            if (requiredBytes <= maxRateBufferBytes * 1.1) {
                if (selectedInterval == -1
                    || numSlices <= taskConcurrency * 2 / 3
                    || (selectedSlices < taskConcurrency / 2 && numSlices <= taskConcurrency)) {
                    selectedInterval = interval;
                    selectedSlices = numSlices;
                } else {
                    break;
                }
            }
        }
        final long interval = selectedInterval;
        // TODO: round interval so that subsequent queries can use the same cache
        return plan.transformUp(EsQueryExec.class, queryExec -> {
            List<EsQueryExec.QueryBuilderAndTags> inputQueries = queryExec.queryBuilderAndTags();
            if (inputQueries == null || inputQueries.isEmpty()) {
                inputQueries = List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()));
            }
            if (inputQueries.size() > 1) {
                // already partitioned
                return queryExec;
            }
            List<EsQueryExec.QueryBuilderAndTags> newQueries = new ArrayList<>();
            Queries.Clause clause = queryExec.hasScoring() ? Queries.Clause.MUST : Queries.Clause.FILTER;
            ZoneId zoneId = ctx.configuration().zoneId();
            // TODO: we can adjust the filters on timestamp
            for (EsQueryExec.QueryBuilderAndTags qt : inputQueries) {
                var firstPt = Math.ceilDiv(queryStart, interval) * interval;
                if (queryStart < firstPt) {
                    var range = createRangeExpression(queryExec.source(), timestampField, dataType, queryStart, firstPt, zoneId);
                    newQueries.add(buildCombinedQueryAndTags(qt.query(), pushdownPredicates, range, clause, qt.tags()));
                }
                long pt = firstPt;
                while (pt < queryEnd) {
                    long nextPt = Math.min(pt + interval, queryEnd);
                    var range = createRangeExpression(queryExec.source(), timestampField, dataType, pt, nextPt, zoneId);
                    newQueries.add(buildCombinedQueryAndTags(qt.query(), pushdownPredicates, range, clause, qt.tags()));
                    pt = nextPt;
                }
            }
            if (newQueries.size() == 1) {
                return queryExec;
            }
            return queryExec.withQueryBuilderAndTags(newQueries);
        });
    }

    static List<EsqlBinaryComparison> predicates(PhysicalPlan plan, Expression field) {
        List<EsqlBinaryComparison> binaryComparisons = new ArrayList<>();
        plan.forEachUp(FilterExec.class, filter -> {
            Expression condition = filter.condition();
            if (condition instanceof And and) {
                Predicates.splitAnd(and).forEach(e -> addBinaryComparisonOnField(e, field, binaryComparisons));
            } else {
                addBinaryComparisonOnField(condition, field, binaryComparisons);
            }
        });
        return binaryComparisons;
    }

    static void addBinaryComparisonOnField(Expression expression, Expression field, List<EsqlBinaryComparison> binaryComparisons) {
        if (expression instanceof EsqlBinaryComparison esqlBinaryComparison
            && esqlBinaryComparison.right().foldable()
            && esqlBinaryComparison.left().semanticEquals(field)) {
            binaryComparisons.add(esqlBinaryComparison);
        }
    }

    static final long[] TIME_SERIES_PARTITION_INTERVALS = new long[] {
        TimeValue.timeValueSeconds(1).millis(),
        TimeValue.timeValueSeconds(5).millis(),
        TimeValue.timeValueSeconds(10).millis(),
        TimeValue.timeValueSeconds(20).millis(),
        TimeValue.timeValueSeconds(30).millis(),

        TimeValue.timeValueMinutes(1).millis(),
        TimeValue.timeValueMinutes(2).millis(),
        TimeValue.timeValueMinutes(5).millis(),
        TimeValue.timeValueMinutes(10).millis(),
        TimeValue.timeValueMinutes(15).millis(),
        TimeValue.timeValueMinutes(20).millis(),
        TimeValue.timeValueMinutes(30).millis(),

        TimeValue.timeValueHours(1).millis(),
        TimeValue.timeValueHours(2).millis(),
        TimeValue.timeValueHours(5).millis(),
        TimeValue.timeValueHours(12).millis(),

        TimeValue.timeValueDays(1).millis(),
        TimeValue.timeValueDays(7).millis() };
}
