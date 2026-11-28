/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.querydsl.query.TsidPrefixRangeQueryBuilder;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.timeseries.TsidPrefixHistogram;
import org.elasticsearch.xpack.esql.timeseries.TsidPrefixHistogramCollector;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Splits a time-series data-node plan into branches so each holds one {@code _tsid} range.
 */
public final class TimeSeriesSplitPlanner {
    private final TransportService transportService;

    // TODO: accurate estimate
    static final long ESTIMATED_BYTES_PER_GROUP = 135;
    private static final long NO_BUCKET = 0;

    public TimeSeriesSplitPlanner(TransportService transportService) {
        this.transportService = transportService;
    }

    /** Builds the execution subplans after probing for time-series splits when needed. */
    public void buildSubPlan(
        CancellableTask queryTask,
        PhysicalPlan plan,
        TransportVersion minimumVersion,
        Configuration configuration,
        EsqlFlags flags,
        ActionListener<SubPlan> subPlanListener
    ) {
        final ActionListener<PhysicalPlan> listener = subPlanListener.map(PlannerUtils::buildSubPlan);
        if (minimumVersion.supports(TsidPrefixRangeQueryBuilder.TSID_PREFIX_RANGE_QUERY) == false) {
            listener.onResponse(plan);
            return;
        }
        final QueryPragmas pragmas = configuration.pragmas();
        final int maxConcurrentBranches = pragmas.branchParallelDegree();
        final int maxSplits = Math.min(
            pragmas.timeSeriesMaxSplits(),
            Math.min(pragmas.maxBranchCount(flags.maxBranchCount()), pragmas.maxBranchCountPerMerge(flags.maxBranchCountPerMerge()))
        );
        if (canReducePeakMemory(maxSplits, maxConcurrentBranches) == false) {
            listener.onResponse(plan);
            return;
        }
        Nodes nodes = findNodes(plan);
        if (nodes == null) {
            listener.onResponse(plan);
            return;
        }
        final EsRelation relation = nodes.relation();
        final Bucket timeBucket = nodes.partialAgg().timeBucket();
        final long now = configuration.now().toEpochMilli();
        final long[] timeBounds = new long[] { Long.MIN_VALUE, Long.MAX_VALUE };
        final QueryBuilder queryFilter = PlannerUtils.canMatchFilter(flags, configuration, minimumVersion, nodes.fragment());
        if (queryFilter != null) {
            collectTimestampBounds(queryFilter, now, timeBounds);
        }
        List<String> metricFields = metricFields(nodes.partialAgg());
        if (metricFields.isEmpty()) {
            listener.onResponse(plan);
            return;
        }
        final long estimatedGroups = estimateGroupFromIndexStats(relation.indexProperties().values(), timeBucket, timeBounds);
        final long bytesPerSplit = pragmas.timeSeriesSplitTargetMemory().getBytes();
        if (estimatedGroups <= 0
            || canReducePeakMemory(numSplits(estimatedGroups, bytesPerSplit, maxSplits), maxConcurrentBranches) == false) {
            listener.onResponse(plan);
            return;
        }
        final long bucketMillis;
        if (timeBucket == null) {
            bucketMillis = NO_BUCKET;
        } else {
            Rounding.Prepared rounding = timeBucket.getDateRounding(FoldContext.small(), null, null);
            long bucketStart = rounding.round(timeBounds[0] == Long.MIN_VALUE ? now : timeBounds[0]);
            bucketMillis = rounding.nextRoundingValue(bucketStart) - bucketStart;
        }
        Map<String, String[]> indices = new HashMap<>();
        Map<String, OriginalIndices> originalIndices = new HashMap<>();
        for (Map.Entry<String, List<String>> cluster : relation.concreteIndices().entrySet()) {
            indices.put(cluster.getKey(), cluster.getValue().toArray(String[]::new));
            originalIndices.put(
                cluster.getKey(),
                new OriginalIndices(
                    relation.originalIndices().getOrDefault(cluster.getKey(), List.of()).toArray(String[]::new),
                    SearchRequest.DEFAULT_INDICES_OPTIONS
                )
            );
        }
        TsidPrefixHistogramCollector.collect(
            transportService,
            queryTask,
            indices,
            originalIndices,
            metricFields,
            timeBounds[0],
            timeBounds[1],
            bucketMillis,
            listener.map(resp -> {
                queryTask.ensureNotCancelled();
                // TODO: shard or cluster failures leave a partial histogram, which undercounts the groups
                TsidPrefixHistogram histogram = resp.histogram();
                long groups = histogram.estimatedGroups(bucketMillis);
                int numSplits = numSplits(groups, bytesPerSplit, maxSplits);
                if (canReducePeakMemory(numSplits, maxConcurrentBranches) == false) {
                    return plan;
                }
                List<TsidPrefixRange> ranges = histogram.splits(numSplits);
                if (canReducePeakMemory(ranges.size(), maxConcurrentBranches) == false) {
                    return plan;
                }
                return split(plan, nodes, ranges);
            })
        );
    }

    /**
     * If all the splits are running concurrently, then it might not reduce the peak the memory
     */
    private static boolean canReducePeakMemory(int numSplits, int maxConcurrentBranches) {
        return numSplits > maxConcurrentBranches;
    }

    static int numSplits(long groups, long memoryPerSplit, int maxSplits) {
        long bytes = groups > Long.MAX_VALUE / ESTIMATED_BYTES_PER_GROUP ? Long.MAX_VALUE : groups * ESTIMATED_BYTES_PER_GROUP;
        return (int) Math.min(maxSplits, Math.ceilDiv(bytes, memoryPerSplit));
    }

    /**
     * Estimates groups from field caps {@link TimeSeriesStats}
     */
    static long estimateGroupFromIndexStats(Collection<IndexProperties> indices, @Nullable Bucket timeBucket, long[] bounds) {
        long groups = 0;
        for (IndexProperties properties : indices) {
            TimeSeriesStats stats = properties.timeSeriesStats();
            if (stats == null) {
                return -1;
            }
            long minTimestamp = Math.max(stats.startMillis(), bounds[0]);
            long maxTimestamp = Math.min(stats.endMillis(), bounds[1]);
            if (minTimestamp > maxTimestamp) {
                continue;
            }
            long coveredMillis = maxTimestamp - minTimestamp + 1;
            double covered = (double) coveredMillis / (stats.endMillis() - stats.startMillis() + 1);
            long seriesMillis = (long) Math.ceil(stats.seriesMillis() * covered);
            if (seriesMillis == 0) {
                continue;
            }
            // Add one bucket per series for boundary alignment when grouping by time.
            groups += Math.ceilDiv(seriesMillis, coveredMillis);
            if (timeBucket != null) {
                Rounding.Prepared rounding = timeBucket.getDateRounding(FoldContext.small(), minTimestamp, maxTimestamp);
                long bucketStart = rounding.round(minTimestamp);
                long bucketMillis = rounding.nextRoundingValue(bucketStart) - bucketStart;
                if (bucketMillis != NO_BUCKET) {
                    groups += Math.ceilDiv(seriesMillis, bucketMillis);
                }
            }
        }
        return groups;
    }

    static PhysicalPlan split(PhysicalPlan plan, Nodes nodes, List<TsidPrefixRange> ranges) {
        TimeSeriesAggregateExec finalAgg = nodes.finalAgg();
        ExchangeExec exchange = nodes.exchange();
        FragmentExec fragment = nodes.fragment();
        List<PhysicalPlan> branches = new ArrayList<>(ranges.size());
        for (TsidPrefixRange range : ranges) {
            QueryBuilder filter = new TsidPrefixRangeQueryBuilder(range);
            if (fragment.esFilter() != null) {
                filter = new BoolQueryBuilder().filter(filter).must(fragment.esFilter());
            }
            PhysicalPlan branch = new FragmentExec(
                fragment.source(),
                fragment.fragment(),
                filter,
                fragment.estimatedRowSize(),
                fragment.isFromViewBranch()
            ).withOrigin(fragment.planId());

            PhysicalPlan branchExchange = new ExchangeExec(exchange.source(), exchange.output(), exchange.inBetweenAggs(), branch)
                .withOrigin(exchange.planId());
            branches.add(finalAgg.replaceChild(branchExchange).withOrigin(finalAgg.planId()));
        }
        MergeExec merge = new MergeExec(finalAgg.source(), branches, finalAgg.output(), MergeExec.Kind.FORK);
        return plan.transformUp(PhysicalPlan.class, p -> p == finalAgg ? merge : p);
    }

    record Nodes(
        TimeSeriesAggregateExec finalAgg,
        ExchangeExec exchange,
        FragmentExec fragment,
        TimeSeriesAggregate partialAgg,
        EsRelation relation
    ) {}

    static Nodes findNodes(PhysicalPlan plan) {
        List<TimeSeriesAggregateExec> reductions = plan.collect(TimeSeriesAggregateExec.class);
        if (reductions.size() != 1 || plan.anyMatch(p -> p instanceof MergeExec)) {
            return null;
        }
        TimeSeriesAggregateExec finalAgg = reductions.getFirst();
        if (finalAgg.getMode() == AggregatorMode.FINAL
            && finalAgg.child() instanceof ExchangeExec exchange
            && exchange.child() instanceof FragmentExec fragment
            && fragment.fragment() instanceof TimeSeriesAggregate partial
            && partial.aggregates().stream().anyMatch(TimeSeriesSplitPlanner::requiresTimeSeriesSource)) {
            List<EsRelation> relations = partial.collect(EsRelation.class);
            if (relations.size() != 1) {
                return null;
            }
            EsRelation relation = relations.getFirst();
            if (relation.indexProperties().isEmpty()
                || relation.indexProperties().values().stream().allMatch(TimeSeriesSplitPlanner::hasTsidPrefixes) == false) {
                return null;
            }
            return new Nodes(finalAgg, exchange, fragment, partial, relation);
        }
        return null;
    }

    private static boolean hasTsidPrefixes(IndexProperties properties) {
        int createdVersion = properties.createdVersion();
        return properties.indexMode() == IndexMode.TIME_SERIES
            && createdVersion != 0
            && PartitionedDocValues.canPartitionByTsidPrefix(IndexVersion.fromId(createdVersion));
    }

    private static boolean requiresTimeSeriesSource(Expression aggregate) {
        return aggregate.anyMatch(e -> e instanceof TimeSeriesAggregateFunction ts && ts.requiredTimeSeriesSource());
    }

    private static List<String> metricFields(TimeSeriesAggregate aggregate) {
        Expression condition = IgnoreNullMetrics.metricsFilter(aggregate);
        if (condition == null) {
            return List.of();
        }
        List<String> fields = new ArrayList<>();
        for (IsNotNull isNotNull : condition.collect(IsNotNull.class)) {
            if (isNotNull.field() instanceof FieldAttribute field) {
                fields.add(field.fieldName().string());
            } else {
                return List.of();
            }
        }
        return fields;
    }

    private static void collectTimestampBounds(QueryBuilder filter, long nowInMillis, long[] bounds) {
        if (filter instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.filter()) {
                collectTimestampBounds(clause, nowInMillis, bounds);
            }
            for (QueryBuilder clause : bool.must()) {
                collectTimestampBounds(clause, nowInMillis, bounds);
            }
        } else if (filter instanceof SingleValueQuery.AbstractBuilder singleValue) {
            collectTimestampBounds(singleValue.next(), nowInMillis, bounds);
        } else if (filter instanceof RangeQueryBuilder range && range.fieldName().equals(DataStream.TIMESTAMP_FIELD_NAME)) {
            try {
                DateFormatter formatter = range.format() == null
                    ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER
                    : DateFormatter.forPattern(range.format());
                DateMathParser parser = formatter.toDateMathParser();
                ZoneId zone = range.timeZone() == null ? null : ZoneId.of(range.timeZone());
                if (range.from() != null) {
                    long lower = parser.parse(range.from().toString(), () -> nowInMillis, range.includeLower() == false, zone)
                        .toEpochMilli();
                    bounds[0] = Math.max(bounds[0], lower);
                }
                if (range.to() != null) {
                    long upper = parser.parse(range.to().toString(), () -> nowInMillis, range.includeUpper(), zone).toEpochMilli();
                    bounds[1] = Math.min(bounds[1], upper);
                }
            } catch (RuntimeException e) {
                // a bound that cannot be parsed here only loses the clipping; the shards still apply the filter
            }
        }
    }
}
