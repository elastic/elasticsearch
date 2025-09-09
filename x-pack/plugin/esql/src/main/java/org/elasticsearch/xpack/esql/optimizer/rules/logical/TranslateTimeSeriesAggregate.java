/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Time-series aggregation is special because it must be computed per time series, regardless of the grouping keys.
 * The keys must be `_tsid` or a pair of `_tsid` and `time_bucket`. To support user-defined grouping keys,
 * we first execute the rate aggregation using the time-series keys, then perform another aggregation with
 * the resulting rate using the user-specific keys.
 * <p>
 * This class translates the aggregates in the time-series aggregations to standard aggregates.
 * This approach helps avoid introducing new plans and operators for time-series aggregations specially.
 * <p>
 * Examples:
 * <pre>
 * TS k8s | STATS max(rate(request))
 *
 * becomes
 *
 * TS k8s
 * | STATS rate_$1 = rate(request) BY _tsid
 * | STATS max(rate_$1)
 *
 * TS k8s | STATS max(rate(request)) BY host
 *
 * becomes
 *
 * TS k8s
 * | STATS rate_$1=rate(request), VALUES(host) BY _tsid
 * | STATS max(rate_$1) BY host=`VALUES(host)`
 *
 * TS k8s | STATS avg(rate(request)) BY host
 *
 * becomes
 *
 * TS k8s
 * | STATS rate_$1=rate(request), VALUES(host) BY _tsid
 * | STATS sum(rate_$1), count(rate_$1) BY host=`VALUES(host)`
 * | EVAL `avg(rate(request))` = `sum(rate_$1)` / `count(rate_$1)`
 * | KEEP `avg(rate(request))`, host
 *
 * TS k8s | STATS avg(rate(request)) BY host, bucket(@timestamp, 1minute)
 *
 * becomes
 *
 * TS k8s
 * | EVAL  `bucket(@timestamp, 1minute)`=datetrunc(@timestamp, 1minute)
 * | STATS rate_$1=rate(request), VALUES(host) BY _tsid,`bucket(@timestamp, 1minute)`
 * | STATS sum=sum(rate_$1), count(rate_$1) BY host=`VALUES(host)`, `bucket(@timestamp, 1minute)`
 * | EVAL `avg(rate(request))` = `sum(rate_$1)` / `count(rate_$1)`
 * | KEEP `avg(rate(request))`, host, `bucket(@timestamp, 1minute)`
 * </pre>
 *
 * Non time-series aggregates will be rewritten with last_over_time used in the first pass aggregation.
 * Here, we don't have the staleness interval, but allow any value within the bucket (_tsid and optionally time-bucket).
 *
 * <pre>
 * TS k8s | STATS max(rate(request)), max(memory_used) becomes:
 *
 * TS k8s
 * | STATS rate_$1=rate(request), $last_m1=last_over_time(memory_used) BY _tsid
 * | STATS max(rate_$1), `max(memory_used)` = max($last_m1)
 *
 * TS k8s | STATS max(rate(request)) avg(memory_used) BY host
 *
 * becomes
 *
 * TS k8s
 * | STATS rate_$1=rate(request), $p1=last_over_time(memory_used), VALUES(host) BY _tsid
 * | STATS max(rate_$1), $sum=sum($p1), $count=count($p1) BY host=`VALUES(host)`
 * | EVAL `avg(memory_used)` = $sum / $count
 * | KEEP `max(rate(request))`, `avg(memory_used)`, host
 *
 * TS k8s | STATS min(memory_used) sum(rate(request)) BY pod, bucket(@timestamp, 5m)
 *
 * becomes
 *
 * TS k8s
 * | EVAL `bucket(@timestamp, 5m)` = datetrunc(@timestamp, '5m')
 * | STATS rate_$1=rate(request), $p1=last_over_time(memory_used)), VALUES(pod) BY _tsid, `bucket(@timestamp, 5m)`
 * | STATS sum(rate_$1), `min(memory_used)` = min($p1) BY pod=`VALUES(pod)`, `bucket(@timestamp, 5m)`
 * | KEEP `min(memory_used)`, `sum(rate_$1)`, pod, `bucket(@timestamp, 5m)`
 *
 * {agg}_over_time time-series aggregation will be rewritten in the similar way
 *
 * TS k8s | STATS sum(max_over_time(memory_usage)) BY host, bucket(@timestamp, 1minute)
 *
 * becomes
 *
 * FROM k8s
 * | STATS max_over_time_$1 = max(memory_usage), host_values=VALUES(host) BY _tsid, time_bucket=bucket(@timestamp, 1minute)
 * | STATS sum(max_over_time_$1) BY host_values, time_bucket
 *
 *
 * TS k8s | STATS sum(avg_over_time(memory_usage)) BY host, bucket(@timestamp, 1minute)
 *
 * becomes
 *
 * FROM k8s
 * | STATS avg_over_time_$1 = avg(memory_usage), host_values=VALUES(host) BY _tsid, time_bucket=bucket(@timestamp, 1minute)
 * | STATS sum(avg_over_time_$1) BY host_values, time_bucket
 *
 * TS k8s | STATS max(rate(post_requests) + rate(get_requests)) BY host, bucket(@timestamp, 1minute)
 *
 * becomes
 *
 * FROM k8s
 * | STATS rate_$1=rate(post_requests), rate_$2=rate(post_requests) BY _tsid, time_bucket=bucket(@timestamp, 1minute)
 * | STATS max(rate_$1 + rate_$2) BY host_values, time_bucket
 * </pre>
 */
public final class TranslateTimeSeriesAggregate extends OptimizerRules.OptimizerRule<Aggregate> {

    public TranslateTimeSeriesAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (aggregate instanceof TimeSeriesAggregate ts && ts.timeBucket() == null) {
            return translate(ts);
        } else {
            return aggregate;
        }
    }

    LogicalPlan translate(TimeSeriesAggregate aggregate) {
        Holder<Attribute> tsid = new Holder<>();
        Holder<Attribute> timestamp = new Holder<>();
        aggregate.forEachDown(EsRelation.class, r -> {
            for (Attribute attr : r.output()) {
                if (attr.name().equals(MetadataAttribute.TSID_FIELD)) {
                    tsid.set(attr);
                }
                if (attr.name().equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                    timestamp.set(attr);
                }
            }
        });
        if (tsid.get() == null) {
            tsid.set(new MetadataAttribute(aggregate.source(), MetadataAttribute.TSID_FIELD, DataType.KEYWORD, false));
        }
        if (timestamp.get() == null) {
            throw new IllegalArgumentException("_tsid or @timestamp field are missing from the time-series source");
        }
        Map<AggregateFunction, Alias> timeSeriesAggs = new HashMap<>();
        List<NamedExpression> firstPassAggs = new ArrayList<>();
        List<NamedExpression> secondPassAggs = new ArrayList<>();
        Holder<Boolean> hasRateAggregates = new Holder<>(Boolean.FALSE);
        var internalNames = new InternalNames();
        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                Holder<Boolean> changed = new Holder<>(Boolean.FALSE);
                Expression outerAgg = af.transformDown(TimeSeriesAggregateFunction.class, tsAgg -> {
                    changed.set(Boolean.TRUE);
                    if (tsAgg instanceof Rate) {
                        hasRateAggregates.set(Boolean.TRUE);
                    }
                    AggregateFunction firstStageFn = tsAgg.perTimeSeriesAggregation();
                    Alias newAgg = timeSeriesAggs.computeIfAbsent(firstStageFn, k -> {
                        Alias firstStageAlias = new Alias(tsAgg.source(), internalNames.next(tsAgg.functionName()), firstStageFn);
                        firstPassAggs.add(firstStageAlias);
                        return firstStageAlias;
                    });
                    return newAgg.toAttribute();
                });
                if (changed.get()) {
                    secondPassAggs.add(new Alias(alias.source(), alias.name(), outerAgg, agg.id()));
                } else {
                    // TODO: reject over_time_aggregation only
                    var tsAgg = new LastOverTime(af.source(), af.field(), timestamp.get());
                    AggregateFunction firstStageFn = tsAgg.perTimeSeriesAggregation();
                    Alias newAgg = timeSeriesAggs.computeIfAbsent(firstStageFn, k -> {
                        Alias firstStageAlias = new Alias(tsAgg.source(), internalNames.next(tsAgg.functionName()), firstStageFn);
                        firstPassAggs.add(firstStageAlias);
                        return firstStageAlias;
                    });
                    secondPassAggs.add((Alias) agg.transformUp(f -> f == af.field(), f -> newAgg.toAttribute()));
                }
            }
        }
        // time-series aggregates must be grouped by _tsid (and time-bucket) first and re-group by users key
        List<Expression> firstPassGroupings = new ArrayList<>();
        firstPassGroupings.add(tsid.get());
        List<Expression> secondPassGroupings = new ArrayList<>();
        Holder<NamedExpression> timeBucketRef = new Holder<>();
        aggregate.child().forEachExpressionUp(NamedExpression.class, e -> {
            for (Expression child : e.children()) {
                if (child instanceof Bucket bucket && bucket.field().equals(timestamp.get())) {
                    if (timeBucketRef.get() != null) {
                        throw new IllegalArgumentException("expected at most one time bucket");
                    }
                    timeBucketRef.set(e);
                } else if (child instanceof TBucket tbucket && tbucket.field().equals(timestamp.get())) {
                    if (timeBucketRef.get() != null) {
                        throw new IllegalArgumentException("expected at most one time tbucket");
                    }
                    Bucket bucket = (Bucket) tbucket.surrogate();
                    timeBucketRef.set(new Alias(e.source(), bucket.functionName(), bucket, e.id()));
                }
            }
        });
        NamedExpression timeBucket = timeBucketRef.get();
        for (Expression group : aggregate.groupings()) {
            if (group instanceof Attribute == false) {
                throw new EsqlIllegalArgumentException("expected named expression for grouping; got " + group);
            }
            final Attribute g = (Attribute) group;
            final NamedExpression newFinalGroup;
            if (timeBucket != null && g.id().equals(timeBucket.id())) {
                newFinalGroup = timeBucket.toAttribute();
                firstPassGroupings.add(newFinalGroup);
            } else {
                newFinalGroup = new Alias(g.source(), g.name(), new Values(g.source(), g), g.id());
                firstPassAggs.add(newFinalGroup);
            }
            secondPassGroupings.add(new Alias(g.source(), g.name(), newFinalGroup.toAttribute(), g.id()));
        }
        LogicalPlan newChild = aggregate.child().transformUp(EsRelation.class, r -> {
            IndexMode indexMode = hasRateAggregates.get() ? r.indexMode() : IndexMode.STANDARD;
            if (r.output().contains(tsid.get()) == false) {
                return new EsRelation(
                    r.source(),
                    r.indexPattern(),
                    indexMode,
                    r.indexNameWithModes(),
                    CollectionUtils.combine(r.output(), tsid.get())
                );
            } else {
                return new EsRelation(r.source(), r.indexPattern(), indexMode, r.indexNameWithModes(), r.output());
            }
        });
        final var firstPhase = new TimeSeriesAggregate(
            newChild.source(),
            newChild,
            firstPassGroupings,
            mergeExpressions(firstPassAggs, firstPassGroupings),
            (Bucket) Alias.unwrap(timeBucket)
        );
        return new Aggregate(firstPhase.source(), firstPhase, secondPassGroupings, mergeExpressions(secondPassAggs, secondPassGroupings));
    }

    private static List<? extends NamedExpression> mergeExpressions(
        List<? extends NamedExpression> aggregates,
        List<Expression> groupings
    ) {
        List<NamedExpression> merged = new ArrayList<>(aggregates.size() + groupings.size());
        merged.addAll(aggregates);
        groupings.forEach(g -> merged.add(Expressions.attribute(g)));
        return merged;
    }

    private static class InternalNames {
        final Map<String, Integer> next = new HashMap<>();

        String next(String prefix) {
            int id = next.merge(prefix, 1, Integer::sum);
            return prefix + "_$" + id;
        }
    }
}
