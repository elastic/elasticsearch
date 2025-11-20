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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.PackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.UnpackDimension;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
 * | STATS rate_$1=rate(request), DIMENSION_VALUES(host) BY _tsid
 * | STATS max(rate_$1) BY host=`DIMENSION_VALUES(host)`
 *
 * TS k8s | STATS avg(rate(request)) BY host
 *
 * becomes
 *
 * TS k8s
 * | STATS rate_$1=rate(request), DIMENSION_VALUES(host) BY _tsid
 * | STATS sum(rate_$1), count(rate_$1) BY host=`DIMENSION_VALUES(host)`
 * | EVAL `avg(rate(request))` = `sum(rate_$1)` / `count(rate_$1)`
 * | KEEP `avg(rate(request))`, host
 *
 * TS k8s | STATS avg(rate(request)) BY host, bucket(@timestamp, 1minute)
 *
 * becomes
 *
 * TS k8s
 * | EVAL  `bucket(@timestamp, 1minute)`=datetrunc(@timestamp, 1minute)
 * | STATS rate_$1=rate(request), DIMENSION_VALUES(host) BY _tsid,`bucket(@timestamp, 1minute)`
 * | STATS sum=sum(rate_$1), count(rate_$1) BY host=`DIMENSION_VALUES(host)`, `bucket(@timestamp, 1minute)`
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
public final class TranslateTimeSeriesAggregate extends OptimizerRules.ParameterizedOptimizerRule<
    TimeSeriesAggregate,
    LogicalOptimizerContext> {

    public TranslateTimeSeriesAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, LogicalOptimizerContext context) {
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
        Holder<Boolean> requiredTimeSeriesSource = new Holder<>(Boolean.FALSE);
        var internalNames = new InternalNames();
        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                Holder<Boolean> changed = new Holder<>(Boolean.FALSE);
                final Expression inlineFilter;
                if (af.hasFilter()) {
                    inlineFilter = af.filter();
                    af = af.withFilter(Literal.TRUE);
                } else {
                    inlineFilter = null;
                }
                Expression outerAgg = af.transformDown(TimeSeriesAggregateFunction.class, tsAgg -> {
                    if (inlineFilter != null) {
                        if (tsAgg.hasFilter() == false) {
                            throw new IllegalStateException("inline filter isn't propagated to time-series aggregation");
                        }
                    } else if (tsAgg.hasFilter()) {
                        throw new IllegalStateException("unexpected inline filter in time-series aggregation");
                    }
                    changed.set(Boolean.TRUE);
                    if (tsAgg.requiredTimeSeriesSource()) {
                        requiredTimeSeriesSource.set(Boolean.TRUE);
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
                    final Expression aggField = af.field();
                    var tsAgg = new LastOverTime(af.source(), aggField, af.window(), timestamp.get());
                    final AggregateFunction firstStageFn;
                    if (inlineFilter != null) {
                        firstStageFn = tsAgg.perTimeSeriesAggregation().withFilter(inlineFilter);
                    } else {
                        firstStageFn = tsAgg.perTimeSeriesAggregation();
                    }
                    Alias newAgg = timeSeriesAggs.computeIfAbsent(firstStageFn, k -> {
                        Alias firstStageAlias = new Alias(tsAgg.source(), internalNames.next(tsAgg.functionName()), firstStageFn);
                        firstPassAggs.add(firstStageAlias);
                        return firstStageAlias;
                    });
                    secondPassAggs.add((Alias) agg.transformUp(f -> f == aggField || f instanceof AggregateFunction, e -> {
                        if (e == aggField) {
                            return newAgg.toAttribute();
                        } else if (e instanceof AggregateFunction f) {
                            return f.withFilter(Literal.TRUE);
                        } else {
                            return e;
                        }
                    }));
                }
            }
        }
        if (aggregate.child().output().contains(timestamp.get()) == false) {
            var timestampAwareFunctions = timeSeriesAggs.keySet()
                .stream()
                .filter(ts -> ts instanceof TimestampAware)
                .map(Node::sourceText)
                .sorted()
                .toList();
            if (timestampAwareFunctions.isEmpty() == false) {
                int size = timestampAwareFunctions.size();
                throw new IllegalArgumentException(
                    "Function"
                        + (size > 1 ? "s " : " ")
                        + "["
                        + String.join(", ", timestampAwareFunctions.subList(0, Math.min(size, 3)))
                        + (size > 3 ? ", ..." : "")
                        + "] require"
                        + (size > 1 ? " " : "s ")
                        + "a @timestamp field of type date or date_nanos to be present when run with the TS command, "
                        + "but it was not present."
                );
            }
        }
        // time-series aggregates must be grouped by _tsid (and time-bucket) first and re-group by users key
        List<Expression> firstPassGroupings = new ArrayList<>();
        firstPassGroupings.add(tsid.get());
        List<Alias> packDimensions = new ArrayList<>();
        List<Expression> secondPassGroupings = new ArrayList<>();
        List<Alias> unpackDimensions = new ArrayList<>();
        Holder<NamedExpression> timeBucketRef = new Holder<>();
        aggregate.child().forEachExpressionUp(NamedExpression.class, e -> {
            for (Expression child : e.children()) {
                if (child instanceof Bucket bucket && bucket.field().equals(timestamp.get())) {
                    if (timeBucketRef.get() != null) {
                        throw new IllegalArgumentException("expected at most one time bucket");
                    }
                    timeBucketRef.set(e);
                } else if (child instanceof TBucket tbucket && tbucket.timestamp().equals(timestamp.get())) {
                    if (timeBucketRef.get() != null) {
                        throw new IllegalArgumentException("expected at most one time tbucket");
                    }
                    Bucket bucket = (Bucket) tbucket.surrogate();
                    timeBucketRef.set(new Alias(e.source(), bucket.functionName(), bucket, e.id()));
                }
            }
        });
        NamedExpression timeBucket = timeBucketRef.get();
        for (var group : aggregate.groupings()) {
            if (group instanceof Attribute == false) {
                throw new EsqlIllegalArgumentException("expected named expression for grouping; got " + group);
            }
            final Attribute g = (Attribute) group;
            if (timeBucket != null && g.id().equals(timeBucket.id())) {
                var newFinalGroup = timeBucket.toAttribute();
                firstPassGroupings.add(newFinalGroup);
                secondPassGroupings.add(new Alias(g.source(), g.name(), newFinalGroup.toAttribute(), g.id()));
            } else {
                var valuesAgg = new Alias(g.source(), g.name(), valuesAggregate(context, g));
                firstPassAggs.add(valuesAgg);
                Alias pack = new Alias(
                    g.source(),
                    internalNames.next("pack" + g.name()),
                    new PackDimension(g.source(), valuesAgg.toAttribute())
                );
                packDimensions.add(pack);
                Alias grouping = new Alias(g.source(), internalNames.next("group" + g.name()), pack.toAttribute());
                secondPassGroupings.add(grouping);
                Alias unpack = new Alias(
                    g.source(),
                    g.name(),
                    new UnpackDimension(g.source(), grouping.toAttribute(), g.dataType().noText()),
                    g.id()
                );
                unpackDimensions.add(unpack);
            }
        }
        LogicalPlan newChild = aggregate.child().transformUp(EsRelation.class, r -> {
            IndexMode indexMode = requiredTimeSeriesSource.get() ? r.indexMode() : IndexMode.STANDARD;
            if (r.output().contains(tsid.get()) == false) {
                return r.withIndexMode(indexMode).withAttributes(CollectionUtils.combine(r.output(), tsid.get()));
            } else {
                return r.withIndexMode(indexMode);
            }
        });
        final var firstPhase = new TimeSeriesAggregate(
            aggregate.source(),
            newChild,
            firstPassGroupings,
            mergeExpressions(firstPassAggs, firstPassGroupings),
            (Bucket) Alias.unwrap(timeBucket)
        );
        checkWindow(firstPhase);
        if (packDimensions.isEmpty()) {
            return new Aggregate(
                firstPhase.source(),
                firstPhase,
                secondPassGroupings,
                mergeExpressions(secondPassAggs, secondPassGroupings)
            );
        } else {
            Eval packValues = new Eval(firstPhase.source(), firstPhase, packDimensions);
            Aggregate secondPhase = new Aggregate(
                firstPhase.source(),
                packValues,
                secondPassGroupings,
                mergeExpressions(secondPassAggs, secondPassGroupings)
            );
            Eval unpackValues = new Eval(secondPhase.source(), secondPhase, unpackDimensions);
            List<NamedExpression> projects = new ArrayList<>();
            for (NamedExpression agg : secondPassAggs) {
                projects.add(Expressions.attribute(agg));
            }
            int pos = 0;
            for (Expression group : secondPassGroupings) {
                Attribute g = Expressions.attribute(group);
                if (timeBucket != null && g.id().equals(timeBucket.id())) {
                    projects.add(g);
                } else {
                    projects.add(unpackDimensions.get(pos++).toAttribute());
                }
            }
            return new Project(newChild.source(), unpackValues, projects);
        }
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

    private AggregateFunction valuesAggregate(LogicalOptimizerContext context, Attribute group) {
        if (group.isDimension() && context.minimumVersion().supports(DimensionValues.DIMENSION_VALUES_VERSION)) {
            return new DimensionValues(group.source(), group);
        } else {
            return new Values(group.source(), group);
        }
    }

    private static class InternalNames {
        final Map<String, Integer> next = new HashMap<>();

        String next(String prefix) {
            int id = next.merge(prefix, 1, Integer::sum);
            return prefix + "_$" + id;
        }
    }

    void checkWindow(TimeSeriesAggregate agg) {
        boolean hasWindow = false;
        for (NamedExpression aggregate : agg.aggregates()) {
            if (Alias.unwrap(aggregate) instanceof AggregateFunction af && af.hasWindow()) {
                hasWindow = true;
                break;
            }
        }
        if (hasWindow == false) {
            return;
        }
        final long bucketInMillis = getTimeBucketInMillis(agg);
        if (bucketInMillis <= 0) {
            throw new EsqlIllegalArgumentException(
                "Using a window in aggregation [{}] requires a time bucket in groupings",
                agg.sourceText()
            );
        }
        for (NamedExpression aggregate : agg.aggregates()) {
            if (Alias.unwrap(aggregate) instanceof AggregateFunction af && af.hasWindow()) {
                Expression window = af.window();
                if (window.foldable() && window.fold(FoldContext.small()) instanceof Duration d) {
                    final long windowInMills = d.toMillis();
                    if (windowInMills >= bucketInMillis && windowInMills % bucketInMillis == 0) {
                        continue;
                    }
                }
                throw new EsqlIllegalArgumentException(
                    "Unsupported window [{}] for aggregate function [{}]; "
                        + "the window must be larger than the time bucket [{}] and an exact multiple of it",
                    window.sourceText(),
                    af.sourceText(),
                    Objects.requireNonNull(agg.timeBucket()).sourceText()
                );
            }
        }
    }

    private long getTimeBucketInMillis(TimeSeriesAggregate agg) {
        final Bucket bucket = agg.timeBucket();
        if (bucket != null && bucket.buckets().foldable() && bucket.buckets().fold(FoldContext.small()) instanceof Duration d) {
            return d.toMillis();
        }
        return -1L;
    }
}
