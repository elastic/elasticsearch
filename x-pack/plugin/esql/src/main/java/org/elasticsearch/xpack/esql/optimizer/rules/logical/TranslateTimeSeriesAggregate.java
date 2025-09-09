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
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * Non-rate aggregates will be rewritten as a pair of to_partial and from_partial aggregates, where the `to_partial`
 * aggregates will be executed in the first pass and always produce an intermediate output regardless of the aggregate
 * mode. The `from_partial` aggregates will be executed on the second pass and always receive intermediate output
 * produced by `to_partial`. Examples:
 *
 * <pre>
 * TS k8s | STATS max(rate(request)), max(memory_used) becomes:
 *
 * TS k8s
 * | STATS rate_$1=rate(request), $p1=to_partial(max(memory_used)) BY _tsid
 * | STATS max(rate_$1), `max(memory_used)` = from_partial($p1, max($_))
 *
 * TS k8s | STATS max(rate(request)) avg(memory_used) BY host
 *
 * becomes
 *
 * TS k8s
 * | STATS rate_$1=rate(request), $p1=to_partial(sum(memory_used)), $p2=to_partial(count(memory_used)), VALUES(host) BY _tsid
 * | STATS max(rate_$1), $sum=from_partial($p1, sum($_)), $count=from_partial($p2, count($_)) BY host=`VALUES(host)`
 * | EVAL `avg(memory_used)` = $sum / $count
 * | KEEP `max(rate(request))`, `avg(memory_used)`, host
 *
 * TS k8s | STATS min(memory_used) sum(rate(request)) BY pod, bucket(@timestamp, 5m)
 *
 * becomes
 *
 * TS k8s
 * | EVAL `bucket(@timestamp, 5m)` = datetrunc(@timestamp, '5m')
 * | STATS rate_$1=rate(request), $p1=to_partial(min(memory_used)), VALUES(pod) BY _tsid, `bucket(@timestamp, 5m)`
 * | STATS sum(rate_$1), `min(memory_used)` = from_partial($p1, min($)) BY pod=`VALUES(pod)`, `bucket(@timestamp, 5m)`
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
        Map<AggregateFunction, Alias> timeSeriesAggs = new HashMap<>();
        List<NamedExpression> firstPassAggs = new ArrayList<>();
        List<NamedExpression> secondPassAggs = new ArrayList<>();
        Holder<Boolean> hasRateAggregates = new Holder<>(Boolean.FALSE);
        boolean hasTopLevelOverTimeAggs = false;
        var internalNames = new InternalNames();
        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                Holder<Boolean> changed = new Holder<>(Boolean.FALSE);
                if (af instanceof TimeSeriesAggregateFunction) {
                    hasTopLevelOverTimeAggs = true;
                }
                // NOCOMMIT TODO: If af is a time series agg, do the group by all behavior
                /*
                This transformation does several things
                    - extract the inner TimeSeriesAggregateFunction and get its perTimeSeriesAggregation equivalent
                    - Save that aggregation in timeSeriesAggs to later build a new Aggregate node from
                    - Replace the aggregate sub-function of af with a reference to the extracted aggregation.  This changes
                      outerAgg to be operating on the output of a previous step, rather than on a sub-expression
                 */
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
                    // In this branch, we found a time series aggregation, and are constructing a vertical aggregation around it
                    secondPassAggs.add(new Alias(alias.source(), alias.name(), outerAgg, agg.id()));
                } else {
                    // This case deals with a non-time series agg by splitting it into two partial steps
                    var toPartial = new Alias(agg.source(), alias.name(), new ToPartial(agg.source(), af.field(), af));
                    var fromPartial = new FromPartial(agg.source(), toPartial.toAttribute(), af);
                    firstPassAggs.add(toPartial);
                    secondPassAggs.add(new Alias(alias.source(), alias.name(), fromPartial, alias.id()));
                }
            }
        }
        if (timeSeriesAggs.isEmpty()) {
            // no time-series aggregations, run a regular aggregation instead.
            return new Aggregate(aggregate.source(), aggregate.child(), aggregate.groupings(), aggregate.aggregates());
        }

        // Collect time series specific grouping fields from the leaf relation
        Holder<Attribute> tsid = new Holder<>();
        Holder<Attribute> timestamp = new Holder<>();
        Set<Attribute> dimensions = new HashSet<>();
        aggregate.forEachDown(EsRelation.class, r -> {
            for (Attribute attr : r.output()) {
                if (attr.name().equals(MetadataAttribute.TSID_FIELD)) {
                    tsid.set(attr);
                }
                if (attr.name().equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                    timestamp.set(attr);
                }
                if (attr.isDimension()) {
                    dimensions.add(attr);
                }
            }
        });
        if (tsid.get() == null) {
            tsid.set(new MetadataAttribute(aggregate.source(), MetadataAttribute.TSID_FIELD, DataType.KEYWORD, false));
        }
        if (timestamp.get() == null) {
            throw new IllegalArgumentException("_tsid or @timestamp field are missing from the time-series source");
        }

        // time-series aggregates must be grouped by _tsid (and time-bucket) first and re-group by users key
        List<Expression> firstPassGroupings = new ArrayList<>();
        firstPassGroupings.add(tsid.get());
        List<Expression> secondPassGroupings = new ArrayList<>();
        Holder<NamedExpression> timeBucketRef = new Holder<>();
        // Extract the time grouping from the rest of the groupings.
        aggregate.child().forEachExpressionUp(NamedExpression.class, e -> {
            for (Expression child : e.children()) {
                // We need two branches here because the TBUCKET translation rule runs after this rule
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

        // NOCOMMIT - TODO: Presumably somewhere in here, I need to add the dimension groups
        // Construct the groupings for the new aggregations
        for (Expression group : aggregate.groupings()) {
            if (group instanceof Attribute == false) {
                throw new EsqlIllegalArgumentException("expected named expression for grouping; got " + group);
            }
            final Attribute g = (Attribute) group;
            final NamedExpression newFinalGroup;
            if (timeBucket != null && g.id().equals(timeBucket.id())) {
                // Add the time bucket grouping to both the inner and outer aggregations
                newFinalGroup = timeBucket.toAttribute();
                firstPassGroupings.add(newFinalGroup);
            } else {
                // Pass all the second pass groupings through the first pass aggregation via a values agg
                // NOCOMMIT TODO: Skip this for the group by all case
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
