/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Rate aggregation is special because it must be computed per time series, regardless of the grouping keys.
 * The keys must be `_tsid` or a pair of `_tsid` and `time_bucket`. To support user-defined grouping keys,
 * we first execute the rate aggregation using the time-series keys, then perform another aggregation with
 * the resulting rate using the user-specific keys.
 * <p>
 * This class translates the aggregates in the METRICS commands to standard aggregates.
 * This approach helps avoid introducing new plans and operators for metrics aggregations specially.
 * <p>
 * Examples:
 * <pre>
 * METRICS k8s max(rate(request))
 *
 * becomes
 *
 * METRICS k8s
 * | STATS rate(request) BY _tsid
 * | STATS max(`rate(request)`)
 *
 * METRICS k8s max(rate(request)) BY host
 *
 * becomes
 *
 * METRICS k8s
 * | STATS rate(request), VALUES(host) BY _tsid
 * | STATS max(`rate(request)`) BY host=`VALUES(host)`
 *
 * METRICS k8s avg(rate(request)) BY host
 *
 * becomes
 *
 * METRICS k8s
 * | STATS rate(request), VALUES(host) BY _tsid
 * | STATS sum=sum(`rate(request)`), count(`rate(request)`) BY host=`VALUES(host)`
 * | EVAL `avg(rate(request))` = `sum(rate(request))` / `count(rate(request))`
 * | KEEP `avg(rate(request))`, host
 *
 * METRICS k8s avg(rate(request)) BY host, bucket(@timestamp, 1minute)
 *
 * becomes
 *
 * METRICS k8s
 * | EVAL  `bucket(@timestamp, 1minute)`=datetrunc(@timestamp, 1minute)
 * | STATS rate(request), VALUES(host) BY _tsid,`bucket(@timestamp, 1minute)`
 * | STATS sum=sum(`rate(request)`), count(`rate(request)`) BY host=`VALUES(host)`, `bucket(@timestamp, 1minute)`
 * | EVAL `avg(rate(request))` = `sum(rate(request))` / `count(rate(request))`
 * | KEEP `avg(rate(request))`, host, `bucket(@timestamp, 1minute)`
 * </pre>
 *
 * Non-rate aggregates will be rewritten as a pair of to_partial and from_partial aggregates, where the `to_partial`
 * aggregates will be executed in the first pass and always produce an intermediate output regardless of the aggregate
 * mode. The `from_partial` aggregates will be executed on the second pass and always receive intermediate output
 * produced by `to_partial`. Examples:
 *
 * <pre>
 * METRICS k8s max(rate(request)), max(memory_used) becomes:
 *
 * METRICS k8s
 * | STATS rate(request), $p1=to_partial(max(memory_used)) BY _tsid
 * | STATS max(`rate(request)`), `max(memory_used)` = from_partial($p1, max($_))
 *
 * METRICS k8s max(rate(request)) avg(memory_used) BY host
 *
 * becomes
 *
 * METRICS k8s
 * | STATS rate(request), $p1=to_partial(sum(memory_used)), $p2=to_partial(count(memory_used)), VALUES(host) BY _tsid
 * | STATS max(`rate(request)`), $sum=from_partial($p1, sum($_)), $count=from_partial($p2, count($_)) BY host=`VALUES(host)`
 * | EVAL `avg(memory_used)` = $sum / $count
 * | KEEP `max(rate(request))`, `avg(memory_used)`, host
 *
 * METRICS k8s min(memory_used) sum(rate(request)) BY pod, bucket(@timestamp, 5m)
 *
 * becomes
 *
 * METRICS k8s
 * | EVAL `bucket(@timestamp, 5m)` = datetrunc(@timestamp, '5m')
 * | STATS rate(request), $p1=to_partial(min(memory_used)), VALUES(pod) BY _tsid, `bucket(@timestamp, 5m)`
 * | STATS sum(`rate(request)`), `min(memory_used)` = from_partial($p1, min($)) BY pod=`VALUES(pod)`, `bucket(@timestamp, 5m)`
 * | KEEP `min(memory_used)`, `sum(rate(request))`, pod, `bucket(@timestamp, 5m)`
 * </pre>
 */
public final class TranslateMetricsAggregate extends OptimizerRules.OptimizerRule<Aggregate> {

    public TranslateMetricsAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (aggregate.aggregateType() == Aggregate.AggregateType.METRICS) {
            return translate(aggregate);
        } else {
            return aggregate;
        }
    }

    LogicalPlan translate(Aggregate metrics) {
        Map<Rate, Alias> rateAggs = new HashMap<>();
        List<NamedExpression> firstPassAggs = new ArrayList<>();
        List<NamedExpression> secondPassAggs = new ArrayList<>();
        for (NamedExpression agg : metrics.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction af) {
                Holder<Boolean> changed = new Holder<>(Boolean.FALSE);
                Expression outerAgg = af.transformDown(Rate.class, rate -> {
                    changed.set(Boolean.TRUE);
                    Alias rateAgg = rateAggs.computeIfAbsent(rate, k -> {
                        Alias newRateAgg = new Alias(rate.source(), agg.name(), rate);
                        firstPassAggs.add(newRateAgg);
                        return newRateAgg;
                    });
                    return rateAgg.toAttribute();
                });
                if (changed.get()) {
                    secondPassAggs.add(new Alias(alias.source(), alias.name(), null, outerAgg, agg.id()));
                } else {
                    var toPartial = new Alias(agg.source(), alias.name(), new ToPartial(agg.source(), af.field(), af));
                    var fromPartial = new FromPartial(agg.source(), toPartial.toAttribute(), af);
                    firstPassAggs.add(toPartial);
                    secondPassAggs.add(new Alias(alias.source(), alias.name(), null, fromPartial, alias.id()));
                }
            }
        }
        if (rateAggs.isEmpty()) {
            return toStandardAggregate(metrics);
        }
        Holder<Attribute> tsid = new Holder<>();
        Holder<Attribute> timestamp = new Holder<>();
        metrics.forEachDown(EsRelation.class, r -> {
            for (Attribute attr : r.output()) {
                if (attr.name().equals(MetadataAttribute.TSID_FIELD)) {
                    tsid.set(attr);
                }
                if (attr.name().equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                    timestamp.set(attr);
                }
            }
        });
        if (tsid.get() == null || timestamp.get() == null) {
            throw new IllegalArgumentException("_tsid or @timestamp field are missing from the metrics source");
        }
        // metrics aggregates must be grouped by _tsid (and time-bucket) first and re-group by users key
        List<Expression> firstPassGroupings = new ArrayList<>();
        firstPassGroupings.add(tsid.get());
        List<Expression> secondPassGroupings = new ArrayList<>();
        Holder<NamedExpression> timeBucketRef = new Holder<>();
        metrics.child().forEachExpressionUp(NamedExpression.class, e -> {
            for (Expression child : e.children()) {
                if (child instanceof Bucket bucket && bucket.field().equals(timestamp.get())) {
                    if (timeBucketRef.get() != null) {
                        throw new IllegalArgumentException("expected at most one time bucket");
                    }
                    timeBucketRef.set(e);
                }
            }
        });
        NamedExpression timeBucket = timeBucketRef.get();
        for (Expression group : metrics.groupings()) {
            if (group instanceof Attribute == false) {
                throw new EsqlIllegalArgumentException("expected named expression for grouping; got " + group);
            }
            final Attribute g = (Attribute) group;
            final NamedExpression newFinalGroup;
            if (timeBucket != null && g.id().equals(timeBucket.id())) {
                newFinalGroup = timeBucket.toAttribute();
                firstPassGroupings.add(newFinalGroup);
            } else {
                newFinalGroup = new Alias(g.source(), g.name(), null, new Values(g.source(), g), g.id());
                firstPassAggs.add(newFinalGroup);
            }
            secondPassGroupings.add(new Alias(g.source(), g.name(), null, newFinalGroup.toAttribute(), g.id()));
        }
        return newAggregate(
            newAggregate(metrics.child(), Aggregate.AggregateType.METRICS, firstPassAggs, firstPassGroupings),
            Aggregate.AggregateType.STANDARD,
            secondPassAggs,
            secondPassGroupings
        );
    }

    private static Aggregate toStandardAggregate(Aggregate metrics) {
        final LogicalPlan child = metrics.child().transformDown(EsRelation.class, r -> {
            var attributes = new ArrayList<>(new AttributeSet(metrics.inputSet()));
            attributes.removeIf(a -> a.name().equals(MetadataAttribute.TSID_FIELD));
            if (attributes.stream().noneMatch(a -> a.name().equals(MetadataAttribute.TIMESTAMP_FIELD))) {
                attributes.removeIf(a -> a.name().equals(MetadataAttribute.TIMESTAMP_FIELD));
            }
            return new EsRelation(r.source(), r.index(), new ArrayList<>(attributes), IndexMode.STANDARD);
        });
        return new Aggregate(metrics.source(), child, Aggregate.AggregateType.STANDARD, metrics.groupings(), metrics.aggregates());
    }

    private static Aggregate newAggregate(
        LogicalPlan child,
        Aggregate.AggregateType type,
        List<? extends NamedExpression> aggregates,
        List<Expression> groupings
    ) {
        return new Aggregate(
            child.source(),
            child,
            type,
            groupings,
            Stream.concat(aggregates.stream(), groupings.stream().map(Expressions::attribute)).toList()
        );
    }
}
