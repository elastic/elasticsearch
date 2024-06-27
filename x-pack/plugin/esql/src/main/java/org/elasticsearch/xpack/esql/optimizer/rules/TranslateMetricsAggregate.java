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
import org.elasticsearch.xpack.esql.core.optimizer.OptimizerRules;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

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
 * Mixing between rate and non-rate aggregates will be supported later.
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
        Map<Rate, Alias> rateAggs = new HashMap<>(); // TODO
        List<NamedExpression> nonRateAggs = new ArrayList<>();
        List<Alias> outerRateAggs = new ArrayList<>();
        for (NamedExpression agg : metrics.aggregates()) {
            if (agg instanceof Alias alias) {
                // METRICS af(rate(counter)) becomes STATS $rate_1=rate(counter) | STATS `af(rate(counter))`=af($rate_1)
                if (alias.child() instanceof AggregateFunction outerRate) {
                    Holder<Boolean> changed = new Holder<>(Boolean.FALSE);
                    Expression outerAgg = outerRate.transformDown(Rate.class, rate -> {
                        changed.set(Boolean.TRUE);
                        Alias rateAgg = rateAggs.computeIfAbsent(rate, k -> new Alias(rate.source(), agg.name(), rate));
                        return rateAgg.toAttribute();
                    });
                    if (changed.get()) {
                        outerRateAggs.add(new Alias(alias.source(), alias.name(), null, outerAgg, agg.id()));
                    }
                } else {
                    nonRateAggs.add(agg);
                }
            }
        }
        if (rateAggs.isEmpty()) {
            return toStandardAggregate(metrics);
        }
        if (nonRateAggs.isEmpty() == false) {
            // TODO: support this
            throw new IllegalArgumentException("regular aggregates with rate aggregates are not supported yet");
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
        List<Expression> initialGroupings = new ArrayList<>();
        initialGroupings.add(tsid.get());
        List<Expression> finalGroupings = new ArrayList<>();
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
        List<NamedExpression> initialAggs = new ArrayList<>(rateAggs.values());
        for (Expression group : metrics.groupings()) {
            if (group instanceof Attribute == false) {
                throw new EsqlIllegalArgumentException("expected named expression for grouping; got " + group);
            }
            final Attribute g = (Attribute) group;
            final NamedExpression newFinalGroup;
            if (timeBucket != null && g.id().equals(timeBucket.id())) {
                newFinalGroup = timeBucket.toAttribute();
                initialGroupings.add(newFinalGroup);
            } else {
                newFinalGroup = new Alias(g.source(), g.name(), null, new Values(g.source(), g), g.id());
                initialAggs.add(newFinalGroup);
            }
            finalGroupings.add(new Alias(g.source(), g.name(), null, newFinalGroup.toAttribute(), g.id()));
        }
        var finalAggregates = Stream.concat(outerRateAggs.stream(), nonRateAggs.stream()).toList();
        return newAggregate(
            newAggregate(metrics.child(), Aggregate.AggregateType.METRICS, initialAggs, initialGroupings),
            Aggregate.AggregateType.STANDARD,
            finalAggregates,
            finalGroupings
        );
    }

    private static Aggregate toStandardAggregate(Aggregate metrics) {
        final LogicalPlan child = metrics.child().transformDown(EsRelation.class, r -> {
            var attributes = new ArrayList<>(new AttributeSet(metrics.inputSet()));
            attributes.removeIf(a -> a.name().equals(MetadataAttribute.TSID_FIELD));
            if (attributes.stream().noneMatch(a -> a.name().equals(MetadataAttribute.TIMESTAMP_FIELD)) == false) {
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
