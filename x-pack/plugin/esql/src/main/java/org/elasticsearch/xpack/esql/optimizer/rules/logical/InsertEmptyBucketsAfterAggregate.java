/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.operator.InsertEmptyBucketsOperator.DefaultValue;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.InsertEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

/**
 * Inserts a coordinator-only {@link InsertEmptyBuckets} above each {@link Aggregate} whose groupings include one or more
 * {@link Bucket}s (or {@link TBucket}s) with {@code {"include_empty_buckets": true}}, and attaches default values for
 * aggregate outputs (zero for {@link Count}/{@link CountDistinct}, null otherwise).
 * <p>
 * Runs in the Substitutions batch after {@link ReplaceAggregateAggExpressionWithEval} and surrogate aggregation rewrites,
 * so defaults target the decomposed aggregate outputs (e.g. {@code AVG} → {@code SUM}/{@code COUNT}) and so
 * {@link InsertEmptyBuckets} sits directly above the Aggregate — including under {@code UnpackDims} for time-series
 * aggregations with dimensions, where groupings already use the packed attribute.
 * <p>
 * Bucket expressions are rediscovered from the Aggregate's groupings and from {@link Eval} nodes below it (where
 * {@link ReplaceAggregateNestedExpressionWithEval} extracts evaluatable grouping functions). {@link TimeSeriesAggregate}
 * first-pass nodes are skipped; empty buckets are filled against the second-pass {@link Aggregate}.
 */
public final class InsertEmptyBucketsAfterAggregate extends OptimizerRules.OptimizerRule<Aggregate> {

    public InsertEmptyBucketsAfterAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (aggregate instanceof TimeSeriesAggregate) {
            // First-pass TS agg; empty buckets are filled against the second-pass Aggregate above.
            return aggregate;
        }

        AttributeMap<Expression> aliases = collectAliases(aggregate);

        AttributeMap.Builder<Bucket> bucketsBuilder = AttributeMap.builder();
        AttributeSet.Builder groupsBuilder = AttributeSet.builder();
        for (Expression grouping : aggregate.groupings()) {
            Attribute attribute = Expressions.attribute(grouping);
            Expression expression = aliases.resolve(attribute);
            if (expression instanceof Bucket bucket && bucket.includeEmptyBuckets()) {
                bucketsBuilder.put(attribute, bucket);
            } else {
                groupsBuilder.add(attribute);
            }
        }
        AttributeMap<Bucket> buckets = bucketsBuilder.build();
        AttributeSet groups = groupsBuilder.build();
        return buckets.isEmpty()
            ? aggregate
            : new InsertEmptyBuckets(aggregate.source(), aggregate, buckets, groups, defaultValues(aggregate, buckets, groups));
    }

    private static AttributeMap<Expression> collectAliases(Aggregate aggregate) {
        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        aggregate.forEachExpression(Alias.class, a -> aliasesBuilder.put(a.toAttribute(), a.child()));
        collectAliases(aggregate.child(), aliasesBuilder);
        return aliasesBuilder.build();
    }

    private static void collectAliases(LogicalPlan plan, AttributeMap.Builder<Expression> aliasesBuilder) {
        if (plan instanceof Aggregate && plan instanceof TimeSeriesAggregate == false) {
            // Stop at any Aggregate beyond the original one: any "include_empty_buckets" belongs to
            // that Aggregate, and not to the one currently being processed.
            // Do continue processing a TimeSeriesAggregate, which is this Aggregate's "sibling".
            return;
        }
        plan.forEachExpression(Alias.class, a -> aliasesBuilder.put(a.toAttribute(), a.child()));
        for (LogicalPlan child : plan.children()) {
            collectAliases(child, aliasesBuilder);
        }
    }

    private static AttributeMap<DefaultValue> defaultValues(Aggregate aggregate, AttributeMap<Bucket> buckets, AttributeSet groups) {
        AttributeMap.Builder<DefaultValue> defaultValues = AttributeMap.builder();
        for (NamedExpression aggregateExpression : aggregate.aggregates()) {
            Attribute attribute = aggregateExpression.toAttribute();
            if (buckets.containsKey(attribute) || groups.contains(attribute)) {
                // Grouping field: the operator sources its value/type from the bucket cursor / representative row.
                continue;
            }
            Expression aggFn = Alias.unwrap(aggregateExpression);
            Object value = (aggFn instanceof Count) || (aggFn instanceof CountDistinct) ? 0L : null;
            defaultValues.put(attribute, new DefaultValue(PlannerUtils.toElementType(attribute.dataType()), value));
        }
        return defaultValues.build();
    }
}
