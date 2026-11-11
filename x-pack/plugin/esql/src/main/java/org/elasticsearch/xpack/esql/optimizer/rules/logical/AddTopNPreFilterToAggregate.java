/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.operator.topn.TopNPreFilterOperator;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNPreFilter;

import java.util.List;

/**
 * Adds a {@link TopNPreFilter} before an {@link Aggregate} whose output is limited to a small number of groups, for example:
 * <pre>
 *  .. | STATS ... BY k, ... | SORT k | LIMIT N
 *  becomes
 *  .. | TopNPreFilter(k) | STATS ... BY k, ... | TOPN k N
 *
 *  .. | STATS ... BY k, ... | LIMIT N
 *  becomes
 *  .. | TopNPreFilter(k) | STATS ... BY k, ... | TOPN k ASC N
 *
 * </pre>
 * <p>
 * {@link TopNPreFilter} is an approximate filter: it never drops a row that makes it into the final top {@code N} groups,
 * but it may keep rows outside of them. For correctness, the aggregate must be followed by an exact {@link TopN} on the
 * same key, which discards those possibly incomplete groups. Also, no filter may sit between the {@link TopNPreFilter} and
 * the aggregate, as it could drop rows that belong to the final {@link TopN}; hence a dedicated plan node rather than a filter
 * expression, which the optimizer could reorder.
 */
public final class AddTopNPreFilterToAggregate extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {
    public static final TransportVersion TOPN_PREFILTER_LONG = TransportVersion.fromName("topn_prefilter_long");
    /**
     * Matches the default limit, so a {@code STATS} without an explicit {@code LIMIT} will have this optimization.
     * With 1000 distinct long keys, the overhead of the pre-filter and the final {@link TopN} is small even when the filter drops
     * no rows at all. If more expensive types such as {@code BytesRef} are supported later, this threshold may need to be
     * lower for them, but 1000 is fine for longs.
     */
    private static final int LIMIT_THRESHOLD = 1000;

    public AddTopNPreFilterToAggregate() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Limit limit, LogicalOptimizerContext context) {
        if (validLimit(limit) == false) {
            return limit;
        }
        if (limit.child() instanceof OrderBy orderBy && orderBy.child() instanceof Aggregate aggregate) {
            Order primary = orderBy.order().getFirst();
            if (validAggregate(aggregate)
                && primary.child() instanceof Attribute output
                && isSupportedType(context.minimumVersion(), output.dataType())
                && groupingKey(aggregate, output) instanceof Attribute key) {
                var preFilter = new TopNPreFilter(
                    limit.source(),
                    aggregate.child(),
                    key,
                    limit.limit(),
                    primary.direction() == Order.OrderDirection.ASC,
                    primary.nullsPosition() == Order.NullsPosition.FIRST
                );
                return new TopN(limit.source(), aggregate.replaceChild(preFilter), orderBy.order(), limit.limit(), limit.local());
            }
            return limit;
        }
        if (limit.child() instanceof Aggregate aggregate
            && validAggregate(aggregate)
            && firstGroupingKeyCandidate(context.minimumVersion(), aggregate) instanceof Attribute key) {
            for (NamedExpression output : aggregate.aggregates()) {
                if (Alias.unwrap(output).semanticEquals(key)) {
                    var preFilter = new TopNPreFilter(limit.source(), aggregate.child(), key, limit.limit(), true, false);
                    Order order = new Order(limit.source(), output.toAttribute(), Order.OrderDirection.ASC, Order.NullsPosition.LAST);
                    return new TopN(limit.source(), aggregate.replaceChild(preFilter), List.of(order), limit.limit(), limit.local());
                }
            }
        }
        return limit;
    }

    private static Attribute firstGroupingKeyCandidate(TransportVersion minVersion, Aggregate aggregate) {
        for (Expression g : aggregate.groupings()) {
            if (g instanceof Attribute key && isSupportedType(minVersion, key.dataType())) {
                return key;
            }
        }
        return null;
    }

    private static boolean validLimit(Limit limit) {
        if (limit.limit() instanceof Literal literal && literal.value() instanceof Number n) {
            long value = n.longValue();
            return value > 0 && value <= LIMIT_THRESHOLD;
        }
        return false;
    }

    private static boolean validAggregate(Aggregate aggregate) {
        // works with a single grouping too, but the gain there is could be small to pay for the filter, so require two groups
        return aggregate.getClass() == Aggregate.class && aggregate.groupings().size() >= 2;
    }

    private static Attribute groupingKey(Aggregate aggregate, Attribute output) {
        for (NamedExpression e : aggregate.aggregates()) {
            if (e.toAttribute().semanticEquals(output)) {
                return Alias.unwrap(e) instanceof Attribute key ? key : null;
            }
        }
        return null;
    }

    /**
     * Matches the supported type in {@link TopNPreFilterOperator}
     */
    private static boolean isSupportedType(TransportVersion minVersion, DataType type) {
        return minVersion.supports(TOPN_PREFILTER_LONG) && type == DataType.LONG;
    }
}
