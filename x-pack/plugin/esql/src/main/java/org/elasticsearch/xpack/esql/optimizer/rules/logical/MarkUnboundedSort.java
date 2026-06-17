/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.List;

/**
 * After all limit-push-down passes have run, any {@link OrderBy} node that remains is genuinely
 * unbounded: the optimizer already tried every avenue to combine a {@link org.elasticsearch.xpack.esql.plan.logical.Limit}
 * with that sort and failed (e.g. a post-join {@code WHERE} on a joined field, or an {@code MV_EXPAND},
 * blocking the outer limit from reaching the sort).
 * <p>
 * For those cases, when the sort's data source is a Lucene-backed {@link EsRelation} and every sort
 * key is a native indexed field, convert the {@link OrderBy} to a {@link TopN} with the
 * {@code unboundedSort} flag set. The physical optimizer's {@code PushTopNToSource} rule will then
 * push this to a {@code LuceneSearchAfterSortedSourceOperator} on each shard, which streams documents
 * in sort order without allocating a fixed-size priority queue. The coordinator's
 * {@code SortedMergeSourceOperator} merges the per-shard streams, and any outer {@code LIMIT N}
 * downstream acts as the pipeline stop.
 * <p>
 * The {@code unboundedSort} flag is the authoritative gate at every downstream decision point.
 * {@link Integer#MAX_VALUE} is used as a structural limit placeholder (required because downstream
 * plan nodes expect a foldable limit literal), but no code should key off this value — only the flag.
 * <p>
 * This rule is gated on {@link #STREAMING_SORT_VERSION}: on mixed-version clusters where any node
 * is older than this version the rule is skipped, and the query falls back to the pre-existing
 * "Unbounded SORT not supported" rejection. This prevents sending an unbounded-sort reduction plan
 * to data nodes that do not understand the per-shard sink / sorted-merge model.
 * <p>
 * Example queries that benefit from this rule:
 * <pre>{@code
 * FROM main_index
 * | SORT main_idx_field DESC
 * | LOOKUP JOIN lookup_index ON lookup_key
 * | WHERE lookup_field == "foo"
 * | LIMIT 25
 *
 * FROM main_index
 * | SORT main_idx_field
 * | MV_EXPAND tags
 * | WHERE tags == "foo"
 * | LIMIT 25
 * }</pre>
 */
public class MarkUnboundedSort extends OptimizerRules.ParameterizedOptimizerRule<OrderBy, LogicalOptimizerContext> {

    /**
     * Transport version at which the streaming sorted-merge execution model (per-shard sinks +
     * {@link org.elasticsearch.compute.operator.topn.SortedMergeSourceOperator}) was introduced.
     * The rule is gated on this version so that mixed-version clusters do not receive a plan
     * containing an unbounded-sort {@code TopNExec} on nodes that cannot execute it.
     */
    public static final TransportVersion STREAMING_SORT_VERSION = TransportVersion.fromName("esql_unbounded_sort");

    public MarkUnboundedSort() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(OrderBy orderBy, LogicalOptimizerContext ctx) {
        if (ctx.minimumVersion().supports(STREAMING_SORT_VERSION) == false) {
            return orderBy;
        }
        if (hasLuceneSource(orderBy.child()) == false) {
            return orderBy;
        }
        if (hasOnlyNativeFieldSorts(orderBy.order()) == false) {
            return orderBy;
        }
        // Integer.MAX_VALUE is kept as the limit value (semantically "no limit") so that downstream
        // code that inspects the limit for Lucene routing still routes correctly. The unboundedSort
        // flag — not the value — is the authoritative gate at every decision point.
        var maxLimit = new Literal(orderBy.source(), Integer.MAX_VALUE, DataType.INTEGER);
        return new TopN(orderBy.source(), orderBy.child(), orderBy.order(), maxLimit, false).withUnboundedSort();
    }

    /**
     * Returns {@code true} only when the plan's leaf is an {@link EsRelation} (Lucene-backed),
     * traversing through {@link UnaryPlan} nodes but stopping at {@link MvExpand} (which produces
     * synthetic rows that are not Lucene doc-level, so sorting them like Lucene docs is incorrect).
     */
    static boolean hasLuceneSource(LogicalPlan plan) {
        LogicalPlan current = plan;
        while (current instanceof UnaryPlan unary) {
            if (unary instanceof MvExpand) {
                return false;
            }
            current = unary.child();
        }
        return current instanceof EsRelation;
    }

    /**
     * Returns {@code true} only when every sort key is a plain {@link FieldAttribute} — i.e. a native
     * Elasticsearch field that can be pushed to {@code LuceneSearchAfterSortedSourceOperator}.
     * Computed expressions from {@code EVAL} produce {@code ReferenceAttribute}s, and metadata fields
     * like {@code _score} produce {@code MetadataAttribute}s; neither can be sorted at the Lucene level,
     * so the streaming path must not be activated for them.
     */
    static boolean hasOnlyNativeFieldSorts(List<Order> orders) {
        for (Order order : orders) {
            if (order.child() instanceof FieldAttribute == false) {
                return false;
            }
        }
        return true;
    }
}
