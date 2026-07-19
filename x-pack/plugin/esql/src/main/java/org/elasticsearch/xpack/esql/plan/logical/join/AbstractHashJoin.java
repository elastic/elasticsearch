/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Shared base for coordinator-only joins whose right ("build") side is an independent subquery that must be executed first, its result
 * buffered into a {@link LocalRelation}, and only then joined against the streaming left ("probe") side. Concretely this covers
 * the {@code field IN (subquery)} family ({@link AbstractSubqueryJoin} and its {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin}
 * subtypes) and the PromQL vector-matching INNER equi-join ({@code EqJoin}).
 * <p>
 * Unlike {@link InlineJoin}, the right side does not embed the left via a {@code StubRelation}; it is a self-contained subquery, so no stub
 * replacement or deep copy is needed and the node executed as the subplan is the very same instance held on the join's right.
 * <p>
 * The coordinator phase loop (see {@code EsqlSession}) drives this base directly: {@link #firstSubPlan} yields the next
 * unmaterialized right subquery and the concrete join family substitutes the materialized result back into the plan.
 * <p>
 * These nodes are ephemeral: they are resolved away on the coordinator before the physical plan crosses the wire, so they are never
 * serialized (see {@link #writeTo}).
 */
public abstract class AbstractHashJoin extends Join implements SortPreserving, ExecutesOn.Coordinator {

    protected AbstractHashJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config, ExecuteLocation.ANY);
    }

    protected AbstractHashJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields
    ) {
        super(source, left, right, type, leftFields, rightFields, null, ExecuteLocation.ANY);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    /**
     * Finds the first (bottom-up) join in the plan whose right subquery has not yet been replaced with results, and returns it as the next
     * subplan to execute. Bottom-up ordering ensures nested subqueries are resolved before the outer ones that depend on them.
     */
    public static LogicalPlanTuple firstSubPlan(LogicalPlan optimizedPlan, Set<LocalRelation> subPlansResults) {
        Holder<LogicalPlan> subPlanHolder = new Holder<>();
        optimizedPlan.forEachUp(AbstractHashJoin.class, join -> {
            if (subPlanHolder.get() == null) {
                if (join.right() instanceof LocalRelation lr && subPlansResults.contains(lr)) {
                    return;
                }
                subPlanHolder.set(join.right());
            }
        });
        LogicalPlan subPlan = subPlanHolder.get();
        if (subPlan == null) {
            return null;
        }
        subPlan.setOptimized();
        // The subplan is the very same instance held on the join's right side, so it doubles as the identity key used to substitute the
        // materialized result back into the main plan - hence both tuple slots are the same.
        return new LogicalPlanTuple(subPlan, subPlan);
    }

    /**
     * Build the terminal plan for a materialized hash-join path. The right side has already been wrapped in a {@link LocalRelation}.
     * Subclasses can override this to keep different rows or append computed output, while sharing the common right-side materialization
     * and subplan scheduling owned by this base class.
     */
    protected LogicalPlan buildHashJoinPathPlan(
        LogicalPlan leftSide,
        LocalRelation materializedRight,
        JoinConfig leftJoinConfig,
        Attribute sentinelAttr,
        Source source,
        boolean rightHadNulls
    ) {
        Join leftJoin = new Join(source, leftSide, materializedRight, leftJoinConfig, ExecuteLocation.ANY);
        Filter filter = new Filter(source, leftJoin, sentinelFilterCondition(source, sentinelAttr));
        List<NamedExpression> leftOutput = new ArrayList<>(left().output());
        return new Project(source, filter, leftOutput);
    }

    /**
     * Sentinel-column filter used by hash-join rewrites. The default keeps matched rows; subclasses can invert or replace this condition.
     */
    protected Expression sentinelFilterCondition(Source source, Attribute sentinel) {
        return new IsNotNull(source, sentinel);
    }

    /**
     * Whether NULL-keyed left rows should be removed before the materialized hash join. The default avoids treating NULL as a joinable
     * key when the terminal plan filters after a LEFT join.
     */
    protected boolean filterNullLeftKeysBeforeHashJoin() {
        return true;
    }

    /**
     * Tuple holding the subplan to execute and the original plan node used as the identity key when substituting the result back.
     */
    public record LogicalPlanTuple(LogicalPlan subPlan, LogicalPlan originalSubPlan) {}

}
