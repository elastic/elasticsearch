/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.function.Predicate.not;
import static java.util.stream.Stream.concat;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Like {@link LookupJoin} but unmatched results are dropped.
 */
public class InnerJoin extends Join implements SortPreserving {

    private final List<Attribute> addedFields;
    private final boolean unique;
    private List<Attribute> lazyOutput;

    public InnerJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        boolean unique
    ) {
        super(source, left, right, JoinTypes.INNER, leftFields, rightFields, null, ExecuteLocation.COORDINATOR);
        this.addedFields = addedFields;
        this.unique = unique;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<Attribute> leftFields() {
        return config().leftFields();
    }

    public List<Attribute> rightFields() {
        return config().rightFields();
    }

    public List<Attribute> addedFields() {
        return addedFields;
    }

    public boolean unique() {
        return unique;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(
                concat(right().output().stream().filter(not(rightFields()::contains)), leftFields().stream()).toList(),
                left().output().stream().filter(not(leftFields()::contains)).toList()
            );
        }
        return lazyOutput;
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return new ArrayList<>(output());
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    /**
     * Returns {@code true} when this join still needs its right-side subplan to be executed, i.e.
     * the right side has not yet been replaced with a materialized {@link LocalRelation}.
     */
    public static boolean isPending(InnerJoin join, Set<LocalRelation> subPlansResults) {
        return !(join.right() instanceof LocalRelation lr && subPlansResults.contains(lr));
    }

    /**
     * Extracts the subplan tuple from a specific {@link InnerJoin} node that is already known to be
     * pending (via {@link #isPending}). Unlike {@link #firstSubPlan}, this does not re-scan the whole
     * plan. The right side is an independent subquery, so the executed node is the very same instance
     * held on the join's right side and doubles as the identity key for {@link #newMainPlan}.
     */
    public static LogicalPlanTuple subPlanFor(InnerJoin join) {
        LogicalPlan subPlan = join.right();
        subPlan.setOptimized();
        return new LogicalPlanTuple(subPlan, subPlan);
    }

    /**
     * Finds the first (bottom-up) {@link InnerJoin} whose right subquery has not yet been replaced with results.
     */
    public static LogicalPlanTuple firstSubPlan(LogicalPlan optimizedPlan, Set<LocalRelation> subPlansResults) {
        var joinHolder = new Holder<InnerJoin>();
        optimizedPlan.forEachUp(InnerJoin.class, join -> {
            if (joinHolder.get() == null && isPending(join, subPlansResults)) {
                joinHolder.set(join);
            }
        });
        InnerJoin join = joinHolder.get();
        return join != null ? subPlanFor(join) : null;
    }

    /**
     * Rebuilds the main plan after the right-side subquery has been materialized, replacing the matching InnerJoin's right child with
     * the materialized local relation.
     */
    public static LogicalPlan newMainPlan(LogicalPlan optimizedPlan, LogicalPlanTuple subPlans, LocalRelation resultWrapper) {
        LogicalPlan newPlan = optimizedPlan.transformUp(
            InnerJoin.class,
            join -> join.right() == subPlans.originalSubPlan() ? join.replaceRight(resultWrapper) : join
        );
        newPlan.setOptimized();
        return newPlan;
    }

    @Override
    public InnerJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new InnerJoin(source(), left, right, leftFields(), rightFields(), addedFields, unique);
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, InnerJoin::new, left(), right(), leftFields(), rightFields(), addedFields, unique);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        InnerJoin innerJoin = (InnerJoin) o;
        return unique == innerJoin.unique && addedFields.equals(innerJoin.addedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), addedFields, unique);
    }

    /**
     * Tuple holding the subplan to execute and the original plan node used as the identity key when substituting the result back.
     */
    public record LogicalPlanTuple(LogicalPlan subPlan, LogicalPlan originalSubPlan) {}
}
