/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * The engine's INNER equi-join, currently used to implement PromQL vector matching. It is a specialized INNER join: the right ("build")
 * side must have unique keys (the physical {@link org.elasticsearch.xpack.esql.plan.physical.EqJoinExec} builds a {@code RowInTableLookup}
 * from it), so each probe row matches at most one build row - i.e. it covers 1:1 and many-to-one, never many-to-many. It is the INNER
 * counterpart of the hash flavor of LEFT join ({@code HashJoinExec}): same broadcast, in-memory, single-valued-key, unique-build machinery,
 * differing only in that unmatched probe rows are dropped rather than null-filled.
 * <p>
 * As an {@link AbstractHashJoin} its right side is an independent subquery, materialized to a {@code LocalRelation} by the
 * coordinator phase loop before the {@code Mapper} lowers it to an {@code EqJoinExec}. The left side is the "many"/probe side
 * (its identity is preserved). {@code addedFields} are the build columns copied into the result (value + copied labels).
 * {@code unique} selects 1:1 matching (at most one probe row per build row) vs many-to-one ({@code group_left}/{@code group_right}).
 * Being coordinator-only and ephemeral, it is never serialized (see {@link AbstractHashJoin#writeTo}).
 */
public class EqJoin extends AbstractHashJoin {

    private final List<Attribute> addedFields;
    private final boolean unique;
    private List<Attribute> lazyOutput;

    public EqJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        boolean unique
    ) {
        super(source, left, right, JoinTypes.INNER, leftFields, rightFields);
        this.addedFields = addedFields;
        this.unique = unique;
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
            List<Attribute> leftOutputWithoutKeys = left().output().stream().filter(attr -> leftFields().contains(attr) == false).toList();
            List<Attribute> rightWithAppendedKeys = new ArrayList<>(right().output());
            rightWithAppendedKeys.removeAll(rightFields());
            rightWithAppendedKeys.addAll(leftFields());
            lazyOutput = mergeOutputAttributes(rightWithAppendedKeys, leftOutputWithoutKeys);
        }
        return lazyOutput;
    }

    /**
     * {@link Join#computeOutputExpressions(List, List)} throws for non-LEFT join types, so INNER computes its own output (mirroring
     * {@link #output()}, which is what the physical {@code EqJoinExec} produces).
     */
    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return new ArrayList<>(output());
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    /**
     * Rebuilds the main plan after the right-side subquery has been materialized, replacing the matching EqJoin's right child with
     * the materialized local relation.
     */
    public static LogicalPlan newMainPlan(
        LogicalPlan optimizedPlan,
        AbstractHashJoin.LogicalPlanTuple subPlans,
        LocalRelation resultWrapper
    ) {
        LogicalPlan newPlan = optimizedPlan.transformUp(
            EqJoin.class,
            join -> join.right() == subPlans.originalSubPlan() ? join.replaceRight(resultWrapper) : join
        );
        newPlan.setOptimized();
        return newPlan;
    }

    @Override
    public EqJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new EqJoin(source(), left, right, leftFields(), rightFields(), addedFields, unique);
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, EqJoin::new, left(), right(), leftFields(), rightFields(), addedFields, unique);
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
        EqJoin eqJoin = (EqJoin) o;
        return unique == eqJoin.unique && addedFields.equals(eqJoin.addedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), addedFields, unique);
    }
}
