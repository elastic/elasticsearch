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
 * Coordinator hash join over a local build side (maps to {@code HashJoinExec}).
 * <p>
 * {@link JoinTypes#INNER} drops unmatched probe rows; {@link JoinTypes#LEFT} keeps them (PromQL {@code unless}).
 */
public class HashJoin extends Join implements SortPreserving {

    private final List<Attribute> addedFields;
    private final boolean unique;
    private List<Attribute> lazyOutput;

    public HashJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        boolean unique
    ) {
        this(source, left, right, leftFields, rightFields, addedFields, unique, JoinTypes.INNER);
    }

    public HashJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        boolean unique,
        JoinType type
    ) {
        super(source, left, right, type, leftFields, rightFields, null, ExecuteLocation.COORDINATOR);
        if (type != JoinTypes.INNER && type != JoinTypes.LEFT) {
            throw new IllegalArgumentException("HashJoin supports INNER or LEFT, got [" + type.joinName() + "]");
        }
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
     * Finds the first (bottom-up) {@link HashJoin} whose right subquery has not yet been replaced with results.
     */
    public static LogicalPlanTuple firstSubPlan(LogicalPlan optimizedPlan, Set<LocalRelation> subPlansResults) {
        var subPlanHolder = new Holder<LogicalPlan>();
        optimizedPlan.forEachUp(HashJoin.class, join -> {
            if (subPlanHolder.get() == null) {
                if ((join.right() instanceof LocalRelation lr && subPlansResults.contains(lr)) == false) {
                    subPlanHolder.set(join.right());
                }
            }
        });

        var subPlan = subPlanHolder.get();
        if (subPlan == null) {
            return null;
        }
        subPlan.setOptimized();
        // same instance held on the join's right side, so it doubles as the identity key used to substitute the
        // materialized result back into the main plan hence both tuple slots are the same.
        return new LogicalPlanTuple(subPlan, subPlan);
    }

    /**
     * Rebuilds the main plan after the right-side subquery has been materialized, replacing the matching HashJoin's right child with
     * the materialized local relation.
     */
    public static LogicalPlan newMainPlan(LogicalPlan optimizedPlan, LogicalPlanTuple subPlans, LocalRelation resultWrapper) {
        LogicalPlan newPlan = optimizedPlan.transformUp(
            HashJoin.class,
            join -> join.right() == subPlans.originalSubPlan() ? join.replaceRight(resultWrapper) : join
        );
        newPlan.setOptimized();
        return newPlan;
    }

    @Override
    public HashJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new HashJoin(source(), left, right, leftFields(), rightFields(), addedFields, unique, config().type());
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, HashJoin::new, left(), right(), leftFields(), rightFields(), addedFields, unique, config().type());
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
        HashJoin hashJoin = (HashJoin) o;
        return unique == hashJoin.unique && addedFields.equals(hashJoin.addedFields);
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
