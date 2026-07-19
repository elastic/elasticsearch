/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Coordinator-only INNER equi-join over a materialized "build" side, used to implement PromQL
 * vector matching. It is an ephemeral node: it never crosses the wire and is expanded into a chain
 * of existing operators by {@link org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner#planEqJoin}
 * (row lookup + optional 1:1 uniqueness guard + inner-drop filter + column load), so it is not
 * serialized (see {@link #writeTo}).
 * <p>
 * Shape mirrors {@link HashJoinExec} so translation can later map a logical {@code EqJoin} onto it
 * exactly as {@code Join} maps onto {@link HashJoinExec}. The extra {@link #unique} flag selects 1:1
 * ({@code true}: enforce at-most-one probe row per build row) vs many-to-one ({@code false}:
 * {@code group_left}/{@code group_right}, fan-out allowed).
 */
public class EqJoinExec extends BinaryExec implements EstimatesRowSize {

    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    private final List<Attribute> addedFields;
    private final boolean unique;
    private List<Attribute> lazyOutput;
    private AttributeSet lazyAddedFields;

    public EqJoinExec(
        Source source,
        PhysicalPlan left,
        PhysicalPlan joinData,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields,
        boolean unique
    ) {
        super(source, left, joinData);
        this.leftFields = leftFields;
        this.rightFields = rightFields;
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

    public PhysicalPlan joinData() {
        return right();
    }

    public List<Attribute> leftFields() {
        return leftFields;
    }

    public List<Attribute> rightFields() {
        return rightFields;
    }

    public Set<Attribute> addedFields() {
        if (lazyAddedFields == null) {
            lazyAddedFields = AttributeSet.of(addedFields);
        }
        return lazyAddedFields;
    }

    /**
     * {@code true} for 1:1 matching (enforce at-most-one probe row per build row); {@code false} for
     * many-to-one ({@code group_left}/{@code group_right}).
     */
    public boolean unique() {
        return unique;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, addedFields);
        return this;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> leftOutputWithoutKeys = left().output().stream().filter(attr -> leftFields.contains(attr) == false).toList();
            List<Attribute> rightWithAppendedKeys = new ArrayList<>(right().output());
            rightWithAppendedKeys.removeAll(rightFields);
            rightWithAppendedKeys.addAll(leftFields);

            lazyOutput = mergeOutputAttributes(rightWithAppendedKeys, leftOutputWithoutKeys);
        }
        return lazyOutput;
    }

    @Override
    public AttributeSet inputSet() {
        // The build side is materialized; only the probe (left) side flows in.
        return left().outputSet();
    }

    @Override
    protected AttributeSet computeReferences() {
        return Expressions.references(leftFields);
    }

    @Override
    public AttributeSet leftReferences() {
        return Expressions.references(leftFields);
    }

    @Override
    public AttributeSet rightReferences() {
        return Expressions.references(rightFields);
    }

    @Override
    public EqJoinExec replaceChildren(PhysicalPlan left, PhysicalPlan right) {
        return new EqJoinExec(source(), left, right, leftFields, rightFields, addedFields, unique);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EqJoinExec::new, left(), right(), leftFields, rightFields, addedFields, unique);
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
        EqJoinExec eqJoin = (EqJoinExec) o;
        return unique == eqJoin.unique
            && leftFields.equals(eqJoin.leftFields)
            && rightFields.equals(eqJoin.rightFields)
            && addedFields.equals(eqJoin.addedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), leftFields, rightFields, addedFields, unique);
    }
}
