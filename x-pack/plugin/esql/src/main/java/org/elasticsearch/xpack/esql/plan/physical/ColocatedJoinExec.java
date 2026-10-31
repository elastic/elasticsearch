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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Data-node side of {@link org.elasticsearch.xpack.esql.plan.logical.join.ColocatedJoin}. Shaped like {@link LookupJoinExec}:
 * the left side is a local scan, the right side stays a logical {@link FragmentExec} that the lookup service compiles into
 * per-page queries against the partner shard (same shard number as the left page) on this node.
 * <p>
 * The join is {@code INNER}: the operator emits only the left rows with a match, once per matching right row, so no
 * filter or projection is needed above this node.
 * <p>
 * Only exists on the data node between {@code LocalMapper} and {@code LocalExecutionPlanner}; it is never serialized.
 */
public class ColocatedJoinExec extends BinaryExec implements EstimatesRowSize {
    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    /**
     * Columns contributed by the right side (its output minus the join key). Kept explicitly, as in {@link LookupJoinExec},
     * because the right fragment loses its attributes once planned.
     */
    private final List<Attribute> addedFields;
    private List<Attribute> lazyOutput;

    public ColocatedJoinExec(
        Source source,
        PhysicalPlan left,
        PhysicalPlan right,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields
    ) {
        super(source, left, right);
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.addedFields = addedFields;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<Attribute> leftFields() {
        return leftFields;
    }

    public List<Attribute> rightFields() {
        return rightFields;
    }

    public List<Attribute> addedFields() {
        return addedFields;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = new ArrayList<>(left().output());
            var addedFieldsNames = addedFields.stream().map(Attribute::name).toList();
            lazyOutput.removeIf(a -> addedFieldsNames.contains(a.name()));
            lazyOutput.addAll(addedFields);
        }
        return lazyOutput;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, addedFields, true);
        return this;
    }

    @Override
    public AttributeSet inputSet() {
        // The right side is materialized by the lookup; only the left side feeds this node.
        return left().outputSet();
    }

    @Override
    protected AttributeSet computeReferences() {
        // Only the left keys need field extraction below this node; the right columns come back from the lookup.
        return Expressions.references(leftFields);
    }

    @Override
    public AttributeSet leftReferences() {
        return Expressions.references(leftFields);
    }

    @Override
    public AttributeSet rightReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public ColocatedJoinExec replaceChildren(PhysicalPlan left, PhysicalPlan right) {
        return new ColocatedJoinExec(source(), left, right, leftFields, rightFields, addedFields);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ColocatedJoinExec::new, left(), right(), leftFields, rightFields, addedFields);
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
        ColocatedJoinExec other = (ColocatedJoinExec) o;
        return leftFields.equals(other.leftFields) && rightFields.equals(other.rightFields) && addedFields.equals(other.addedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), leftFields, rightFields, addedFields);
    }
}
