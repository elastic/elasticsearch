/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Joins probe and build inputs that have been partitioned by the same key.
 */
public class DistributedHashJoinExec extends BinaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "DistributedHashJoinExec",
        DistributedHashJoinExec::new
    );

    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    private final List<Attribute> addedFields;
    private List<Attribute> lazyOutput;

    /**
     * Creates a join over co-partitioned probe and build inputs.
     */
    public DistributedHashJoinExec(
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

    private DistributedHashJoinExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readNamedWriteable(PhysicalPlan.class));
        this.leftFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.rightFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.addedFields = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
        out.writeNamedWriteableCollection(addedFields);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
            List<String> addedFieldNames = addedFields.stream().map(Attribute::name).toList();
            lazyOutput.removeIf(attribute -> addedFieldNames.contains(attribute.name()));
            lazyOutput.addAll(addedFields);
        }
        return lazyOutput;
    }

    @Override
    public AttributeSet inputSet() {
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
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, addedFields);
        return this;
    }

    @Override
    public DistributedHashJoinExec replaceChildren(PhysicalPlan left, PhysicalPlan right) {
        return new DistributedHashJoinExec(source(), left, right, leftFields, rightFields, addedFields);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, DistributedHashJoinExec::new, left(), right(), leftFields, rightFields, addedFields);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (super.equals(obj) == false) {
            return false;
        }
        DistributedHashJoinExec other = (DistributedHashJoinExec) obj;
        return leftFields.equals(other.leftFields) && rightFields.equals(other.rightFields) && addedFields.equals(other.addedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), leftFields, rightFields, addedFields);
    }
}
