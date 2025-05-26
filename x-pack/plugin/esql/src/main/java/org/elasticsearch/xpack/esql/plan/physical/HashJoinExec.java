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
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class HashJoinExec extends BinaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "HashJoinExec",
        HashJoinExec::new
    );

    private final List<Attribute> matchFields;
    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    private final List<Attribute> addedFields;
    private List<Attribute> lazyOutput;
    private AttributeSet lazyAddedFields;

    public HashJoinExec(
        Source source,
        PhysicalPlan left,
        PhysicalPlan hashData,
        List<Attribute> matchFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> addedFields
    ) {
        super(source, left, hashData);
        this.matchFields = matchFields;
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.addedFields = addedFields;
    }

    private HashJoinExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readNamedWriteable(PhysicalPlan.class));
        this.matchFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.leftFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.rightFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.addedFields = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteableCollection(matchFields);
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
        out.writeNamedWriteableCollection(addedFields);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public PhysicalPlan joinData() {
        return right();
    }

    public List<Attribute> matchFields() {
        return matchFields;
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
        // TODO: this is a hack until qualifiers land since the right side is always materialized
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
    public HashJoinExec replaceChildren(PhysicalPlan left, PhysicalPlan right) {
        return new HashJoinExec(source(), left, right, matchFields, leftFields, rightFields, addedFields);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, HashJoinExec::new, left(), right(), matchFields, leftFields, rightFields, addedFields);
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
        HashJoinExec hash = (HashJoinExec) o;
        return matchFields.equals(hash.matchFields)
            && leftFields.equals(hash.leftFields)
            && rightFields.equals(hash.rightFields)
            && addedFields.equals(hash.addedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), matchFields, leftFields, rightFields, addedFields);
    }
}
