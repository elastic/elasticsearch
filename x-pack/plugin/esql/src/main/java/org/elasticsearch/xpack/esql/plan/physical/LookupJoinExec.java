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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class LookupJoinExec extends BinaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "LookupJoinExec",
        LookupJoinExec::new
    );

    private final List<Attribute> matchFields;
    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    private final List<Attribute> output;
    private List<Attribute> lazyAddedFields;

    public LookupJoinExec(
        Source source,
        PhysicalPlan left,
        PhysicalPlan lookup,
        List<Attribute> matchFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> output
    ) {
        super(source, left, lookup);
        this.matchFields = matchFields;
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.output = output;
    }

    private LookupJoinExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readNamedWriteable(PhysicalPlan.class));
        this.matchFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.leftFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.rightFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.output = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteableCollection(matchFields);
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
        out.writeNamedWriteableCollection(output);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public PhysicalPlan lookup() {
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

    public List<Attribute> addedFields() {
        if (lazyAddedFields == null) {
            AttributeSet set = outputSet();
            set.removeAll(left().output());
            for (Attribute m : matchFields) {
                set.removeIf(a -> a.name().equals(m.name()));
            }
            lazyAddedFields = new ArrayList<>(set);
            lazyAddedFields.sort(Comparator.comparing(Attribute::name));
        }
        return lazyAddedFields;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, output);
        return this;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public AttributeSet inputSet() {
        // TODO: this is a hack since the right side is always materialized - instead this should
        // return the _doc so the extraction can happen lazily
        return left().outputSet();
    }

    @Override
    protected AttributeSet computeReferences() {
        // TODO: same as above - once lazy materialization of both sides lands, this needs updating
        return Expressions.references(leftFields);
    }

    @Override
    public LookupJoinExec replaceChildren(PhysicalPlan left, PhysicalPlan right) {
        return new LookupJoinExec(source(), left, right, matchFields, leftFields, rightFields, output);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, LookupJoinExec::new, left(), right(), matchFields, leftFields, rightFields, output);
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
        LookupJoinExec hash = (LookupJoinExec) o;
        return matchFields.equals(hash.matchFields)
            && leftFields.equals(hash.leftFields)
            && rightFields.equals(hash.rightFields)
            && output.equals(hash.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), matchFields, leftFields, rightFields, output);
    }
}
