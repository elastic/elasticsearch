/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class HashJoinExec extends UnaryExec implements EstimatesRowSize {
    private final LocalSourceExec joinData;
    private final List<Attribute> matchFields;
    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    private final List<Attribute> output;
    private AttributeSet lazyAddedFields;

    public HashJoinExec(
        Source source,
        PhysicalPlan child,
        LocalSourceExec hashData,
        List<Attribute> matchFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        List<Attribute> output
    ) {
        super(source, child);
        this.joinData = hashData;
        this.matchFields = matchFields;
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.output = output;
    }

    public HashJoinExec(PlanStreamInput in) throws IOException {
        super(Source.readFrom(in), in.readPhysicalPlanNode());
        this.joinData = new LocalSourceExec(in);
        this.matchFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.leftFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.rightFields = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.output = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    public void writeTo(PlanStreamOutput out) throws IOException {
        source().writeTo(out);
        out.writePhysicalPlanNode(child());
        joinData.writeTo(out);
        out.writeNamedWriteableCollection(matchFields);
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
        out.writeNamedWriteableCollection(output);
    }

    public LocalSourceExec joinData() {
        return joinData;
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
            lazyAddedFields = outputSet();
            lazyAddedFields.removeAll(child().output());
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
    public HashJoinExec replaceChild(PhysicalPlan newChild) {
        return new HashJoinExec(source(), newChild, joinData, matchFields, leftFields, rightFields, output);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, HashJoinExec::new, child(), joinData, matchFields, leftFields, rightFields, output);
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
        return joinData.equals(hash.joinData)
            && matchFields.equals(hash.matchFields)
            && leftFields.equals(hash.leftFields)
            && rightFields.equals(hash.rightFields)
            && output.equals(hash.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinData, matchFields, leftFields, rightFields, output);
    }
}
