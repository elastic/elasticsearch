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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class EvalExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EvalExec",
        EvalExec::new
    );

    private final List<Alias> fields;

    public EvalExec(Source source, PhysicalPlan child, List<Alias> fields) {
        super(source, child);
        this.fields = fields;
    }

    private EvalExec(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), ((PlanStreamInput) in).readPhysicalPlanNode(), in.readCollectionAsList(Alias::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        ((PlanStreamOutput) out).writePhysicalPlanNode(child());
        out.writeCollection(fields());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public List<Alias> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(fields, child().output());
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new EvalExec(source(), newChild, fields);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EvalExec::new, child(), fields);
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, fields);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EvalExec eval = (EvalExec) o;
        return child().equals(eval.child()) && Objects.equals(fields, eval.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }
}
