/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CompletionExec extends UnaryExec implements EstimatesRowSize {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "CompileExec",
        CompletionExec::new
    );
    private final ReferenceAttribute target;
    private final Expression prompt;
    private final String inferenceId;

    public CompletionExec(Source source, PhysicalPlan child, String inferenceId, Expression prompt, ReferenceAttribute target) {
        super(source, child);
        this.target = target;
        this.prompt = prompt;
        this.inferenceId = inferenceId;
    }

    public CompletionExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readString(),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(ReferenceAttribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(inferenceId);
        out.writeNamedWriteable(prompt());
        out.writeNamedWriteable(target());
    }

    public NamedExpression target() {
        return target;
    }

    public Expression prompt() {
        return prompt;
    }

    public String inferenceId() {
        return inferenceId;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected AttributeSet computeReferences() {
        return prompt.references();
    }

    @Override
    public List<Attribute> output() {
        return NamedExpressions.mergeOutputAttributes(List.of(target), child().output());
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new CompletionExec(source(), newChild, inferenceId, prompt, target);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, CompletionExec::new, child(), inferenceId, prompt, target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), target, prompt, inferenceId);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        CompletionExec other = ((CompletionExec) obj);
        return Objects.equals(target, other.target)
            && Objects.equals(prompt, other.prompt)
            && Objects.equals(inferenceId, other.inferenceId);
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, List.of(target));
        return this;
    }
}
