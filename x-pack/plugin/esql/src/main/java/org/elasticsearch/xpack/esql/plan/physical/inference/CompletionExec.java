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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CompletionExec extends UnaryExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "CompileExec",
        CompletionExec::new
    );
    private final ReferenceAttribute target;
    private final Expression prompt;
    private final Expression inferenceId;

    public CompletionExec(Source source, PhysicalPlan child, ReferenceAttribute target, Expression prompt, Expression inferenceId) {
        super(source, child);
        this.target = target;
        this.prompt = prompt;
        this.inferenceId = inferenceId;
    }

    public CompletionExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(ReferenceAttribute.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(target());
        out.writeNamedWriteable(prompt());
        out.writeNamedWriteable(inferenceId());
    }

    public NamedExpression target() {
        return target;
    }

    public Expression prompt() {
        return prompt;
    }

    public Expression inferenceId() {
        return inferenceId;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public List<Attribute> output() {
        return NamedExpressions.mergeOutputAttributes(List.of(target), child().output());
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new CompletionExec(source(), newChild, target, prompt, inferenceId);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, CompletionExec::new, child(), target, prompt, inferenceId);
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
        return Objects.equals(target, other.target) && Objects.equals(prompt, other.prompt) && Objects.equals(inferenceId, other.inferenceId);
    }
}
