/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Completion extends UnaryPlan {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Completion", Completion::new);

    private final NamedExpression target;

    private final Expression prompt;

    private final Expression inferenceId;

    public Completion(Source source, LogicalPlan child, NamedExpression target, Expression prompt, Expression inferenceId) {
        super(source, child);
        this.target = target;
        this.prompt = prompt;
        this.inferenceId = inferenceId;
    }

    public Completion(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(NamedExpression.class),
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), target, prompt, inferenceId);
    }

    @Override
    public String commandName() {
        return "COMPLETION";
    }

    @Override
    public boolean expressionsResolved() {
        return target.resolved() && prompt.resolved() && inferenceId.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Completion(source(), newChild, target, prompt, inferenceId);
    }

    @Override
    public List<Attribute> output() {
        return NamedExpressions.mergeOutputAttributes(List.of( new ReferenceAttribute(Source.EMPTY, target.name(), DataType.TEXT)), child().output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Completion::new, child(), target, prompt, inferenceId);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        Completion other = ((Completion) obj);
        return Objects.equals(target, other.target) && Objects.equals(prompt, other.prompt) && Objects.equals(inferenceId, other.inferenceId);
    }
}
