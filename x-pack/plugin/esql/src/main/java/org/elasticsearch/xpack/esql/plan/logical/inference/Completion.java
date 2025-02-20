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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Completion extends InferencePlan implements GeneratingPlan<Completion> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Completion",
        Completion::new
    );

    private final Expression prompt;
    private final ReferenceAttribute target;

    public Completion(Source source, LogicalPlan child, String inferenceId, Expression prompt, ReferenceAttribute target) {
        super(source, child, inferenceId);
        this.prompt = prompt;
        this.target = target;
    }

    public Completion(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readString(),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(ReferenceAttribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(prompt);
        out.writeNamedWriteable(target);
    }

    @Override
    public Completion withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        return new Completion(
            source(),
            child(),
            inferenceId(),
            prompt,
            new ReferenceAttribute(Source.EMPTY, newNames.getFirst(), DataType.KEYWORD)
        );
    }

    public ReferenceAttribute target() {
        return target;
    }

    public Expression prompt() {
        return prompt;
    }

    @Override
    public List<Attribute> output() {
        return NamedExpressions.mergeOutputAttributes(List.of(target), child().output());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Completion(source(), newChild, inferenceId(), prompt, target);
    }

    @Override
    public boolean expressionsResolved() {
        return prompt.resolved() && target.resolved();
    }

    @Override
    protected AttributeSet computeReferences() {
        return prompt.references();
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return List.of(target);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Completion::new, child(), inferenceId(), prompt, target);
    }

    @Override
    public TaskType taskType() {
        return TaskType.COMPLETION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Completion other = (Completion) o;
        return Objects.equals(prompt, other.prompt) && Objects.equals(target, other.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prompt, target);
    }
}
