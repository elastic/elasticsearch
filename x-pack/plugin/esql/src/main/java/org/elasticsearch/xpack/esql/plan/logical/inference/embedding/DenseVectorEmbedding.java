/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference.embedding;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class DenseVectorEmbedding extends InferencePlan<DenseVectorEmbedding> implements TelemetryAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "DenseVectorEmbedding",
        DenseVectorEmbedding::new
    );

    private final Expression input;
    private final Attribute targetField;
    private List<Attribute> lazyOutput;

    public DenseVectorEmbedding(Source source, LogicalPlan child, Expression inferenceId, Expression input, Attribute targetField) {
        super(source, child, inferenceId);
        this.input = input;
        this.targetField = targetField;
    }

    public DenseVectorEmbedding(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(inferenceId());
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(targetField);
    }

    public Expression input() {
        return input;
    }

    public Attribute embeddingField() {
        return targetField;
    }

    @Override
    public TaskType taskType() {
        return TaskType.TEXT_EMBEDDING;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(List.of(targetField), child().output());
        }
        return lazyOutput;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return List.of(targetField);
    }

    @Override
    public DenseVectorEmbedding withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        return new DenseVectorEmbedding(source(), child(), inferenceId(), input, this.renameTargetField(newNames.get(0)));
    }

    private Attribute renameTargetField(String newName) {
        if (newName.equals(targetField.name())) {
            return targetField;
        }

        return targetField.withName(newName).withId(new NameId());
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && input.resolved() && targetField.resolved();
    }

    @Override
    public DenseVectorEmbedding withInferenceId(Expression newInferenceId) {
        return new DenseVectorEmbedding(source(), child(), newInferenceId, input, targetField);
    }

    @Override
    public DenseVectorEmbedding replaceChild(LogicalPlan newChild) {
        return new DenseVectorEmbedding(source(), newChild, inferenceId(), input, targetField);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, DenseVectorEmbedding::new, child(), inferenceId(), input, targetField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DenseVectorEmbedding that = (DenseVectorEmbedding) o;
        return Objects.equals(input, that.input) && Objects.equals(targetField, that.targetField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, targetField);
    }
}
