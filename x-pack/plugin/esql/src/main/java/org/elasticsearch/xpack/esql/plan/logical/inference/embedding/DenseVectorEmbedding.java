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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
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
    private final Expression dimensions;
    private final Attribute targetField;
    private List<Attribute> lazyOutput;

    public DenseVectorEmbedding(Source source, LogicalPlan child, Expression inferenceId, Expression input, Attribute targetField) {
        this(source, child, inferenceId, new UnresolvedDimensions(inferenceId), input, targetField);
    }

    DenseVectorEmbedding(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        Expression dimensions,
        Expression input,
        Attribute targetField
    ) {
        super(source, child, inferenceId);
        this.input = input;
        this.targetField = targetField;
        this.dimensions = dimensions;
    }

    public DenseVectorEmbedding(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
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
        out.writeNamedWriteable(dimensions);
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

    public Expression dimensions() {
        return dimensions;
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
        return new DenseVectorEmbedding(source(), child(), inferenceId(), dimensions, input, this.renameTargetField(newNames.get(0)));
    }

    private Attribute renameTargetField(String newName) {
        if (newName.equals(targetField.name())) {
            return targetField;
        }

        return targetField.withName(newName).withId(new NameId());
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && input.resolved() && targetField.resolved() && dimensions.resolved();
    }

    @Override
    public DenseVectorEmbedding withInferenceId(Expression newInferenceId) {
        return new DenseVectorEmbedding(source(), child(), newInferenceId, dimensions, input, targetField);
    }

    public DenseVectorEmbedding withDimensions(Expression newDimensions) {
        return new DenseVectorEmbedding(source(), child(), inferenceId(), newDimensions, input, targetField);
    }

    public DenseVectorEmbedding withTargetField(Attribute targetField) {
        return new DenseVectorEmbedding(source(), child(), inferenceId(), dimensions, input, targetField);
    }

    @Override
    public DenseVectorEmbedding withModelConfigurations(ModelConfigurations modelConfig) {
        boolean hasChanged = false;
        Expression newDimensions = dimensions;

        if (dimensions.resolved() == false
            && modelConfig.getServiceSettings() != null
            && modelConfig.getServiceSettings().dimensions() > 0) {
            hasChanged = true;
            newDimensions = new Literal(Source.EMPTY, modelConfig.getServiceSettings().dimensions(), DataType.INTEGER);
        }

        return hasChanged ? withDimensions(newDimensions) : this;
    }

    @Override
    public DenseVectorEmbedding replaceChild(LogicalPlan newChild) {
        return new DenseVectorEmbedding(source(), newChild, inferenceId(), dimensions, input, targetField);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, DenseVectorEmbedding::new, child(), inferenceId(), dimensions, input, targetField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DenseVectorEmbedding that = (DenseVectorEmbedding) o;
        return Objects.equals(input, that.input)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(targetField, that.targetField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, targetField, dimensions);
    }

    private static class UnresolvedDimensions extends Literal implements Unresolvable {

        private final String inferenceId;

        private UnresolvedDimensions(Expression inferenceId) {
            super(Source.EMPTY, null, DataType.NULL);
            this.inferenceId = BytesRefs.toString(inferenceId.fold(FoldContext.small()));
        }

        @Override
        public String unresolvedMessage() {
            return "Dimensions cannot be resolved for inference endpoint[" + inferenceId + "]";
        }
    }
}
