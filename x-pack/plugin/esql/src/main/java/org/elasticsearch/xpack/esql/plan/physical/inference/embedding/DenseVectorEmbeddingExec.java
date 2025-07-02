/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference.embedding;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.InferenceExec;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class DenseVectorEmbeddingExec extends InferenceExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "DenseVectorEmbeddingExec",
        DenseVectorEmbeddingExec::new
    );

    private final Expression input;
    private final Attribute targetField;
    private List<Attribute> lazyOutput;

    public DenseVectorEmbeddingExec(Source source, PhysicalPlan child, Expression inferenceId, Expression input, Attribute targetField) {
        super(source, child, inferenceId);
        this.input = input;
        this.targetField = targetField;
    }

    public DenseVectorEmbeddingExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    public Expression input() {
        return input;
    }

    public Attribute targetField() {
        return targetField;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(targetField);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, DenseVectorEmbeddingExec::new, child(), inferenceId(), input, targetField);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new DenseVectorEmbeddingExec(source(), newChild, inferenceId(), input, targetField);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(List.of(targetField), child().output());
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return input.references();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DenseVectorEmbeddingExec that = (DenseVectorEmbeddingExec) o;
        return Objects.equals(input, that.input) && Objects.equals(targetField, that.targetField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, targetField);
    }
}
