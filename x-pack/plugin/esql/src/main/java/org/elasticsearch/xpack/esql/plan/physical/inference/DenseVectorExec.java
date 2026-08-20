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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Physical counterpart of {@link org.elasticsearch.xpack.esql.plan.logical.inference.DenseVector}: embeds each input field
 * into a generated {@code <field>_dense_vector} column. Carries a 1:1 aligned list of input {@link #fields} and generated
 * {@link #generatedFields}; the planner turns each pair into a {@code TextEmbeddingOperator}.
 */
public class DenseVectorExec extends InferenceExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "DenseVectorExec",
        DenseVectorExec::new
    );

    private final List<NamedExpression> fields;
    private final List<Attribute> generatedFields;
    private final TimeValue timeout;
    private final org.elasticsearch.inference.DataType inputType;
    private final TaskType endpointTaskType;
    private List<Attribute> lazyOutput;

    public DenseVectorExec(
        Source source,
        PhysicalPlan child,
        Expression inferenceId,
        List<NamedExpression> fields,
        List<Attribute> generatedFields,
        TimeValue timeout,
        org.elasticsearch.inference.DataType inputType,
        TaskType endpointTaskType
    ) {
        super(source, child, inferenceId);
        this.fields = fields;
        this.generatedFields = generatedFields;
        this.timeout = timeout;
        this.inputType = inputType;
        this.endpointTaskType = endpointTaskType;
    }

    public DenseVectorExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.getTransportVersion().supports(InferencePlan.ESQL_INFERENCE_ACCEPT_TIMEOUT) ? in.readOptionalTimeValue() : null,
            in.getTransportVersion().supports(InferencePlan.ESQL_DENSE_VECTOR_TYPE_OPTION)
                ? org.elasticsearch.inference.DataType.fromString(in.readString())
                : org.elasticsearch.inference.DataType.TEXT,
            in.getTransportVersion().supports(InferencePlan.ESQL_DENSE_VECTOR_TYPE_OPTION) && in.readBoolean()
                ? TaskType.fromStream(in)
                : null
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteableCollection(fields);
        out.writeNamedWriteableCollection(generatedFields);
        if (out.getTransportVersion().supports(InferencePlan.ESQL_INFERENCE_ACCEPT_TIMEOUT)) {
            out.writeOptionalTimeValue(timeout);
        }
        if (out.getTransportVersion().supports(InferencePlan.ESQL_DENSE_VECTOR_TYPE_OPTION)) {
            out.writeString(inputType.name());
            if (endpointTaskType == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                endpointTaskType.writeTo(out);
            }
        }
    }

    public List<NamedExpression> fields() {
        return fields;
    }

    public List<Attribute> generatedFields() {
        return generatedFields;
    }

    public TimeValue timeout() {
        return timeout;
    }

    /** Input modality selected by the {@code type} option. */
    public org.elasticsearch.inference.DataType inputType() {
        return inputType;
    }

    /** Task type of the resolved inference endpoint, used to route to the matching request shape. */
    public TaskType endpointTaskType() {
        return endpointTaskType;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            DenseVectorExec::new,
            child(),
            inferenceId(),
            fields,
            generatedFields,
            timeout,
            inputType,
            endpointTaskType
        );
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new DenseVectorExec(source(), newChild, inferenceId(), fields, generatedFields, timeout, inputType, endpointTaskType);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(generatedFields, child().output());
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return Expressions.references(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DenseVectorExec other = (DenseVectorExec) o;
        return Objects.equals(fields, other.fields)
            && Objects.equals(generatedFields, other.generatedFields)
            && Objects.equals(timeout, other.timeout)
            && inputType == other.inputType
            && endpointTaskType == other.endpointTaskType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields, generatedFields, timeout, inputType, endpointTaskType);
    }
}
