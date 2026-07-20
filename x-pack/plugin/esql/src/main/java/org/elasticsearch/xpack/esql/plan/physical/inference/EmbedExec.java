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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
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

public class EmbedExec extends InferenceExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EmbedExec",
        EmbedExec::new
    );

    private final Expression input;
    private final Attribute targetField;
    private final TimeValue timeout;
    private List<Attribute> lazyOutput;

    public EmbedExec(Source source, PhysicalPlan child, Expression inferenceId, Expression input, Attribute targetField) {
        this(source, child, inferenceId, input, targetField, null);
    }

    public EmbedExec(
        Source source,
        PhysicalPlan child,
        Expression inferenceId,
        Expression input,
        Attribute targetField,
        TimeValue timeout
    ) {
        super(source, child, inferenceId);
        this.input = input;
        this.targetField = targetField;
        this.timeout = timeout;
    }

    public EmbedExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class),
            in.getTransportVersion().supports(InferencePlan.ESQL_INFERENCE_ACCEPT_TIMEOUT) ? in.readOptionalTimeValue() : null
        );
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
        if (out.getTransportVersion().supports(InferencePlan.ESQL_INFERENCE_ACCEPT_TIMEOUT)) {
            out.writeOptionalTimeValue(timeout);
        }
    }

    public Expression input() {
        return input;
    }

    public Attribute targetField() {
        return targetField;
    }

    public TimeValue timeout() {
        return timeout;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EmbedExec::new, child(), inferenceId(), input, targetField, timeout);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new EmbedExec(source(), newChild, inferenceId(), input, targetField, timeout);
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
        EmbedExec embed = (EmbedExec) o;

        return Objects.equals(input, embed.input)
            && Objects.equals(targetField, embed.targetField)
            && Objects.equals(timeout, embed.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, targetField, timeout);
    }
}
