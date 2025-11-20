/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;
import org.elasticsearch.xpack.esql.plan.logical.Streaming;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public abstract class InferencePlan<PlanType extends InferencePlan<PlanType>> extends UnaryPlan
    implements
        Streaming,
        SortAgnostic,
        GeneratingPlan<InferencePlan<PlanType>>,
        ExecutesOn.Coordinator {

    public static final String INFERENCE_ID_OPTION_NAME = "inference_id";
    public static final List<String> VALID_INFERENCE_OPTION_NAMES = List.of(INFERENCE_ID_OPTION_NAME);

    private final Expression inferenceId;

    protected InferencePlan(Source source, LogicalPlan child, Expression inferenceId) {
        super(source, child);
        this.inferenceId = inferenceId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(inferenceId());
    }

    public Expression inferenceId() {
        return inferenceId;
    }

    @Override
    public boolean expressionsResolved() {
        return inferenceId.resolved();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        InferencePlan<?> other = (InferencePlan<?>) o;
        return Objects.equals(inferenceId(), other.inferenceId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceId());
    }

    public abstract TaskType taskType();

    public abstract PlanType withInferenceId(Expression newInferenceId);

    public PlanType withInferenceResolutionError(String inferenceId, String error) {
        return withInferenceId(new UnresolvedAttribute(inferenceId().source(), inferenceId, error));
    }

    public List<String> validOptionNames() {
        return VALID_INFERENCE_OPTION_NAMES;
    }

    /**
     * Checks if this InferencePlan is foldable (all input expressions are foldable).
     * A plan is foldable if all its input expressions can be evaluated statically.
     */
    public abstract boolean isFoldable();
}
