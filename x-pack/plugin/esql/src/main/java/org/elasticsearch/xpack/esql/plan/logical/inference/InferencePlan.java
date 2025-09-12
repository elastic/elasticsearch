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
import org.elasticsearch.xpack.esql.inference.ResolvedInference;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public abstract class InferencePlan<PlanType extends InferencePlan<PlanType>> extends UnaryPlan
    implements
        SortAgnostic,
        GeneratingPlan<InferencePlan<PlanType>>,
        ExecutesOn.Coordinator {

    public static final String INFERENCE_ID_OPTION_NAME = "inference_id";
    public static final List<String> VALID_INFERENCE_OPTION_NAMES = List.of(INFERENCE_ID_OPTION_NAME);

    private final Expression inferenceId;
    private final TaskType taskType;

    protected InferencePlan(Source source, LogicalPlan child, Expression inferenceId, TaskType taskType) {
        super(source, child);
        this.inferenceId = inferenceId;
        this.taskType = taskType;
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
        return Objects.equals(inferenceId(), other.inferenceId()) && taskType == other.taskType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceId(), taskType);
    }

    public TaskType taskType() {
        return taskType;
    }

    public abstract PlanType withInferenceId(Expression newInferenceId);

    public abstract List<TaskType> supportedTaskTypes();

    @SuppressWarnings("unchecked")
    public PlanType withResolvedInference(ResolvedInference resolvedInference) {
        if (supportedTaskTypes().stream().noneMatch(resolvedInference.taskType()::equals)) {
            String error = "cannot use inference endpoint ["
                + resolvedInference.inferenceId()
                + "] with task type ["
                + resolvedInference.taskType()
                + "] within a "
                + nodeName()
                + " command. Only inference endpoints with the task type "
                + supportedTaskTypes()
                + " are supported.";
            return withInferenceResolutionError(resolvedInference.inferenceId(), error);
        }

        return (PlanType) this;
    }

    public PlanType withInferenceResolutionError(String inferenceId, String error) {
        return withInferenceId(new UnresolvedAttribute(inferenceId().source(), inferenceId, error));
    }

    public List<String> validOptionNames() {
        return VALID_INFERENCE_OPTION_NAMES;
    }
}
