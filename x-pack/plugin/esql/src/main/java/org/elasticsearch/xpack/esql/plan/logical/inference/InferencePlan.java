/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.RowLimited;
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
        ExecutesOn.Coordinator,
        RowLimited<PlanType> {

    public static final TransportVersion ESQL_INFERENCE_USAGE_LIMIT = TransportVersion.fromName("esql_inference_usage_limit");

    public static final String INFERENCE_ID_OPTION_NAME = "inference_id";
    public static final List<String> VALID_INFERENCE_OPTION_NAMES = List.of(INFERENCE_ID_OPTION_NAME);

    private final Expression inferenceId;
    private final Expression rowLimit;

    protected InferencePlan(Source source, LogicalPlan child, Expression inferenceId, Expression rowLimit) {
        super(source, child);
        this.inferenceId = inferenceId;
        this.rowLimit = rowLimit;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(inferenceId());

        if (out.getTransportVersion().supports(ESQL_INFERENCE_USAGE_LIMIT)) {
            out.writeNamedWriteable(rowLimit());
        }
    }

    public Expression inferenceId() {
        return inferenceId;
    }

    public Expression rowLimit() {
        return rowLimit;
    }

    @Override
    public int maxRows() {
        return Foldables.intValueOf(rowLimit, rowLimit.sourceText(), "row limit");
    }

    @Override
    public LogicalPlan surrogate() {
        return this;
    }

    @Override
    public boolean expressionsResolved() {
        return inferenceId.resolved() && rowLimit.resolved();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        InferencePlan<?> other = (InferencePlan<?>) o;
        return Objects.equals(inferenceId, other.inferenceId) && Objects.equals(rowLimit, other.rowLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceId(), rowLimit());
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
