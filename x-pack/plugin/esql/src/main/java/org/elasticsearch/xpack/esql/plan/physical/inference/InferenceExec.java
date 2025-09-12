/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.io.IOException;
import java.util.Objects;

public abstract class InferenceExec extends UnaryExec {
    private final Expression inferenceId;
    private final TaskType taskType;

    protected InferenceExec(Source source, PhysicalPlan child, Expression inferenceId, TaskType taskType) {
        super(source, child);
        this.inferenceId = inferenceId;
        this.taskType = taskType;
    }

    public Expression inferenceId() {
        return inferenceId;
    }

    public TaskType taskType() {
        return taskType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(inferenceId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        InferenceExec that = (InferenceExec) o;
        return inferenceId.equals(that.inferenceId) && taskType == that.taskType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceId(), taskType);
    }
}
