/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.workflow;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.workflow.Workflow;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Physical plan for the WORKFLOW command.
 * Executes a Kibana workflow synchronously via HTTP and returns its output as a JSON column.
 */
public class WorkflowExec extends UnaryExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "WorkflowExec",
        WorkflowExec::new
    );

    private final Expression workflowId;
    private final List<Alias> inputs;
    private final Attribute targetField;
    private final Workflow.ErrorHandling errorHandling;

    private List<Attribute> lazyOutput;

    public WorkflowExec(
        Source source,
        PhysicalPlan child,
        Expression workflowId,
        List<Alias> inputs,
        Attribute targetField,
        Workflow.ErrorHandling errorHandling
    ) {
        super(source, child);
        this.workflowId = workflowId;
        this.inputs = inputs;
        this.targetField = targetField;
        this.errorHandling = errorHandling;
    }

    public WorkflowExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Alias.class),
            in.readNamedWriteable(Attribute.class),
            in.readEnum(Workflow.ErrorHandling.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(workflowId);
        out.writeNamedWriteableCollection(inputs);
        out.writeNamedWriteable(targetField);
        out.writeEnum(errorHandling);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression workflowId() {
        return workflowId;
    }

    public List<Alias> inputs() {
        return inputs;
    }

    public Attribute targetField() {
        return targetField;
    }

    public Workflow.ErrorHandling errorHandling() {
        return errorHandling;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, WorkflowExec::new, child(), workflowId, inputs, targetField, errorHandling);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new WorkflowExec(source(), newChild, workflowId, inputs, targetField, errorHandling);
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
        // Collect references from all input expressions
        return Expressions.references(inputs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        WorkflowExec that = (WorkflowExec) o;
        return Objects.equals(workflowId, that.workflowId)
            && Objects.equals(inputs, that.inputs)
            && Objects.equals(targetField, that.targetField)
            && errorHandling == that.errorHandling;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), workflowId, inputs, targetField, errorHandling);
    }
}
