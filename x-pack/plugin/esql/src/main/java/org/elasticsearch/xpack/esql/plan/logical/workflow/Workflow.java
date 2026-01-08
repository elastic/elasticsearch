/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.workflow;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.Streaming;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Logical plan for the WORKFLOW command.
 * Executes a Kibana workflow synchronously and returns its output as a JSON column.
 *
 * Syntax: WORKFLOW "workflow_id" WITH (input1 = expr1, input2 = expr2, ...) AS target [ON ERROR NULL|FAIL]
 */
public class Workflow extends UnaryPlan
    implements
        Streaming,
        SortAgnostic,
        SortPreserving,
        GeneratingPlan<Workflow>,
        ExecutesOn.Coordinator,
        SurrogateLogicalPlan,
        TelemetryAware,
        PostAnalysisVerificationAware {

    public static final String DEFAULT_OUTPUT_FIELD_NAME = "workflow";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Workflow", Workflow::new);

    /**
     * Error handling mode for workflow execution.
     */
    public enum ErrorHandling {
        /** Fail the entire query if workflow execution fails */
        FAIL,
        /** Return NULL for rows where workflow execution fails */
        NULL
    }

    private final Expression workflowId;
    private final List<Alias> inputs;
    private final Attribute targetField;
    private final Expression rowLimit;
    private final ErrorHandling errorHandling;

    private List<Attribute> lazyOutput;

    public Workflow(
        Source source,
        LogicalPlan child,
        Expression workflowId,
        List<Alias> inputs,
        Attribute targetField,
        Expression rowLimit,
        ErrorHandling errorHandling
    ) {
        super(source, child);
        this.workflowId = workflowId;
        this.inputs = inputs;
        this.targetField = targetField;
        this.rowLimit = rowLimit;
        this.errorHandling = errorHandling;
    }

    public Workflow(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Alias.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Expression.class),
            in.readEnum(ErrorHandling.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(workflowId);
        out.writeNamedWriteableCollection(inputs);
        out.writeNamedWriteable(targetField);
        out.writeNamedWriteable(rowLimit);
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

    public Expression rowLimit() {
        return rowLimit;
    }

    public ErrorHandling errorHandling() {
        return errorHandling;
    }

    @Override
    public Workflow replaceChild(LogicalPlan newChild) {
        return new Workflow(source(), newChild, workflowId, inputs, targetField, rowLimit, errorHandling);
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
    public Workflow withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        return new Workflow(source(), child(), workflowId, inputs, renameTargetField(newNames.get(0)), rowLimit, errorHandling);
    }

    private Attribute renameTargetField(String newName) {
        if (newName.equals(targetField.name())) {
            return targetField;
        }
        return targetField.withName(newName).withId(new NameId());
    }

    @Override
    protected AttributeSet computeReferences() {
        // Collect references from all input expressions
        return Expressions.references(inputs);
    }

    @Override
    public boolean expressionsResolved() {
        if (workflowId.resolved() == false || targetField.resolved() == false || rowLimit.resolved() == false) {
            return false;
        }
        for (Alias input : inputs) {
            if (input.resolved() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public LogicalPlan surrogate() {
        // Insert implicit LIMIT before workflow execution for safety
        return this.replaceChild(new Limit(Source.EMPTY, rowLimit, child()));
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        // Validate workflowId is a string
        if (workflowId.resolved() && DataType.isString(workflowId.dataType()) == false) {
            failures.add(fail(workflowId, "workflow_id must be of type [keyword] or [text] but is [{}]", workflowId.dataType().typeName()));
        }

        // Validate all input values can be serialized to JSON (representable types)
        for (Alias input : inputs) {
            Expression value = input.child();
            if (value.resolved() && DataType.isRepresentable(value.dataType()) == false) {
                failures.add(fail(value, "workflow input [{}] has unsupported type [{}]", input.name(), value.dataType().typeName()));
            }
        }
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Workflow::new, child(), workflowId, inputs, targetField, rowLimit, errorHandling);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Workflow workflow = (Workflow) o;
        return Objects.equals(workflowId, workflow.workflowId)
            && Objects.equals(inputs, workflow.inputs)
            && Objects.equals(targetField, workflow.targetField)
            && Objects.equals(rowLimit, workflow.rowLimit)
            && errorHandling == workflow.errorHandling;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), workflowId, inputs, targetField, rowLimit, errorHandling);
    }
}
