/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.inference.ResolvedInference;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Completion extends InferencePlan<Completion> implements TelemetryAware, PostAnalysisVerificationAware {

    public static final String DEFAULT_OUTPUT_FIELD_NAME = "completion";

    public static final List<TaskType> SUPPORTED_TASK_TYPES = List.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Completion",
        Completion::new
    );
    private final Expression prompt;
    private final Attribute targetField;
    private List<Attribute> lazyOutput;

    public Completion(Source source, LogicalPlan p, Expression prompt, Attribute targetField) {
        this(source, p, Literal.NULL, null, prompt, targetField);
    }

    public Completion(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        TaskType taskType,
        Expression prompt,
        Attribute targetField
    ) {
        super(source, child, inferenceId, taskType);
        this.prompt = prompt;
        this.targetField = targetField;
    }

    public Completion(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_CHAT_COMPLETION_SUPPORT)
                ? in.readOptional(input -> TaskType.fromString(input.readString()))
                : TaskType.COMPLETION,
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_CHAT_COMPLETION_SUPPORT)) {
            out.writeOptional((output, taskType) -> output.writeString(taskType.toString()), taskType());
        }
        out.writeNamedWriteable(prompt);
        out.writeNamedWriteable(targetField);
    }

    public Expression prompt() {
        return prompt;
    }

    public Attribute targetField() {
        return targetField;
    }

    @Override
    public Completion withInferenceId(Expression newInferenceId) {
        if (inferenceId().equals(newInferenceId)) {
            return this;
        }

        return new Completion(source(), child(), newInferenceId, taskType(), prompt, targetField);
    }

    @Override
    public List<TaskType> supportedTaskTypes() {
        return SUPPORTED_TASK_TYPES;
    }

    @Override
    public Completion replaceChild(LogicalPlan newChild) {
        return new Completion(source(), newChild, inferenceId(), taskType(), prompt, targetField);
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
    public Completion withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        return new Completion(source(), child(), inferenceId(), taskType(), prompt, this.renameTargetField(newNames.get(0)));
    }

    private Attribute renameTargetField(String newName) {
        if (newName.equals(targetField.name())) {
            return targetField;
        }

        return targetField.withName(newName).withId(new NameId());
    }

    @Override
    public Completion withResolvedInference(ResolvedInference resolvedInference) {
        return super.withResolvedInference(resolvedInference);
    }

    @Override
    protected AttributeSet computeReferences() {
        return prompt.references();
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && prompt.resolved() && targetField.resolved();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (prompt.resolved() && DataType.isString(prompt.dataType()) == false) {
            failures.add(fail(prompt, "prompt must be of type [{}] but is [{}]", TEXT.typeName(), prompt.dataType().typeName()));
        }
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Completion::new, child(), inferenceId(), taskType(), prompt, targetField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Completion completion = (Completion) o;

        return Objects.equals(prompt, completion.prompt) && Objects.equals(targetField, completion.targetField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prompt, targetField);
    }
}
