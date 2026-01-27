/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;
import static org.elasticsearch.xpack.esql.inference.InferenceSettings.COMPLETION_ROW_LIMIT_SETTING;

public class Completion extends InferencePlan<Completion> implements TelemetryAware, PostAnalysisVerificationAware {

    public static final String DEFAULT_OUTPUT_FIELD_NAME = "completion";

    public static final String TASK_SETTINGS_OPTION_NAME = "task_settings";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Completion",
        Completion::new
    );

    private static final Literal DEFAULT_ROW_LIMIT = Literal.integer(Source.EMPTY, COMPLETION_ROW_LIMIT_SETTING.getDefault(Settings.EMPTY));
    public static final MapExpression DEFAULT_TASK_SETTINGS = new MapExpression(Source.EMPTY, List.of());

    private final Expression prompt;
    private final Attribute targetField;
    /**
     * Model-specific task settings passed to the inference endpoint.
     * Common settings include temperature, max_tokens, top_p, etc.
     * Settings are validated by the inference service, not at parse time.
     * Never null - defaults to empty map if not provided.
     */
    private final MapExpression taskSettings;
    private List<Attribute> lazyOutput;

    public Completion(Source source, LogicalPlan p, Expression rowLimit, Expression prompt, Attribute targetField) {
        this(source, p, Literal.NULL, rowLimit, prompt, targetField, DEFAULT_TASK_SETTINGS);
    }

    public Completion(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        Expression rowLimit,
        Expression prompt,
        Attribute targetField
    ) {
        this(source, child, inferenceId, rowLimit, prompt, targetField, DEFAULT_TASK_SETTINGS);
    }

    public Completion(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        Expression rowLimit,
        Expression prompt,
        Attribute targetField,
        MapExpression taskSettings
    ) {
        super(source, child, inferenceId, rowLimit);
        this.prompt = prompt;
        this.targetField = targetField;
        this.taskSettings = taskSettings;
    }

    public Completion(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().supports(ESQL_INFERENCE_ROW_LIMIT) ? in.readNamedWriteable(Expression.class) : DEFAULT_ROW_LIMIT,
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class),
            // COMPLETION is coordinator-only and should not be serialized in normal operation.
            // Deserialization is kept for rolling upgrade safety. Since old versions don't
            // know about task_settings, we use empty defaults.
            (MapExpression) in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(prompt);
        out.writeNamedWriteable(targetField);
        out.writeNamedWriteable(taskSettings);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression prompt() {
        return prompt;
    }

    public Attribute targetField() {
        return targetField;
    }

    public MapExpression taskSettings() {
        return taskSettings;
    }

    @Override
    public Completion withInferenceId(Expression newInferenceId) {
        if (inferenceId().equals(newInferenceId)) {
            return this;
        }

        return new Completion(source(), child(), newInferenceId, rowLimit(), prompt, targetField, taskSettings);
    }

    public Completion withTaskSettings(MapExpression newTaskSettings) {
        if (taskSettings.equals(newTaskSettings)) {
            return this;
        }
        return new Completion(source(), child(), inferenceId(), rowLimit(), prompt, targetField, newTaskSettings);
    }

    @Override
    public Completion replaceChild(LogicalPlan newChild) {
        return new Completion(source(), newChild, inferenceId(), rowLimit(), prompt, targetField, taskSettings);
    }

    @Override
    public List<String> validOptionNames() {
        return List.of(INFERENCE_ID_OPTION_NAME, TASK_SETTINGS_OPTION_NAME);
    }

    @Override
    public TaskType taskType() {
        return TaskType.COMPLETION;
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
        return new Completion(source(), child(), inferenceId(), rowLimit(), prompt, this.renameTargetField(newNames.get(0)), taskSettings);
    }

    private Attribute renameTargetField(String newName) {
        if (newName.equals(targetField.name())) {
            return targetField;
        }

        return targetField.withName(newName).withId(new NameId());
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
    public boolean isFoldable() {
        return prompt.foldable();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (prompt.resolved() && DataType.isString(prompt.dataType()) == false) {
            failures.add(fail(prompt, "prompt must be of type [{}] but is [{}]", TEXT.typeName(), prompt.dataType().typeName()));
        }
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Completion::new, child(), inferenceId(), rowLimit(), prompt, targetField, taskSettings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Completion completion = (Completion) o;

        return Objects.equals(prompt, completion.prompt)
            && Objects.equals(targetField, completion.targetField)
            && Objects.equals(taskSettings, completion.taskSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prompt, targetField, taskSettings);
    }
}
