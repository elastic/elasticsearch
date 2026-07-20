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
import org.elasticsearch.core.TimeValue;
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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;
import static org.elasticsearch.xpack.esql.inference.InferenceSettings.COMPLETION_ROW_LIMIT_SETTING;

public class Embed extends InferencePlan<Embed> implements TelemetryAware, PostAnalysisVerificationAware {

    public static final String DEFAULT_OUTPUT_FIELD_NAME = "embedding";

    public static final String TIMEOUT_OPTION_NAME = "timeout";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Embed",
        Embed::new
    );

    private static final Literal DEFAULT_ROW_LIMIT = Literal.integer(Source.EMPTY, COMPLETION_ROW_LIMIT_SETTING.getDefault(Settings.EMPTY));

    private final Expression input;
    private final Attribute targetField;

    private List<Attribute> lazyOutput;

    public Embed(Source source, LogicalPlan p, Expression rowLimit, Expression input, Attribute targetField) {
        this(source, p, Literal.NULL, rowLimit, input, targetField,  null);
    }

    public Embed(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        Expression rowLimit,
        Expression input,
        Attribute targetField
    ) {
        this(source, child, inferenceId, rowLimit, input, targetField, null);
    }

    public Embed(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        Expression rowLimit,
        Expression input,
        Attribute targetField,
        TimeValue timeout
    ) {
        super(source, child, inferenceId, rowLimit, timeout);
        this.input = input;
        this.targetField = targetField;
    }

    public Embed(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().supports(ESQL_INFERENCE_ROW_LIMIT) ? in.readNamedWriteable(Expression.class) : DEFAULT_ROW_LIMIT,
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class),
            in.getTransportVersion().supports(ESQL_INFERENCE_ACCEPT_TIMEOUT) ? in.readOptionalTimeValue() : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(targetField);
        if (out.getTransportVersion().supports(ESQL_INFERENCE_ACCEPT_TIMEOUT)) {
            out.writeOptionalTimeValue(timeout());
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression input() {
        return input;
    }

    public Attribute targetField() {
        return targetField;
    }

    @Override
    public Embed withInferenceId(Expression newInferenceId) {
        if (inferenceId().equals(newInferenceId)) {
            return this;
        }

        return new Embed(source(), child(), newInferenceId, rowLimit(), input, targetField, timeout());
    }

    @Override
    public Embed withTimeout(TimeValue newTimeout) {
        if (Objects.equals(timeout(), newTimeout)) {
            return this;
        }
        return new Embed(source(), child(), inferenceId(), rowLimit(), input, targetField, newTimeout);
    }

    @Override
    public Embed replaceChild(LogicalPlan newChild) {
        return new Embed(source(), newChild, inferenceId(), rowLimit(), input, targetField, timeout());
    }

    @Override
    public List<String> validOptionNames() {
        return List.of(INFERENCE_ID_OPTION_NAME, TIMEOUT_OPTION_NAME);
    }

    @Override
    public TaskType taskType() {
        return TaskType.TEXT_EMBEDDING;
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
    public Embed withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        return new Embed(
            source(),
            child(),
            inferenceId(),
            rowLimit(),
            input,
            this.renameTargetField(newNames.get(0)),
            timeout()
        );
    }

    private Attribute renameTargetField(String newName) {
        if (newName.equals(targetField.name())) {
            return targetField;
        }

        return targetField.withName(newName).withId(new NameId());
    }

    @Override
    protected AttributeSet computeReferences() {
        return input.references();
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && input.resolved() && targetField.resolved();
    }

    @Override
    public boolean isFoldable() {
        return input.foldable();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (input.resolved() && DataType.isString(input.dataType()) == false) {
            failures.add(fail(input, "input must be of type [{}] but is [{}]", TEXT.typeName(), input.dataType().typeName()));
        }
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Embed::new, child(), inferenceId(), rowLimit(), input, targetField, timeout());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Embed embed = (Embed) o;

        return Objects.equals(input, embed.input)
            && Objects.equals(targetField, embed.targetField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), input, targetField);
    }
}
