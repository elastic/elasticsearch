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
import org.elasticsearch.common.logging.HeaderWarning;
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
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Streaming;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Completion extends InferencePlan<Completion> implements TelemetryAware, PostAnalysisVerificationAware, SurrogateLogicalPlan {

    public static final String DEFAULT_OUTPUT_FIELD_NAME = "completion";

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Completion",
        Completion::new
    );
    public static final int DEFAULT_MAX_ROW_LIMIT = 100;

    private final Expression prompt;
    private final Attribute targetField;
    private final Expression rowLimit;

    private List<Attribute> lazyOutput;

    public Completion(Source source, LogicalPlan p, Expression prompt, Attribute targetField, Expression rowLimit) {
        this(source, p, Literal.keyword(Source.EMPTY, DEFAULT_OUTPUT_FIELD_NAME), prompt, targetField, rowLimit);
    }

    public Completion(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        Expression prompt,
        Attribute targetField,
        Expression rowLimit
    ) {
        super(source, child, inferenceId);
        this.prompt = prompt;
        this.targetField = targetField;
        this.rowLimit = rowLimit;
    }

    public Completion(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class),
            in.getTransportVersion().supports(ESQL_INFERENCE_USAGE_LIMIT)
                ? in.readNamedWriteable(Expression.class)
                : Literal.integer(Source.EMPTY, DEFAULT_MAX_ROW_LIMIT)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(prompt);
        out.writeNamedWriteable(targetField);
        if (out.getTransportVersion().supports(ESQL_INFERENCE_USAGE_LIMIT)) {
            out.writeNamedWriteable(rowLimit);
        }
    }

    public Expression prompt() {
        return prompt;
    }

    public Attribute targetField() {
        return targetField;
    }

    public Expression rowLimit() {
        return rowLimit;
    }

    @Override
    public Completion withInferenceId(Expression newInferenceId) {
        if (inferenceId().equals(newInferenceId)) {
            return this;
        }

        return new Completion(source(), child(), newInferenceId, prompt, targetField, rowLimit);
    }

    @Override
    public Completion replaceChild(LogicalPlan newChild) {
        return new Completion(source(), newChild, inferenceId(), prompt, targetField, rowLimit);
    }

    @Override
    public TaskType taskType() {
        return TaskType.COMPLETION;
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
        return new Completion(source(), child(), inferenceId(), prompt, this.renameTargetField(newNames.get(0)), rowLimit);
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
        return NodeInfo.create(this, Completion::new, child(), inferenceId(), prompt, targetField, rowLimit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Completion completion = (Completion) o;

        return Objects.equals(prompt, completion.prompt)
            && Objects.equals(targetField, completion.targetField)
            && Objects.equals(rowLimit, completion.rowLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prompt, targetField, rowLimit);
    }

    @Override
    public LogicalPlan surrogate() {
        int limit = Foldables.intValueOf(rowLimit, rowLimit.sourceText(), "row limit");
        LogicalPlan newChildPlan = applyLimitToChildPlan(child(), limit);

        if (newChildPlan != child()) {
            HeaderWarning.addWarning("No limit defined, adding default limit of [{}]", limit);
            return replaceChild(newChildPlan);
        }

        return this;
    }

    private LogicalPlan applyLimitToChildPlan(LogicalPlan child, int rowLimit) {
        if (child instanceof Limit limit) {
            Object limitValue = Foldables.literalValueOf(limit.limit());
            if (limitValue instanceof Integer existingLimit && existingLimit <= rowLimit) {
                return child;
            }
            return limit.withLimit(Literal.integer(Source.EMPTY, rowLimit));
        }

        if (child instanceof UnaryPlan unaryPlan) {
            if (unaryPlan instanceof Streaming) {
                return unaryPlan.replaceChild(applyLimitToChildPlan(unaryPlan.child(), rowLimit));
            }

            return unaryPlan.replaceChild(
                new Limit(child.source(), Literal.integer(Source.EMPTY, rowLimit), unaryPlan.child(), false, true)
            );
        }

        return transformChildren(p -> applyLimitToChildPlan(p, rowLimit));
    }
}
