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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

public class Rerank extends InferencePlan<Rerank> implements TelemetryAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Rerank", Rerank::new);
    public static final String DEFAULT_INFERENCE_ID = ".rerank-v1-elasticsearch";
    public static final String INFERENCE_ID_OPTION_NAME = "inferenceId";
    public static final String SCORE_COLUMN_OPTION_NAME = "scoreColumn";

    private final Attribute scoreAttribute;
    private final Expression queryText;
    private final List<Alias> rerankFields;
    private List<Attribute> lazyOutput;

    public Rerank(Source source, LogicalPlan child, Expression queryText, List<Alias> rerankFields) {
        this(
            source,
            child,
            Literal.keyword(Source.EMPTY, DEFAULT_INFERENCE_ID),
            queryText,
            rerankFields,
            new UnresolvedAttribute(Source.EMPTY, MetadataAttribute.SCORE)
        );
    }

    public Rerank(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        Expression queryText,
        List<Alias> rerankFields,
        Attribute scoreAttribute
    ) {
        super(source, child, inferenceId);
        this.queryText = queryText;
        this.rerankFields = rerankFields;
        this.scoreAttribute = scoreAttribute;
    }

    public Rerank(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readCollectionAsList(Alias::new),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(queryText);
        out.writeCollection(rerankFields());
        out.writeNamedWriteable(scoreAttribute);
    }

    public Expression queryText() {
        return queryText;
    }

    public List<Alias> rerankFields() {
        return rerankFields;
    }

    public Attribute scoreAttribute() {
        return scoreAttribute;
    }

    @Override
    public TaskType taskType() {
        return TaskType.RERANK;
    }

    @Override
    public Rerank withInferenceId(Expression newInferenceId) {
        return new Rerank(source(), child(), newInferenceId, queryText, rerankFields, scoreAttribute);
    }

    public Rerank withRerankFields(List<Alias> newRerankFields) {
        return new Rerank(source(), child(), inferenceId(), queryText, newRerankFields, scoreAttribute);
    }

    public Rerank withScoreAttribute(Attribute newScoreAttribute) {
        return new Rerank(source(), child(), inferenceId(), queryText, rerankFields, newScoreAttribute);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Rerank(source(), newChild, inferenceId(), queryText, rerankFields, scoreAttribute);
    }

    @Override
    protected AttributeSet computeReferences() {
        return computeReferences(rerankFields);
    }

    public List<Attribute> generatedAttributes() {
        return List.of(scoreAttribute);
    }

    @Override
    public Rerank withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        return new Rerank(source(), child(), inferenceId(), queryText, rerankFields, this.renameScoreAttribute(newNames.get(0)));
    }

    private Attribute renameScoreAttribute(String newName) {
        if (newName.equals(scoreAttribute.name())) {
            return scoreAttribute;
        }

        return scoreAttribute.withName(newName).withId(new NameId());
    }

    public static AttributeSet computeReferences(List<Alias> fields) {
        AttributeSet rerankFields = AttributeSet.of(asAttributes(fields));
        return Expressions.references(fields).subtract(rerankFields);
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && queryText.resolved() && Resolvables.resolved(rerankFields) && scoreAttribute.resolved();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Rerank::new, child(), inferenceId(), queryText, rerankFields, scoreAttribute);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Rerank rerank = (Rerank) o;
        return Objects.equals(queryText, rerank.queryText)
            && Objects.equals(rerankFields, rerank.rerankFields)
            && Objects.equals(scoreAttribute, rerank.scoreAttribute);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryText, rerankFields, scoreAttribute);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(List.of(scoreAttribute), child().output());
        }
        return lazyOutput;
    }

    public static class Builder {
        private Rerank rerank;

        public Builder(Rerank rerank) {
            this.rerank = rerank;
        }

        public Rerank build() {
            return rerank;
        }

        public Builder withInferenceId(Expression inferenceId) {
            this.rerank = this.rerank.withInferenceId(inferenceId);
            return this;
        }

        public Builder withScoreAttribute(Attribute scoreAttribute) {
            this.rerank = this.rerank.withScoreAttribute(scoreAttribute);
            return this;
        }
    }
}
