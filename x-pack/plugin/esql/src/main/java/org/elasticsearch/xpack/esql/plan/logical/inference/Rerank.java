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
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Rerank extends InferencePlan implements SortAgnostic, SurrogateLogicalPlan {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Rerank", Rerank::new);
    private final Attribute scoreAttribute;
    private final Expression queryText;
    private final List<Alias> rerankFields;
    private List<Attribute> lazyOutput;

    public Rerank(Source source, LogicalPlan child, Expression inferenceId, Expression queryText, List<Alias> rerankFields) {
        super(source, child, inferenceId);
        this.queryText = queryText;
        this.rerankFields = rerankFields;
        this.scoreAttribute = new UnresolvedAttribute(source, MetadataAttribute.SCORE);
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
        AttributeSet.Builder refs = computeReferences(rerankFields).asBuilder();

        if (planHasAttribute(child(), scoreAttribute)) {
            refs.add(scoreAttribute);
        }

        return refs.build();
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
    public LogicalPlan surrogate() {
        Order sortOrder = new Order(source(), scoreAttribute, Order.OrderDirection.DESC, Order.NullsPosition.ANY);
        return new OrderBy(source(), this, List.of(sortOrder));
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = planHasAttribute(child(), scoreAttribute)
                ? child().output()
                : mergeOutputAttributes(List.of(scoreAttribute), child().output());
        }

        return lazyOutput;
    }

    public static boolean planHasAttribute(QueryPlan<?> plan, Attribute attribute) {
        return plan.outputSet().stream().anyMatch(attr -> attr.equals(attribute));
    }
}
