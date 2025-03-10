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
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.asAttributes;

public class Rerank extends InferencePlan implements SortAgnostic, SurrogateLogicalPlan {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Rerank", Rerank::new);
    private final Expression queryText;
    private final List<Alias> rerankFields;

    public Rerank(Source source, LogicalPlan child, Expression inferenceId, Expression queryText, List<Alias> rerankFields) {
        super(source, child, inferenceId);
        this.queryText = queryText;
        this.rerankFields = rerankFields;
    }

    public Rerank(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readCollectionAsList(Alias::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(queryText);
        out.writeCollection(rerankFields());
    }

    public Expression queryText() {
        return queryText;
    }

    public List<Alias> rerankFields() {
        return rerankFields;
    }

    @Override
    public TaskType taskType() {
        return TaskType.RERANK;
    }

    @Override
    public LogicalPlan withInferenceResolutionError(String inferenceId, String error) {
        Expression newInferenceId = new UnresolvedAttribute(inferenceId().source(), inferenceId, error);
        return new Rerank(source(), child(), newInferenceId, queryText, rerankFields);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Rerank(source(), newChild, inferenceId(), queryText, rerankFields);
    }

    @Override
    protected AttributeSet computeReferences() {
        return computeReferences(rerankFields);
    }

    public static AttributeSet computeReferences(List<Alias> fields) {
        AttributeSet generated = new AttributeSet(asAttributes(fields));
        return Expressions.references(fields).subtract(generated);
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && queryText.resolved() && Resolvables.resolved(rerankFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Rerank::new, child(), inferenceId(), queryText, rerankFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Rerank rerank = (Rerank) o;
        return Objects.equals(queryText, rerank.queryText) && Objects.equals(rerankFields, rerank.rerankFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryText, rerankFields);
    }

    @Override
    public LogicalPlan surrogate() {
        Attribute scoreAttribute = child().output().stream().filter(attr -> attr.name().equals(MetadataAttribute.SCORE)).findFirst().get();
        assert scoreAttribute != null;
        Order sortOrder = new Order(source(), scoreAttribute, Order.OrderDirection.DESC, Order.NullsPosition.ANY);
        return new OrderBy(source(), this, List.of(sortOrder));
    }
}
