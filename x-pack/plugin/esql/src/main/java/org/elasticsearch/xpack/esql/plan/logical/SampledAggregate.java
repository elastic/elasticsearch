/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Aggregate, which upon execution is either:
 * <ul>
 * <li> sampled with the {@code sampleProbability} for fast execution; or
 * <li> replaced by an exact aggregate if the {@code originalAggregates} can be pushed down to Lucene.
 * </ul>
 */
public class SampledAggregate extends Aggregate {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "SampledAggregate",
        SampledAggregate::new
    );

    private final List<? extends NamedExpression> originalAggregates;
    private final Expression sampleProbability;

    public SampledAggregate(
        Source source,
        LogicalPlan child,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates,
        List<? extends NamedExpression> originalAggregates,
        Expression sampleProbability
    ) {
        super(source, child, groupings, aggregates);
        this.originalAggregates = originalAggregates;
        this.sampleProbability = sampleProbability;
    }

    public SampledAggregate(StreamInput in) throws IOException {
        super(in);
        this.originalAggregates = in.readNamedWriteableCollectionAsList(NamedExpression.class);
        this.sampleProbability = in.readNamedWriteable(Expression.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteableCollection(originalAggregates);
        out.writeNamedWriteable(sampleProbability);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<SampledAggregate> info() {
        return NodeInfo.create(this, SampledAggregate::new, child(), groupings, aggregates, originalAggregates, sampleProbability);
    }

    @Override
    public SampledAggregate replaceChild(LogicalPlan newChild) {
        return new SampledAggregate(source(), newChild, groupings, aggregates, originalAggregates, sampleProbability);
    }

    @Override
    public SampledAggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        Set<Object> ids = originalAggregates.stream().map(NamedExpression::id).collect(Collectors.toSet());
        List<? extends NamedExpression> newOriginalAggregates = newAggregates.stream().filter(agg -> ids.contains(agg.id())).toList();
        return new SampledAggregate(source(), child, newGroupings, newAggregates, newOriginalAggregates, sampleProbability);
    }

    public List<? extends NamedExpression> originalAggregates() {
        return originalAggregates;
    }

    public Expression sampleProbability() {
        return sampleProbability;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originalAggregates, sampleProbability);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SampledAggregate other = (SampledAggregate) obj;
        return super.equals(other)
            && Objects.equals(originalAggregates, other.originalAggregates)
            && Objects.equals(sampleProbability, other.sampleProbability);
    }
}
