/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class AggregateExec extends AbstractAggregateExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "AggregateExec",
        AggregateExec::new
    );

    public AggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        List<Attribute> intermediateAttributes,
        Integer estimatedRowSize
    ) {
        super(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    protected AggregateExec(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<AggregateExec> info() {
        return NodeInfo.create(this, AggregateExec::new, child(), groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    @Override
    public AggregateExec replaceChild(PhysicalPlan newChild) {
        return new AggregateExec(source(), newChild, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    @Override
    public AggregateExec withAggregates(List<? extends NamedExpression> newAggregates) {
        return new AggregateExec(source(), child(), groupings, newAggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    public AggregateExec withMode(AggregatorMode newMode) {
        return new AggregateExec(source(), child(), groupings, aggregates, newMode, intermediateAttributes, estimatedRowSize);
    }

    @Override
    protected AggregateExec withEstimatedSize(int estimatedRowSize) {
        return new AggregateExec(source(), child(), groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, mode, intermediateAttributes, estimatedRowSize, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        AggregateExec other = (AggregateExec) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(mode, other.mode)
            && Objects.equals(intermediateAttributes, other.intermediateAttributes)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(child(), other.child());
    }

}
