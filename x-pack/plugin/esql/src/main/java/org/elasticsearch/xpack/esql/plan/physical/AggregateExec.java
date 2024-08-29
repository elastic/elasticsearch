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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class AggregateExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "AggregateExec",
        AggregateExec::new
    );

    private final List<? extends Expression> groupings;
    private final List<? extends NamedExpression> aggregates;

    private final Mode mode;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    public enum Mode {
        SINGLE,
        PARTIAL, // maps raw inputs to intermediate outputs
        FINAL, // maps intermediate inputs to final outputs
    }

    public AggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        Mode mode,
        Integer estimatedRowSize
    ) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
        this.mode = mode;
        this.estimatedRowSize = estimatedRowSize;
    }

    private AggregateExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            ((PlanStreamInput) in).readPhysicalPlanNode(),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            in.readEnum(AggregateExec.Mode.class),
            in.readOptionalVInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        ((PlanStreamOutput) out).writePhysicalPlanNode(child());
        out.writeNamedWriteableCollection(groupings());
        out.writeNamedWriteableCollection(aggregates());
        out.writeEnum(getMode());
        out.writeOptionalVInt(estimatedRowSize());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<AggregateExec> info() {
        return NodeInfo.create(this, AggregateExec::new, child(), groupings, aggregates, mode, estimatedRowSize);
    }

    @Override
    public AggregateExec replaceChild(PhysicalPlan newChild) {
        return new AggregateExec(source(), newChild, groupings, aggregates, mode, estimatedRowSize);
    }

    public List<? extends Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    public AggregateExec withMode(Mode newMode) {
        return new AggregateExec(source(), child(), groupings, aggregates, newMode, estimatedRowSize);
    }

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, aggregates);  // The groupings are contained within the aggregates
        int size = state.consumeAllFields(true);
        return Objects.equals(this.estimatedRowSize, size) ? this : new AggregateExec(source(), child(), groupings, aggregates, mode, size);
    }

    public Mode getMode() {
        return mode;
    }

    @Override
    public List<Attribute> output() {
        return Expressions.asAttributes(aggregates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, mode, estimatedRowSize, child());
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
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(child(), other.child());
    }

}
