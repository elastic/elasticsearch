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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for {@code LIMIT N BY expr1, expr2, ...}.
 * Retains at most N rows per group defined by the grouping expressions.
 */
public class LimitByExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "LimitByExec",
        LimitByExec::readFrom
    );

    private final Expression limitPerGroup;
    private final List<Expression> groupings;
    private final Integer estimatedRowSize;

    public LimitByExec(Source source, PhysicalPlan child, Expression limitPerGroup, List<Expression> groupings, Integer estimatedRowSize) {
        super(source, child);
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
        this.estimatedRowSize = estimatedRowSize;
    }

    private static LimitByExec readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        PhysicalPlan child = in.readNamedWriteable(PhysicalPlan.class);
        Expression limit = in.readNamedWriteable(Expression.class);
        Integer estimatedRowSize = in.readOptionalVInt();
        List<Expression> groupings = in.readNamedWriteableCollectionAsList(Expression.class);
        return new LimitByExec(source, child, limit, groupings, estimatedRowSize);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(limitPerGroup());
        out.writeOptionalVInt(estimatedRowSize);
        out.writeNamedWriteableCollection(groupings());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends LimitByExec> info() {
        return NodeInfo.create(this, LimitByExec::new, child(), limitPerGroup, groupings, estimatedRowSize);
    }

    @Override
    public LimitByExec replaceChild(PhysicalPlan newChild) {
        return new LimitByExec(source(), newChild, limitPerGroup, groupings, estimatedRowSize);
    }

    public Expression limitPerGroup() {
        return limitPerGroup;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public PhysicalPlan estimateRowSize(State unused) {
        final List<Attribute> output = output();
        EstimatesRowSize.State state = new EstimatesRowSize.State();
        final boolean needsSortedDocIds = output.stream().anyMatch(a -> a.dataType() == DataType.DOC_DATA_TYPE);
        state.add(needsSortedDocIds, output);
        int size = state.consumeAllFields(true);
        size = Math.max(size, 1);
        return Objects.equals(this.estimatedRowSize, size) ? this : new LimitByExec(source(), child(), limitPerGroup, groupings, size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limitPerGroup, groupings, estimatedRowSize, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LimitByExec other = (LimitByExec) obj;
        return Objects.equals(limitPerGroup, other.limitPerGroup)
            && Objects.equals(groupings, other.groupings)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(child(), other.child());
    }
}
