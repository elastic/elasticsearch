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
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Physical plan node for {@code SORT order1, order2 | LIMIT N BY grouping1, grouping2, ...}.
 * Sorts the input rows retaining at most N rows per group defined by the grouping expressions.
 */
public class TopNByExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "TopNByExec",
        TopNByExec::new
    );

    private final Expression limitPerGroup;
    private final List<Order> order;
    private final List<Expression> groupings;

    /**
     * Attributes that may be extracted as doc values even if that makes them
     * less accurate. This is mostly used for geo fields which lose a lot of
     * precision in their doc values, but in some cases doc values provides
     * <strong>enough</strong> precision to do the job.
     * <p>
     * This is never serialized between nodes and only used locally.
     * </p>
     */
    private final Set<Attribute> docValuesAttributes;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    public TopNByExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize
    ) {
        this(source, child, order, limitPerGroup, groupings, estimatedRowSize, Set.of());
    }

    private TopNByExec(
        Source source,
        PhysicalPlan child,
        List<Order> order,
        Expression limitPerGroup,
        List<Expression> groupings,
        Integer estimatedRowSize,
        Set<Attribute> docValuesAttributes
    ) {
        super(source, child);
        this.order = order;
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
        this.estimatedRowSize = estimatedRowSize;
        this.docValuesAttributes = docValuesAttributes;
    }

    private TopNByExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readCollectionAsList(Order::new),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readOptionalVInt()
        );
        // docValueAttributes are only used on the data node and never serialized.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order());
        out.writeNamedWriteable(limitPerGroup());
        out.writeNamedWriteableCollection(groupings());
        out.writeOptionalVInt(estimatedRowSize());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<TopNByExec> info() {
        return NodeInfo.create(this, TopNByExec::new, child(), order, limitPerGroup, groupings, estimatedRowSize);
    }

    @Override
    public TopNByExec replaceChild(PhysicalPlan newChild) {
        return new TopNByExec(source(), newChild, order, limitPerGroup, groupings, estimatedRowSize, docValuesAttributes);
    }

    public TopNByExec withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
        return new TopNByExec(source(), child(), order, limitPerGroup, groupings, estimatedRowSize, docValuesAttributes);
    }

    public Expression limitPerGroup() {
        return limitPerGroup;
    }

    public List<Order> order() {
        return order;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public Set<Attribute> docValuesAttributes() {
        return docValuesAttributes;
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
        final List<Attribute> output = output();
        final boolean needsSortedDocIds = output.stream().anyMatch(a -> a.dataType() == DataType.DOC_DATA_TYPE);
        state.add(needsSortedDocIds, output);
        int size = state.consumeAllFields(true);
        size = Math.max(size, 1);
        return Objects.equals(this.estimatedRowSize, size)
            ? this
            : new TopNByExec(source(), child(), order, limitPerGroup, groupings, size, docValuesAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limitPerGroup, groupings, estimatedRowSize, docValuesAttributes);
    }

    @Override
    public boolean equals(Object obj) {
        boolean equals = super.equals(obj);
        if (equals) {
            var other = (TopNByExec) obj;
            equals = Objects.equals(order, other.order)
                && Objects.equals(limitPerGroup, other.limitPerGroup)
                && Objects.equals(groupings, other.groupings)
                && Objects.equals(estimatedRowSize, other.estimatedRowSize)
                && Objects.equals(docValuesAttributes, other.docValuesAttributes);
        }
        return equals;
    }
}
