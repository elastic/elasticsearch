/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node for {@code SORT order1, order2 | LIMIT N BY grouping1, grouping2, ...}.
 * Sorts the input rows retaining at most N rows per group defined by the grouping expressions.
 * <p>
 * With ties, a group also keeps every row that sorts equal to its N-th row, like SQL's {@code FETCH FIRST N ROWS WITH TIES}.
 * The query language has no syntax for it; plans built by translation (PromQL {@code or}) use it.
 */
public class TopNBy extends UnaryPlan implements PipelineBreaker, DocPreserving {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "TopNBy", TopNBy::new);
    public static final TransportVersion ESQL_TOPN_BY_WITH_TIES = TransportVersion.fromName("esql_topn_by_with_ties");

    private final List<Order> order;
    private final Expression limitPerGroup;
    private final List<Expression> groupings;
    private final boolean withTies;

    public TopNBy(Source source, LogicalPlan child, List<Order> order, Expression limitPerGroup, List<Expression> groupings) {
        this(source, child, order, limitPerGroup, groupings, false);
    }

    public TopNBy(
        Source source,
        LogicalPlan child,
        List<Order> order,
        Expression limitPerGroup,
        List<Expression> groupings,
        boolean withTies
    ) {
        super(source, child);
        this.order = order;
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
        this.withTies = withTies;
    }

    private TopNBy(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readCollectionAsList(Order::new),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.getTransportVersion().supports(ESQL_TOPN_BY_WITH_TIES) && in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order);
        out.writeNamedWriteable(limitPerGroup);
        out.writeNamedWriteableCollection(groupings);
        writeWithTies(out, withTies);
    }

    /**
     * Writes the ties flag, refusing to send ties to a node that would drop them silently.
     */
    public static void writeWithTies(StreamOutput out, boolean withTies) throws IOException {
        if (out.getTransportVersion().supports(ESQL_TOPN_BY_WITH_TIES)) {
            out.writeBoolean(withTies);
        } else if (withTies) {
            // Dropping the flag would make the peer cut each group at N rows and lose the tied ones.
            throw new IllegalArgumentException(
                "SORT | LIMIT BY with ties is not supported in peer node's version ["
                    + out.getTransportVersion()
                    + "]. Upgrade to version ["
                    + ESQL_TOPN_BY_WITH_TIES
                    + "] or newer."
            );
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean expressionsResolved() {
        return limitPerGroup.resolved() && Resolvables.resolved(order) && Resolvables.resolved(groupings);
    }

    @Override
    protected NodeInfo<TopNBy> info() {
        return NodeInfo.create(this, TopNBy::new, child(), order, limitPerGroup, groupings, withTies);
    }

    @Override
    public TopNBy replaceChild(LogicalPlan newChild) {
        return new TopNBy(source(), newChild, order, limitPerGroup, groupings, withTies);
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

    public boolean withTies() {
        return withTies;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limitPerGroup, groupings, withTies);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var other = (TopNBy) obj;
            return Objects.equals(order, other.order)
                && Objects.equals(limitPerGroup, other.limitPerGroup)
                && Objects.equals(groupings, other.groupings)
                && withTies == other.withTies;
        }
        return false;
    }
}
