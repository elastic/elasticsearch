/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * A window specification for an aggregate whose window is larger than, but not an exact multiple of, the time bucket:
 * {@code W = k * B + r} with {@code k >= 1} and {@code 0 < r < B}. It decomposes the window into two per-bucket
 * aggregation channels that are combined when the final value for an output bucket is emitted:
 * <ul>
 *     <li>a <em>full</em> channel aggregating every row of a bucket, merged across the {@code k} buckets fully
 *     covered by the window, and</li>
 *     <li>a <em>partial</em> channel aggregating only the trailing {@code r} of each bucket (via {@link #partialFilter()},
 *     a {@link WindowFilter} over {@code r}), contributing the boundary bucket's remainder.</li>
 * </ul>
 * This node sits in the {@code window} slot of an
 * {@link org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction}. It is planted by the
 * {@code ApplyWindowSemantics} analyzer rule and consumed by the physical planner, which wires the two aggregation
 * channels; it is never evaluated as a scalar. Keeping the {@link WindowFilter} here (rather than wiring it at the
 * physical layer) both keeps the {@code @timestamp} attribute referenced by the plan, so it survives column pruning
 * and field extraction, and makes the doubled intermediate state derivable from the aggregate alone on every node.
 */
public class WindowWithPartial extends Expression implements VersionedNamedWriteable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WindowWithPartial",
        WindowWithPartial::new
    );

    public static final TransportVersion ESQL_PER_AGGREGATE_WINDOW = TransportVersion.fromName("esql_per_aggregate_window");

    private final Expression window;
    private final WindowFilter partialFilter;

    public WindowWithPartial(Source source, Expression window, WindowFilter partialFilter) {
        super(source, asList(window, partialFilter));
        this.window = window;
        this.partialFilter = partialFilter;
    }

    private WindowWithPartial(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            (WindowFilter) in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(window);
        out.writeNamedWriteable(partialFilter);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_PER_AGGREGATE_WINDOW;
    }

    /**
     * The total window duration {@code W}, a foldable time-duration literal.
     */
    public Expression window() {
        return window;
    }

    /**
     * The row filter capturing the trailing {@code r = W mod B} of each bucket.
     */
    public WindowFilter partialFilter() {
        return partialFilter;
    }

    @Override
    public DataType dataType() {
        return DataType.TIME_DURATION;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected NodeInfo<WindowWithPartial> info() {
        return NodeInfo.create(this, WindowWithPartial::new, window, partialFilter);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new WindowWithPartial(source(), newChildren.get(0), (WindowFilter) newChildren.get(1));
    }
}
