/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PackDimsAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.fromIndex;

/**
 * Collects one or more time-series dimension fields and pack them as a single field.
 */
public class PackDimsAgg extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PackDimsAgg",
        PackDimsAgg::readFrom
    );

    public static final TransportVersion PACK_DIMS_AGG_VERSION = TransportVersion.fromName("pack_dims_agg");

    public static PackDimsAgg create(Source source, List<? extends Expression> dimensions) {
        if (dimensions.isEmpty()) {
            throw new IllegalArgumentException("PackDimsAgg requires at least one dim");
        }
        return new PackDimsAgg(source, dimensions, Literal.TRUE, NO_WINDOW);
    }

    public PackDimsAgg(Source source, List<? extends Expression> dimensions, Expression filter, Expression window) {
        super(source, dimensions, filter, window, List.of());
    }

    private static PackDimsAgg readFrom(StreamInput in) throws IOException {
        // Legacy serialization format for backwards compatibility
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> extraDims = in.readNamedWriteableCollectionAsList(Expression.class);
        return new PackDimsAgg(source, CollectionUtils.combine(List.of(field), extraDims), filter, window);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Legacy serialization format for backwards compatibility
        source().writeTo(out);
        out.writeNamedWriteable(fields().get(0));
        out.writeNamedWriteable(filter());
        if (out.getTransportVersion().supports(WINDOW_INTERVAL)) {
            out.writeNamedWriteable(window());
        }
        out.writeNamedWriteableCollection(fields().subList(1, fields().size()));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<PackDimsAgg> info() {
        return NodeInfo.create(this, PackDimsAgg::new, fields(), filter(), window());
    }

    @Override
    public PackDimsAgg replaceChildren(List<Expression> newChildren) {
        // children layout: dimensions[], filter, window
        int n = newChildren.size();
        List<Expression> dimensions = newChildren.subList(0, n - 2);
        Expression filter = newChildren.get(n - 2);
        Expression window = newChildren.get(n - 1);
        return new PackDimsAgg(source(), dimensions, filter, window);
    }

    @Override
    public PackDimsAgg withFilter(Expression filter) {
        if (filter instanceof Literal l && l.value() == Boolean.TRUE) {
            return this;
        }
        throw new UnsupportedOperationException("Packed dimension values do not support filters");
    }

    @Override
    public DataType dataType() {
        return DataType.SOURCE;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = TypeResolution.TYPE_RESOLVED;
        for (int i = 0; i < fields().size(); i++) {
            resolution = resolution.and(TypeResolutions.isExact(fields().get(i), sourceText(), fromIndex(i)));
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return resolution;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new PackDimsAggregatorFunctionSupplier();
    }
}
