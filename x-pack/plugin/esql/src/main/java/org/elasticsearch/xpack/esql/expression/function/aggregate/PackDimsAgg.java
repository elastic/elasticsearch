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
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PackDimsAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
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
        PackDimsAgg::new
    );

    public static final TransportVersion PACK_DIMS_AGG_VERSION = TransportVersion.fromName("pack_dims_agg");

    public static PackDimsAgg create(Source source, List<? extends Expression> dimensions) {
        if (dimensions.isEmpty()) {
            throw new IllegalArgumentException("PackDimsAgg requires at least one dim");
        }
        return new PackDimsAgg(source, dimensions.getFirst(), Literal.TRUE, NO_WINDOW, dimensions.subList(1, dimensions.size()));
    }

    public PackDimsAgg(Source source, Expression field, Expression filter, Expression window, List<? extends Expression> extraDims) {
        super(source, field, filter, window, extraDims);
    }

    private PackDimsAgg(StreamInput in) throws IOException {
        super(in);
    }

    public List<Expression> dims() {
        return CollectionUtils.combine(List.of(field()), parameters());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<PackDimsAgg> info() {
        return NodeInfo.create(this, PackDimsAgg::new, field(), filter(), window(), parameters());
    }

    @Override
    public PackDimsAgg replaceChildren(List<Expression> newChildren) {
        Expression field = newChildren.get(0);
        Expression filter = newChildren.get(1);
        Expression window = newChildren.get(2);
        List<Expression> extraDims = newChildren.subList(3, newChildren.size());
        return new PackDimsAgg(source(), field, filter, window, extraDims);
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
        List<Expression> dims = dims();
        for (int i = 0; i < dims.size(); i++) {
            resolution = resolution.and(TypeResolutions.isExact(dims.get(i), sourceText(), fromIndex(i)));
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
