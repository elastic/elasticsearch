/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;

@Experimental
public class CountDistinct extends AggregateFunction implements OptionalArgument, ToAggregator {
    private static final int DEFAULT_PRECISION = 3000;
    private final Expression precision;

    public CountDistinct(Source source, Expression field, Expression precision) {
        super(source, field, precision != null ? List.of(precision) : List.of());
        this.precision = precision;
    }

    @Override
    protected NodeInfo<CountDistinct> info() {
        return NodeInfo.create(this, CountDistinct::new, field(), precision);
    }

    @Override
    public CountDistinct replaceChildren(List<Expression> newChildren) {
        return new CountDistinct(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    public DataType dataType() {
        return DataTypes.LONG;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = super.resolveType();
        if (resolution.unresolved() || precision == null) {
            return resolution;
        }

        return isInteger(precision, sourceText(), SECOND);
    }

    @Override
    public AggregatorFunctionSupplier supplier(BigArrays bigArrays, int inputChannel) {
        DataType type = field().dataType();
        int precision = this.precision == null ? DEFAULT_PRECISION : ((Number) this.precision.fold()).intValue();
        if (type == DataTypes.BOOLEAN) {
            // Booleans ignore the precision because there are only two possible values anyway
            return new CountDistinctBooleanAggregatorFunctionSupplier(bigArrays, inputChannel);
        }
        if (type == DataTypes.DATETIME || type == DataTypes.LONG) {
            return new CountDistinctLongAggregatorFunctionSupplier(bigArrays, inputChannel, precision);
        }
        if (type == DataTypes.INTEGER) {
            return new CountDistinctIntAggregatorFunctionSupplier(bigArrays, inputChannel, precision);
        }
        if (type == DataTypes.DOUBLE) {
            return new CountDistinctDoubleAggregatorFunctionSupplier(bigArrays, inputChannel, precision);
        }
        if (type == DataTypes.KEYWORD || type == DataTypes.IP) {
            return new CountDistinctBytesRefAggregatorFunctionSupplier(bigArrays, inputChannel, precision);
        }
        throw new UnsupportedOperationException();
    }
}
