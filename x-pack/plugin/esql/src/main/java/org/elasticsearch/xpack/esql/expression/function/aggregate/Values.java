/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class Values extends AggregateFunction implements ToAggregator {
    @FunctionInfo(
        returnType = { "boolean|date|double|integer|ip|keyword|long|text|version" },
        description = "Collect values for a field.",
        isAggregation = true
    )
    public Values(
        Source source,
        @Param(name = "field", type = { "boolean|date|double|integer|ip|keyword|long|text|version" }) Expression v
    ) {
        super(source, v);
    }

    @Override
    protected NodeInfo<Values> info() {
        return NodeInfo.create(this, Values::new, field());
    }

    @Override
    public Values replaceChildren(List<Expression> newChildren) {
        return new Values(source(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        return EsqlTypeResolutions.isNotSpatial(field(), sourceText(), DEFAULT);
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataType.INTEGER) {
            return new ValuesIntAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.LONG || type == DataType.DATETIME) {
            return new ValuesLongAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.DOUBLE) {
            return new ValuesDoubleAggregatorFunctionSupplier(inputChannels);
        }
        if (DataType.isString(type) || type == DataType.IP || type == DataType.VERSION) {
            return new ValuesBytesRefAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.BOOLEAN) {
            return new ValuesBooleanAggregatorFunctionSupplier(inputChannels);
        }
        // TODO cartesian_point, geo_point
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
