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
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

public class Values extends AggregateFunction implements ToAggregator {
    @FunctionInfo(
        returnType = { "boolean|cartesian_point|date|double|geo_point|integer|ip|keyword|long|text|version" },
        description = "Collect values for a field.",
        isAggregation = true
    )
    public Values(
        Source source,
        @Param(name = "v", type = { "boolean|cartesian_point|date|double|geo_point|integer|ip|keyword|long|text|version" }) Expression v
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
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataTypes.INTEGER) {
            return new ValuesIntAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataTypes.LONG) {
            return new ValuesLongAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataTypes.DOUBLE) {
            return new ValuesDoubleAggregatorFunctionSupplier(inputChannels);
        }
        if (DataTypes.isString(type) || type == DataTypes.IP || type == DataTypes.VERSION) {
            return new ValuesBytesRefAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataTypes.BOOLEAN) {
            return new ValuesBooleanAggregatorFunctionSupplier(inputChannels);
        }
        // TODO cartesian_point, geo_point
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
