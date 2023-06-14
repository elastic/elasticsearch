/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

/**
 * Sum all values of a field in matching documents.
 */
public class Sum extends NumericAggregate {

    public Sum(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Sum> info() {
        return NodeInfo.create(this, Sum::new, field());
    }

    @Override
    public Sum replaceChildren(List<Expression> newChildren) {
        return new Sum(source(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        DataType dt = field().dataType();
        return dt.isInteger() == false || dt == UNSIGNED_LONG ? DOUBLE : LONG;
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(BigArrays bigArrays, int inputChannel) {
        return new SumLongAggregatorFunctionSupplier(bigArrays, inputChannel);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(BigArrays bigArrays, int inputChannel) {
        return new SumIntAggregatorFunctionSupplier(bigArrays, inputChannel);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(BigArrays bigArrays, int inputChannel) {
        return new SumDoubleAggregatorFunctionSupplier(bigArrays, inputChannel);
    }
}
