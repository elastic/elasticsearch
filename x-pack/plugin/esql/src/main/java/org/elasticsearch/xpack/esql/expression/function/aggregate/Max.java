/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

public class Max extends NumericAggregate {

    public Max(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Max> info() {
        return NodeInfo.create(this, Max::new, field());
    }

    @Override
    public Max replaceChildren(List<Expression> newChildren) {
        return new Max(source(), newChildren.get(0));
    }

    @Override
    protected boolean supportsDates() {
        return true;
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new MaxLongAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new MaxIntAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new MaxDoubleAggregatorFunctionSupplier(bigArrays, inputChannels);
    }
}
