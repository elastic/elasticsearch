/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastValueDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastValueIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastValueLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class LastValue extends NumericAggregate {

    public LastValue(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<LastValue> info() {
        return NodeInfo.create(this, LastValue::new, field());
    }

    @Override
    public LastValue replaceChildren(List<Expression> newChildren) {
        return new LastValue(source(), newChildren.get(0));
    }

    @Override
    protected boolean supportsDates() {
        return true;
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new LastValueLongAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new LastValueIntAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new LastValueDoubleAggregatorFunctionSupplier(bigArrays, inputChannels);
    }
}
