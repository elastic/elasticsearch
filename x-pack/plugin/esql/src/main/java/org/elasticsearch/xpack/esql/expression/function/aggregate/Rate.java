/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.RateDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.RateIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.RateLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class Rate extends NumericAggregate {

    public Rate(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Rate> info() {
        return NodeInfo.create(this, Rate::new, field());
    }

    @Override
    public Rate replaceChildren(List<Expression> newChildren) {
        return new Rate(source(), newChildren.get(0));
    }

    @Override
    protected boolean supportsDates() {
        return true;
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new RateLongAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new RateIntAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new RateDoubleAggregatorFunctionSupplier(bigArrays, inputChannels);
    }
}
