/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AvgDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AvgIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AvgLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

@Experimental
public class Avg extends NumericAggregate {

    public Avg(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Avg> info() {
        return NodeInfo.create(this, Avg::new, field());
    }

    @Override
    public Avg replaceChildren(List<Expression> newChildren) {
        return new Avg(source(), newChildren.get(0));
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(BigArrays bigArrays, int inputChannel) {
        return new AvgLongAggregatorFunctionSupplier(bigArrays, inputChannel);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(BigArrays bigArrays, int inputChannel) {
        return new AvgIntAggregatorFunctionSupplier(bigArrays, inputChannel);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(BigArrays bigArrays, int inputChannel) {
        return new AvgDoubleAggregatorFunctionSupplier(bigArrays, inputChannel);
    }
}
