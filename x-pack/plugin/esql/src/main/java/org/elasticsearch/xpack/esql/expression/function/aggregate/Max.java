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
import org.elasticsearch.compute.operator.DriverContext;
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
    protected AggregatorFunctionSupplier longSupplier(DriverContext driverContext, List<Integer> inputChannels) {
        return new MaxLongAggregatorFunctionSupplier(driverContext, inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(DriverContext driverContext, List<Integer> inputChannels) {
        return new MaxIntAggregatorFunctionSupplier(driverContext, inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(DriverContext driverContext, List<Integer> inputChannels) {
        return new MaxDoubleAggregatorFunctionSupplier(driverContext, inputChannels);
    }
}
