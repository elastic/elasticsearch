/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MedianAbsoluteDeviationDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MedianAbsoluteDeviationIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MedianAbsoluteDeviationLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

public class MedianAbsoluteDeviation extends NumericAggregate {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MedianAbsoluteDeviation",
        MedianAbsoluteDeviation::new
    );

    // TODO: Add parameter
    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "The median absolute deviation, a measure of variability.",
        isAggregation = true
    )
    public MedianAbsoluteDeviation(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        super(source, field);
    }

    private MedianAbsoluteDeviation(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<MedianAbsoluteDeviation> info() {
        return NodeInfo.create(this, MedianAbsoluteDeviation::new, field());
    }

    @Override
    public MedianAbsoluteDeviation replaceChildren(List<Expression> newChildren) {
        return new MedianAbsoluteDeviation(source(), newChildren.get(0));
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(List<Integer> inputChannels) {
        return new MedianAbsoluteDeviationLongAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(List<Integer> inputChannels) {
        return new MedianAbsoluteDeviationIntAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(List<Integer> inputChannels) {
        return new MedianAbsoluteDeviationDoubleAggregatorFunctionSupplier(inputChannels);
    }
}
