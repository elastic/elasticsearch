/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Percentile extends NumericAggregate {
    private final Expression percentile;

    public Percentile(Source source, Expression field, Expression percentile) {
        super(source, field, List.of(percentile));
        this.percentile = percentile;
    }

    @Override
    protected NodeInfo<Percentile> info() {
        return NodeInfo.create(this, Percentile::new, field(), percentile);
    }

    @Override
    public Percentile replaceChildren(List<Expression> newChildren) {
        return new Percentile(source(), newChildren.get(0), newChildren.get(1));
    }

    public Expression percentile() {
        return percentile;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isNumeric(field(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isNumeric(percentile, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return isFoldable(percentile, sourceText(), SECOND);
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new PercentileLongAggregatorFunctionSupplier(bigArrays, inputChannels, percentileValue());
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new PercentileIntAggregatorFunctionSupplier(bigArrays, inputChannels, percentileValue());
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new PercentileDoubleAggregatorFunctionSupplier(bigArrays, inputChannels, percentileValue());
    }

    private int percentileValue() {
        return ((Number) percentile.fold()).intValue();
    }
}
