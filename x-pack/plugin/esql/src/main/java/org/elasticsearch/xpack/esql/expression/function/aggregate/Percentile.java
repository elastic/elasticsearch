/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public class Percentile extends NumericAggregate {
    private final Expression percentile;

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "The value at which a certain percentage of observed values occur.",
        isAggregation = true
    )
    public Percentile(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }) Expression field,
        @Param(name = "percentile", type = { "double", "integer", "long" }) Expression percentile
    ) {
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

        TypeResolution resolution = isType(
            field(),
            dt -> dt.isNumeric() && dt != DataTypes.UNSIGNED_LONG,
            sourceText(),
            FIRST,
            "numeric except unsigned_long"
        );
        if (resolution.unresolved()) {
            return resolution;
        }

        return isNumeric(percentile, sourceText(), SECOND).and(isFoldable(percentile, sourceText(), SECOND));
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(List<Integer> inputChannels) {
        return new PercentileLongAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(List<Integer> inputChannels) {
        return new PercentileIntAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(List<Integer> inputChannels) {
        return new PercentileDoubleAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    private int percentileValue() {
        return ((Number) percentile.fold()).intValue();
    }
}
