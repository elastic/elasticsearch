/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.expression.Nullability;
import org.elasticsearch.xpack.qlcore.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.qlcore.expression.function.aggregate.EnclosedAgg;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;
import org.elasticsearch.xpack.qlcore.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.qlcore.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class Count extends AggregateFunction implements EnclosedAgg, ToAggregator {

    @FunctionInfo(returnType = "long", description = "Returns the total number (count) of input values.", isAggregation = true)
    public Count(
        Source source,
        @Param(
            optional = true,
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "date",
                "double",
                "geo_point",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Column or literal for which to count the number of values."
        ) Expression field
    ) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Count> info() {
        return NodeInfo.create(this, Count::new, field());
    }

    @Override
    public Count replaceChildren(List<Expression> newChildren) {
        return new Count(source(), newChildren.get(0));
    }

    @Override
    public String innerName() {
        return "count";
    }

    @Override
    public DataType dataType() {
        return DataTypes.LONG;
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        return CountAggregatorFunction.supplier(inputChannels);
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        return EsqlTypeResolutions.isExact(field(), sourceText(), DEFAULT);
    }
}
