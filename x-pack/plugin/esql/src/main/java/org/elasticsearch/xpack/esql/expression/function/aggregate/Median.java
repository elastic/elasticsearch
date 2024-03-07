/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.expression.Literal;
import org.elasticsearch.xpack.qlcore.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;
import org.elasticsearch.xpack.qlcore.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.qlcore.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.qlcore.expression.TypeResolutions.isType;

public class Median extends AggregateFunction implements SurrogateExpression {
    // TODO: Add the compression parameter
    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "The value that is greater than half of all values and less than half of all values.",
        isAggregation = true
    )
    public Median(Source source, @Param(name = "field", type = { "double", "integer", "long" }) Expression field) {
        super(source, field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataTypes.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long"
        );
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    protected NodeInfo<Median> info() {
        return NodeInfo.create(this, Median::new, field());
    }

    @Override
    public Median replaceChildren(List<Expression> newChildren) {
        return new Median(source(), newChildren.get(0));
    }

    @Override
    public Expression surrogate() {
        return new Percentile(source(), field(), new Literal(source(), (int) QuantileStates.MEDIAN, DataTypes.INTEGER));
    }
}
