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
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public class Median extends AggregateFunction implements SurrogateExpression {
    // TODO: Add the compression parameter
    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "The value that is greater than half of all values and less than half of all values.",
        isAggregation = true
    )
    public Median(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        super(source, field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataTypes.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long or counter types"
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
        var s = source();
        var field = field();

        return field.foldable()
            ? new MvMedian(s, new ToDouble(s, field))
            : new Percentile(source(), field(), new Literal(source(), (int) QuantileStates.MEDIAN, DataTypes.INTEGER));
    }
}
