/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class WeightedAvg extends AggregateFunction implements SurrogateExpression {

    private final Expression weight;

    @FunctionInfo(returnType = "double", description = "The weighted average of a numeric field.", isAggregation = true)
    public WeightedAvg(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }, description = "A numeric value.") Expression field,
        @Param(name = "weight", type = { "double", "integer", "long" }, description = "A numeric weight.") Expression weight
    ) {
        super(source, field, List.of(weight));
        this.weight = weight;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            FIRST,
            "numeric except unsigned_long or counter types"
        );

        if (resolution.unresolved()) {
            return resolution;
        }

        return isType(
            weight,
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            SECOND,
            "numeric except unsigned_long or counter types"
        );
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected NodeInfo<WeightedAvg> info() {
        return NodeInfo.create(this, WeightedAvg::new, field(), weight);
    }

    @Override
    public WeightedAvg replaceChildren(List<Expression> newChildren) {
        return new WeightedAvg(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();
        var weight = weight();

        return field.foldable() ? new MvAvg(s, field)
            : weight.foldable() ? new Div(s, new Sum(s, field), new Count(s, field), dataType())
            : new Div(s, new Sum(s, new Mul(s, field, weight)), new Sum(s, weight), dataType());
    }

    public Expression weight() {
        return weight;
    }
}
