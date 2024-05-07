/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public class Count extends AggregateFunction implements EnclosedAgg, ToAggregator, SurrogateExpression {

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
        return isType(field(), dt -> EsqlDataTypes.isCounterType(dt) == false, sourceText(), DEFAULT, "any type except counter types");
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();

        if (field.foldable()) {
            if (field instanceof Literal l) {
                if (l.value() != null && (l.value() instanceof List<?>) == false) {
                    // TODO: Normalize COUNT(*), COUNT(), COUNT("foobar"), COUNT(1) as COUNT(*).
                    // Does not apply to COUNT([1,2,3])
                    // return new Count(s, new Literal(s, StringUtils.WILDCARD, DataTypes.KEYWORD));
                    return null;
                }
            }

            // COUNT(const) is equivalent to MV_COUNT(const)*COUNT(*) if const is not null; otherwise COUNT(const) == 0.
            return new Mul(
                s,
                new Coalesce(s, new MvCount(s, field), List.of(new Literal(s, 0, DataTypes.INTEGER))),
                new Count(s, new Literal(s, StringUtils.WILDCARD, DataTypes.KEYWORD))
            );
        }

        return null;
    }
}
