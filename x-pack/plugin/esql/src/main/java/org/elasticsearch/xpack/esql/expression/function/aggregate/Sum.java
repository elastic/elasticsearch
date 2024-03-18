/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.List;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

/**
 * Sum all values of a field in matching documents.
 */
public class Sum extends NumericAggregate implements SurrogateExpression {

    @FunctionInfo(returnType = "long", description = "The sum of a numeric field.", isAggregation = true)
    public Sum(Source source, @Param(name = "field", type = { "double", "integer", "long" }) Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Sum> info() {
        return NodeInfo.create(this, Sum::new, field());
    }

    @Override
    public Sum replaceChildren(List<Expression> newChildren) {
        return new Sum(source(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        DataType dt = field().dataType();
        return dt.isInteger() == false || dt == UNSIGNED_LONG ? DOUBLE : LONG;
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(List<Integer> inputChannels) {
        return new SumLongAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(List<Integer> inputChannels) {
        return new SumIntAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(List<Integer> inputChannels) {
        return new SumDoubleAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    public Expression surrogate() {
        // SUM(const) is equivalent to MV_SUM(const)*COUNT(*).
        if (field().foldable()) {
            return new Mul(
                source(),
                new MvSum(Source.EMPTY, field()),
                new Count(Source.EMPTY, new Literal(Source.EMPTY, StringUtils.WILDCARD, DataTypes.KEYWORD))
            );
        }

        return null;
    }
}
