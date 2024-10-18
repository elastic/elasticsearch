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
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

/**
 * Sum all values of a field in matching documents.
 */
public class Sum extends NumericAggregate implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Sum", Sum::new);

    @FunctionInfo(
        returnType = { "long", "double" },
        description = "The sum of a numeric expression.",
        isAggregation = true,
        examples = {
            @Example(file = "stats", tag = "sum"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate "
                    + "the sum of each employee's maximum salary changes, apply the "
                    + "`MV_MAX` function to each row and then sum the results",
                file = "stats",
                tag = "docsStatsSumNestedExpression"
            ) }
    )
    public Sum(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        this(source, field, Literal.TRUE);
    }

    public Sum(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private Sum(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Sum> info() {
        return NodeInfo.create(this, Sum::new, field(), filter());
    }

    @Override
    public Sum replaceChildren(List<Expression> newChildren) {
        return new Sum(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public Sum withFilter(Expression filter) {
        return new Sum(source(), field(), filter);
    }

    @Override
    public DataType dataType() {
        DataType dt = field().dataType();
        return dt.isWholeNumber() == false || dt == UNSIGNED_LONG ? DOUBLE : LONG;
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
        var s = source();
        var field = field();

        // SUM(const) is equivalent to MV_SUM(const)*COUNT(*).
        return field.foldable()
            ? new Mul(s, new MvSum(s, field), new Count(s, new Literal(s, StringUtils.WILDCARD, DataType.KEYWORD)))
            : null;
    }
}
