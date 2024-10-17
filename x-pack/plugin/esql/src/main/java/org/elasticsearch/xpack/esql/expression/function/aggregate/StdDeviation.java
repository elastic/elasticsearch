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
import org.elasticsearch.compute.aggregation.StdDeviationDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDeviationIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDeviationLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;

public class StdDeviation extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StdDeviation",
        StdDeviation::new
    );

    @FunctionInfo(
        returnType = "double",
        description = "The standard deviation of a numeric field.",
        isAggregation = true,
        examples = {
            @Example(file = "stats", tag = "stdev"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the standard "
                    + "deviation of each employee's maximum salary changes, first use `MV_MAX` on each row, "
                    + "and then use `StdDeviation` on the result",
                file = "stats",
                tag = "docsStatsStdDeviationNestedExpression"
            ) }
    )
    public StdDeviation(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        this(source, field, Literal.TRUE);
    }

    public StdDeviation(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private StdDeviation(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected NodeInfo<StdDeviation> info() {
        return NodeInfo.create(this, StdDeviation::new, field(), filter());
    }

    @Override
    public StdDeviation replaceChildren(List<Expression> newChildren) {
        return new StdDeviation(source(), newChildren.get(0), newChildren.get(1));
    }

    public StdDeviation withFilter(Expression filter) {
        return new StdDeviation(source(), field(), filter);
    }

    @Override
    public final AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataType.LONG) {
            return new StdDeviationLongAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.INTEGER) {
            return new StdDeviationIntAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.DOUBLE) {
            return new StdDeviationDoubleAggregatorFunctionSupplier(inputChannels);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
