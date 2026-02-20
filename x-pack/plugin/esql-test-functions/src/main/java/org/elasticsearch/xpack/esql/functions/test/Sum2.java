/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.runtime.RuntimeAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Test aggregate function for runtime aggregator generation.
 *
 * <p>This function is equivalent to {@code SUM()} but uses runtime bytecode
 * generation via {@link RuntimeAggregatorFunctionSupplier} instead of
 * compile-time code generation.
 *
 * <p>Example ES|QL usage:
 * <pre>
 * FROM test | STATS total = sum2(value)
 * FROM test | STATS total = sum2(value) BY category
 * </pre>
 */
public class Sum2 extends AggregateFunction implements ToAggregator {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Sum2", Sum2::new);

    @FunctionInfo(
        returnType = { "long" },
        description = "Sum of integer values (test function using runtime aggregator generation).",
        type = FunctionType.AGGREGATE
    )
    public Sum2(Source source, @Param(name = "number", type = { "integer" }, description = "Integer expression to sum.") Expression field) {
        super(source, field, Literal.TRUE, NO_WINDOW, List.of());
    }

    public Sum2(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, List.of());
    }

    private Sum2(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in)
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Sum2> info() {
        return NodeInfo.create(this, Sum2::new, field(), filter(), window());
    }

    @Override
    public Sum2 replaceChildren(List<Expression> newChildren) {
        return new Sum2(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public Sum2 withFilter(Expression filter) {
        return new Sum2(source(), field(), filter, window());
    }

    @Override
    public DataType dataType() {
        return LONG;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt == DataType.INTEGER, sourceText(), DEFAULT, "integer");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new RuntimeAggregatorFunctionSupplier(Sum2Aggregator.class, "sum2 of ints");
    }
}
