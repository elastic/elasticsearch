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
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Test aggregate function for runtime aggregator generation with BytesRef state.
 *
 * <p>This function finds the maximum string value (lexicographically) using runtime
 * bytecode generation via {@link RuntimeAggregatorFunctionSupplier}.
 *
 * <p>Example ES|QL usage:
 * <pre>
 * FROM test | STATS max_name = max_bytes2(name)
 * FROM test | STATS max_name = max_bytes2(name) BY category
 * </pre>
 */
public class MaxBytes2 extends AggregateFunction implements ToAggregator {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MaxBytes2",
        MaxBytes2::new
    );

    @FunctionInfo(
        returnType = { "keyword" },
        description = "Maximum string value (test function using runtime aggregator generation with BytesRef state).",
        type = FunctionType.AGGREGATE
    )
    public MaxBytes2(
        Source source,
        @Param(name = "string", type = { "keyword" }, description = "String expression to find maximum.") Expression field
    ) {
        super(source, field, Literal.TRUE, NO_WINDOW, List.of());
    }

    public MaxBytes2(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, List.of());
    }

    private MaxBytes2(StreamInput in) throws IOException {
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
    protected NodeInfo<? extends MaxBytes2> info() {
        return NodeInfo.create(this, MaxBytes2::new, field(), filter(), window());
    }

    @Override
    public MaxBytes2 replaceChildren(List<Expression> newChildren) {
        return new MaxBytes2(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public MaxBytes2 withFilter(Expression filter) {
        return new MaxBytes2(source(), field(), filter, window());
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt == DataType.KEYWORD || dt == DataType.TEXT, sourceText(), DEFAULT, "keyword or text");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new RuntimeAggregatorFunctionSupplier(MaxBytes2Aggregator.class, "max_bytes2 of strings");
    }
}
