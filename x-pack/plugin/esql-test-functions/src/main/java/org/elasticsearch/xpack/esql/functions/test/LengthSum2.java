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
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Test aggregate function that sums the lengths of string values.
 * This tests BytesRef input handling with runtime aggregator generation.
 *
 * <p>Example ES|QL usage:
 * <pre>
 * FROM test | STATS total_length = length_sum2(name)
 * FROM test | STATS total_length = length_sum2(name) BY category
 * </pre>
 */
public class LengthSum2 extends AggregateFunction implements ToAggregator {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "LengthSum2",
        LengthSum2::new
    );

    @FunctionInfo(
        returnType = { "long" },
        description = "Sum of string lengths (test function using runtime aggregator generation with BytesRef input).",
        type = FunctionType.AGGREGATE
    )
    public LengthSum2(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "String expression to sum lengths of.") Expression field
    ) {
        super(source, field, Literal.TRUE, NO_WINDOW, List.of());
    }

    public LengthSum2(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, List.of());
    }

    private LengthSum2(StreamInput in) throws IOException {
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
    protected NodeInfo<? extends LengthSum2> info() {
        return NodeInfo.create(this, LengthSum2::new, field(), filter(), window());
    }

    @Override
    public LengthSum2 replaceChildren(List<Expression> newChildren) {
        return new LengthSum2(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public LengthSum2 withFilter(Expression filter) {
        return new LengthSum2(source(), field(), filter, window());
    }

    @Override
    public DataType dataType() {
        return LONG;
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new RuntimeAggregatorFunctionSupplier(LengthSum2Aggregator.class, "length_sum2 of strings");
    }
}
