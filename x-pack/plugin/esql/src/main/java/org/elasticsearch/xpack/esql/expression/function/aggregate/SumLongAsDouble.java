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
import org.elasticsearch.compute.aggregation.SumLongAsDoubleAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * The sum of a {@code long} as a {@code double}: {@code SUM(TO_DOUBLE(x))} for a {@code long} {@code x}, without converting every
 * value to a {@code double} and without a floating point accumulator, see {@link SumLongAsDoubleAggregatorFunctionSupplier}.
 * <p>
 *     This is an implementation detail that only the local, data node, plan uses: {@code LocalLogicalPlanOptimizer} swaps it in
 *     for the {@link Sum} that the {@link Avg} of a {@code long} is rewritten to. It is never part of the plan the coordinator
 *     sends to the data nodes, and it has the intermediate state of {@code SUM} of a {@code double}, so a cluster with nodes
 *     that don't have it keeps working. That is why it needs no transport version, and why it is not in the function registry.
 *     It is registered for serialization only so that every plan node can be written and read, as the other aggregates can.
 * </p>
 */
public class SumLongAsDouble extends NumericAggregate {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SumLongAsDouble",
        SumLongAsDouble::new
    );

    public SumLongAsDouble(Source source, Expression field) {
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public SumLongAsDouble(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, List.of());
    }

    private SumLongAsDouble(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<SumLongAsDouble> info() {
        return NodeInfo.create(this, SumLongAsDouble::new, field(), filter(), window());
    }

    @Override
    public SumLongAsDouble replaceChildren(List<Expression> newChildren) {
        return new SumLongAsDouble(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt == DataType.LONG, sourceText(), DEFAULT, "long");
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier() {
        return new SumLongAsDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier() {
        throw new UnsupportedOperationException("only supports long");
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier() {
        throw new UnsupportedOperationException("only supports long");
    }
}
