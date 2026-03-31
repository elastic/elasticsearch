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
import org.elasticsearch.compute.aggregation.CountApproximateAggregatorFunction;
import org.elasticsearch.compute.aggregation.DenseVectorCountApproximateAggregatorFunction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

/**
 * Used exclusively in the query approximation plan.
 * <p>
 * Counts values by summing doubles, so that intermediate state is {@link DataType#DOUBLE}.
 * This avoids round-off errors when sample correction divides by the sample
 * probability on data nodes — the corrected value stays in floating point and
 * is only rounded to the target integer type on the coordinator.
 */
public class CountApproximate extends Count {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "CountApproximate",
        CountApproximate::new
    );

    public CountApproximate(Source source, Expression field) {
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public CountApproximate(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window);
    }

    private CountApproximate(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in)
        );
        in.readNamedWriteableCollectionAsList(Expression.class);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Count> info() {
        return NodeInfo.create(this, CountApproximate::new, field(), filter(), window());
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new CountApproximate(source(), field(), filter, window());
    }

    @Override
    public CountApproximate replaceChildren(List<Expression> newChildren) {
        return new CountApproximate(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        if (field().dataType() == DataType.DENSE_VECTOR) {
            return DenseVectorCountApproximateAggregatorFunction.supplier();
        }
        return CountApproximateAggregatorFunction.supplier();
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public Expression surrogate() {
        return null;
    }
}
