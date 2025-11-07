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
import org.elasticsearch.compute.aggregation.MergeExponentialHistogramAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Merges multiple histograms into a single histogram.
 * Note that this function is currently only intended for usage in surrogates and not available as a user-facing function.
 * Therefore, it is intentionally not registered in {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry}.
 */
public class Merge extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Merge", Merge::new);

    @FunctionInfo(returnType = "exponential_histogram", type = FunctionType.AGGREGATE)
    public Merge(Source source, @Param(name = "histogram", type = { "exponential_histogram" }) Expression field) {
        this(source, field, Literal.TRUE);
    }

    public Merge(Source source, Expression field, Expression filter) {
        super(source, field, filter, NO_WINDOW, emptyList());
    }

    private Merge(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.EXPONENTIAL_HISTOGRAM;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt == DataType.EXPONENTIAL_HISTOGRAM, sourceText(), DEFAULT, "exponential_histogram");
    }

    @Override
    protected NodeInfo<Merge> info() {
        return NodeInfo.create(this, Merge::new, field(), filter());
    }

    @Override
    public Merge replaceChildren(List<Expression> newChildren) {
        return new Merge(source(), newChildren.get(0), newChildren.get(1));
    }

    public Merge withFilter(Expression filter) {
        return new Merge(source(), field(), filter);
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type == DataType.EXPONENTIAL_HISTOGRAM) {
            return new MergeExponentialHistogramAggregatorFunctionSupplier();
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
