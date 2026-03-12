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
import org.elasticsearch.compute.aggregation.StdDevDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Variance extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Variance", Variance::new);

    @FunctionInfo(
        returnType = "double",
        description = "The population variance of a numeric field.",
        type = FunctionType.AGGREGATE,
        examples = { @Example(file = "stats", tag = "variance") }
    )
    public Variance(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public Variance(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    private Variance(StreamInput in) throws IOException {
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
    protected Expression.TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long or counter types"
        );
    }

    @Override
    protected NodeInfo<Variance> info() {
        return NodeInfo.create(this, Variance::new, field(), filter(), window());
    }

    @Override
    public Variance replaceChildren(List<Expression> newChildren) {
        return new Variance(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    public Variance withFilter(Expression filter) {
        return new Variance(source(), field(), filter, window());
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type == DataType.LONG) {
            return new StdDevLongAggregatorFunctionSupplier(false);
        }
        if (type == DataType.INTEGER) {
            return new StdDevIntAggregatorFunctionSupplier(false);
        }
        if (type == DataType.DOUBLE) {
            return new StdDevDoubleAggregatorFunctionSupplier(false);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
