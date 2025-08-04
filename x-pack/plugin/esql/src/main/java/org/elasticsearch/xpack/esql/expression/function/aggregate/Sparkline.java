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
import org.elasticsearch.compute.aggregation.SparklineDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SparklineFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SparklineIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SparklineLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class Sparkline extends AggregateFunction implements OptionalArgument, ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Sparkline",
        Sparkline::new
    );

    public Sparkline(Source source, Expression field, Expression filter, Expression sortField) {
        super(source, field, filter, sortField != null ? List.of(sortField) : List.of());
    }

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "date_nanos",
            "double",
            "geo_point",
            "geo_shape",
            "integer",
            "ip",
            "keyword",
            "long",
            "unsigned_long",
            "version" },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = """
            Returns values as a multivalued field. The order of the returned values is determined by the given sortField.""",
        appendix = """
            ::::{tip}
            Use [`TOP`](/reference/query-languages/esql/functions-operators/aggregation-functions.md#esql-top)
            if you need to keep repeated values.
            ::::
            ::::{warning}
            This can use a significant amount of memory and ES|QL doesn’t yet
            grow aggregations beyond memory. So this aggregation will work until
            it is used to collect more values than can fit into memory. Once it
            collects too many values it will fail the query with
            a [Circuit Breaker Error](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).
            ::::""",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "string", tag = "values-grouped")
    )
    public Sparkline(
        Source source,
        @Param(
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "unsigned_long",
                "text",
                "version" }
        ) Expression v,
        @Param(name = "sortField", type = { "date" }) Expression sortField
    ) {
        this(source, v, Literal.TRUE, sortField);
    }

    private Sparkline(StreamInput in) throws IOException {
        super(in);
    }

    public Expression sortField() {
        return parameters().get(0);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Sparkline> info() {
        return NodeInfo.create(this, Sparkline::new, field(), filter(), sortField());
    }

    @Override
    public Sparkline replaceChildren(List<Expression> newChildren) {
        return new Sparkline(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public Sparkline withFilter(Expression filter) {
        return new Sparkline(source(), field(), filter, sortField());
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isRepresentableExceptCounters(field(), sourceText(), DEFAULT);
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type == DataType.LONG || type == DataType.DATETIME) {
            return new SparklineLongAggregatorFunctionSupplier(limitValue(), ascending());
        }
        if (type == DataType.INTEGER) {
            return new SparklineIntAggregatorFunctionSupplier(limitValue(), ascending());
        }
        if (type == DataType.DOUBLE) {
            return new SparklineDoubleAggregatorFunctionSupplier(limitValue(), ascending());
        }
        if (type == DataType.FLOAT) {
            return new SparklineFloatAggregatorFunctionSupplier(limitValue(), ascending());
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    private Integer limitValue() {
        return 100;
    }

    private boolean ascending() {
        return true;
    }
}
