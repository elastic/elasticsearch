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
import org.elasticsearch.compute.aggregation.MaxBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.type.AtomType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.AtomType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.AtomType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.AtomType.IP;
import static org.elasticsearch.xpack.esql.core.type.AtomType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.AtomType.LONG;
import static org.elasticsearch.xpack.esql.core.type.AtomType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.AtomType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.AtomType.VERSION;

public class Max extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Max", Max::new);

    private static final Map<DataType, Supplier<AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(BOOLEAN.type(), MaxBooleanAggregatorFunctionSupplier::new),
        Map.entry(LONG.type(), MaxLongAggregatorFunctionSupplier::new),
        Map.entry(UNSIGNED_LONG.type(), MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DATETIME.type(), MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DATE_NANOS.type(), MaxLongAggregatorFunctionSupplier::new),
        Map.entry(INTEGER.type(), MaxIntAggregatorFunctionSupplier::new),
        Map.entry(DOUBLE.type(), MaxDoubleAggregatorFunctionSupplier::new),
        Map.entry(IP.type(), MaxIpAggregatorFunctionSupplier::new),
        Map.entry(KEYWORD.type(), MaxBytesRefAggregatorFunctionSupplier::new),
        Map.entry(TEXT.type(), MaxBytesRefAggregatorFunctionSupplier::new),
        Map.entry(VERSION.type(), MaxBytesRefAggregatorFunctionSupplier::new)
    );

    @FunctionInfo(
        returnType = { "boolean", "double", "integer", "long", "date", "date_nanos", "ip", "keyword", "unsigned_long", "version" },
        description = "The maximum value of a field.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "max"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the maximum "
                    + "over an average of a multivalued column, use `MV_AVG` to first average the "
                    + "multiple values per row, and use the result with the `MAX` function",
                file = "stats",
                tag = "docsStatsMaxNestedExpression"
            ) }
    )
    public Max(
        Source source,
        @Param(
            name = "field",
            type = {
                "aggregate_metric_double",
                "boolean",
                "double",
                "integer",
                "long",
                "date",
                "date_nanos",
                "ip",
                "keyword",
                "text",
                "unsigned_long",
                "version" }
        ) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public Max(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private Max(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Max withFilter(Expression filter) {
        return new Max(source(), field(), filter);
    }

    @Override
    protected NodeInfo<Max> info() {
        return NodeInfo.create(this, Max::new, field(), filter());
    }

    @Override
    public Max replaceChildren(List<Expression> newChildren) {
        return new Max(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(
            field(),
            dt -> SUPPLIERS.containsKey(dt) || dt.atom() == AGGREGATE_METRIC_DOUBLE,
            sourceText(),
            DEFAULT,
            "boolean",
            "date",
            "ip",
            "string",
            "version",
            "aggregate_metric_double",
            "numeric except counter types"
        );
    }

    @Override
    public DataType dataType() {
        if (field().dataType().atom() == AGGREGATE_METRIC_DOUBLE) {
            return DOUBLE.type();
        }
        return field().dataType().noText();
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (SUPPLIERS.containsKey(type) == false) {
            // If the type checking did its job, this should never happen
            throw EsqlIllegalArgumentException.illegalDataType(type);
        }
        return SUPPLIERS.get(type).get();
    }

    @Override
    public Expression surrogate() {
        if (field().dataType().atom() == AGGREGATE_METRIC_DOUBLE) {
            return new Max(source(), FromAggregateMetricDouble.withMetric(source(), field(), AggregateMetricDoubleBlockBuilder.Metric.MAX));
        }
        return field().foldable() ? new MvMax(source(), field()) : null;
    }
}
