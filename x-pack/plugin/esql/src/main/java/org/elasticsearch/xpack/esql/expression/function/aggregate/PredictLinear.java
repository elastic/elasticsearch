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
import org.elasticsearch.compute.aggregation.PredictLinearDoubleAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/** Forecasts each series at the evaluation time plus a horizon in seconds using ordinary least squares. */
public class PredictLinear extends TimeSeriesAggregateFunction implements TimestampAware, ToAggregator, AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PredictLinear",
        PredictLinear::readFrom
    );
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeriesRangeFirst(
            PromqlFunctionDefinition.PromqlParamInfo.of("t", PromqlDataType.SCALAR, "Prediction horizon in seconds."),
            (source, field, ctx, params) -> {
                Expression timestamp = ctx.originalTimestamp();
                if (timestamp.dataType() == DataType.DATE_NANOS) {
                    timestamp = new ToDatetime(source, timestamp, ctx.configuration());
                }
                // A field has one mapped type. Histogram-only ranges therefore contribute no float samples.
                Expression values = field.dataType().isHistogram()
                    ? new Literal(source, null, DataType.DOUBLE)
                    : new MvSingleValueOrNull(source, new ToDouble(source, field));
                return new PredictLinear(source, values, timestamp, Literal.TRUE, ctx.window(), params.getFirst());
            }
        )
        .description("Predicts the value of each time series t seconds after the evaluation time using simple linear regression.")
        .extendedDescription(
            "Requires at least two float samples and should be used with gauges. Native histogram samples are ignored. "
                + "Negative and fractional prediction horizons are supported. "
                + "Duration literals retain millisecond precision: `500ms` is equivalent to `0.5` seconds."
        )
        .differenceFromPrometheus(
            "The prediction horizon must be a number, a duration literal, or arithmetic on literals. "
                + "Scalar functions such as `time()` and `scalar(...)` are not supported. "
                + "A single Elasticsearch field has a fixed type, so mixed float and native histogram ranges cannot be represented."
        )
        .example("predict_linear(node_filesystem_free_bytes[1h], 3600)")
        .stack(PromqlFunctionDefinition.STACK_GA_9_6)
        .name("predict_linear");

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        briefSummary = "Predicts a value using linear regression.",
        description = "Predicts a time-series value at the evaluation time plus a horizon in seconds.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.6.0") }
    )
    public PredictLinear(
        Source source,
        @Param(name = "field", type = { "double" }, description = "the metric field") Expression field,
        Expression timestamp,
        Expression filter,
        Expression window,
        @Param(
            name = "horizon",
            type = { "double", "integer", "long" },
            description = "the prediction horizon in seconds"
        ) Expression horizon
    ) {
        super(source, List.of(field, timestamp), filter, window, List.of(horizon));
    }

    private static PredictLinear readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> arguments = in.readNamedWriteableCollectionAsList(Expression.class);
        return new PredictLinear(source, field, arguments.get(0), filter, window, arguments.get(1));
    }

    @Override
    public Expression timestamp() {
        return fields().get(1);
    }

    public Expression horizon() {
        return parameters().getFirst();
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt == DataType.DOUBLE, sourceText(), FIRST, "double").and(
            isType(horizon(), DataType::isNumeric, sourceText(), SECOND, "numeric")
        ).and(isFoldable(horizon(), sourceText(), SECOND));
    }

    @Override
    public PredictLinear perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public Expression replaceChildren(List<Expression> children) {
        return new PredictLinear(source(), children.get(0), children.get(1), children.get(2), children.get(3), children.get(4));
    }

    @Override
    protected NodeInfo<PredictLinear> info() {
        return NodeInfo.create(this, PredictLinear::new, field(), timestamp(), filter(), window(), horizon());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new PredictLinearDoubleAggregatorFunctionSupplier(Foldables.doubleValueOf(horizon(), sourceText(), "horizon"));
    }
}
