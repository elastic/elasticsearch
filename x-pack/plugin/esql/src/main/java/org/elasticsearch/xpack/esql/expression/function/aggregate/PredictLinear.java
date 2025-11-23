/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DerivDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DerivIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DerivLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SimpleLinearRegressionWithTimeseries;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Calculates the derivative over time of a numeric field using linear regression.
 */
public class PredictLinear extends TimeSeriesAggregateFunction implements ToAggregator, TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PredictLinear",
        PredictLinear::new
    );
    private final Expression timestamp;
    private final Expression t;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Predicts the value of a time series at `t` seconds in the future.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries", tag = "predict_linear") }
    )
    public PredictLinear(
        Source source,
        @Param(
            name = "field",
            type = { "long", "integer", "double" },
            description = "the expression to use for the prediction"
        ) Expression field,
        @Param(
            name = "t",
            type = { "long", "integer", "time_duration", "double" },
            description = "how long in the fututre to predict in seconds for numeric, or in time delta"
        ) Expression t,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, timestamp, t);
    }

    public PredictLinear(Source source, Expression field, Expression filter, Expression window, Expression ts, Expression t) {
        super(source, field, filter, window, List.of(ts, t));
        this.timestamp = ts;
        this.t = t;
    }

    private PredictLinear(org.elasticsearch.common.io.stream.StreamInput in) throws java.io.IOException {
        super(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
        this.t = children().get(4);
        this.timestamp = children().get(3);
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new PredictLinear(source(), field(), filter, window(), timestamp, t);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new PredictLinear(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4)
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, PredictLinear::new, field(), filter(), window(), timestamp, t);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        if (t.foldable() == false) {
            throw new IllegalArgumentException("The 't' parameter of the 'predict_linear' function must be a constant value.");
        }
        final double timeDiffSeconds = switch (t.fold(FoldContext.small())) {
            case Duration d -> d.toMillis() / 1000.0;
            case Long l -> l.doubleValue();
            case Integer i -> i.doubleValue();
            case String s -> Duration.from(Objects.requireNonNull(EsqlDataTypeConverter.parseTemporalAmount(s, DataType.TIME_DURATION)))
                .toMillis() / 1000.0;
            default -> throw new IllegalArgumentException(
                "The 't' parameter of the 'predict_linear' function must be of type long, integer, keyword, or timedelta. It was of type: "
                    + t.dataType()
            );
        };

        SimpleLinearRegressionWithTimeseries.SimpleLinearModelFunction fn = (SimpleLinearRegressionWithTimeseries model) -> {
            double slope = model.slope();
            double intercept = model.intercept();
            double lastTsSec = model.lastTimestamp() / 1000.0;
            if (Double.isNaN(slope)) {
                return Double.NaN;
            }
            return intercept + slope * (lastTsSec + timeDiffSeconds);
        };
        final DataType type = field().dataType();
        final boolean isDateNanos = timestamp.dataType() == DataType.DATE_NANOS;
        return switch (type) {
            case LONG -> new DerivLongAggregatorFunctionSupplier(fn, isDateNanos);
            case INTEGER -> new DerivIntAggregatorFunctionSupplier(fn, isDateNanos);
            case DOUBLE -> new DerivDoubleAggregatorFunctionSupplier(fn, isDateNanos);
            default -> throw new IllegalArgumentException("Unsupported data type for deriv aggregation: " + type);
        };
    }
}
