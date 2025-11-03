/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DerivDoubleGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.DerivIntGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.DerivLongGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SimpleLinearRegressionWithTimeseries;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Calculates the derivative over time of a numeric field using linear regression.
 */
public class PredictLinear extends TimeSeriesAggregateFunction implements ToAggregator {
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
        description = "Predicts the value of a time series at `t` seconds in the future."
    )
    public PredictLinear(
        Source source,
        @Param(name = "field", type = { "long", "integer", "double" }) Expression field,
        @Param(name = "t", type = { "long", "integer", "keyword", "timedelta" }) Expression t,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, timestamp, t);
    }

    public PredictLinear(Source source, Expression field, Expression filter, Expression window, Expression timestamp, Expression t) {
        super(source, field, filter, window, List.of(timestamp, t));
        this.timestamp = timestamp;
        this.t = t;
    }

    private PredictLinear(org.elasticsearch.common.io.stream.StreamInput in) throws java.io.IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
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
        return new PredictLinear(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
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
        final Double timeDiffSeconds = switch (t.fold(FoldContext.small())) {
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
            double lastTs = model.lastTimestamp();
            if (Double.isNaN(slope)) {
                return Double.NaN;
            }
            return intercept + slope * (lastTs + timeDiffSeconds);
        };
        final DataType type = field().dataType();
        return switch (type) {
            case INTEGER -> new DerivIntGroupingAggregatorFunction.Supplier(fn);
            case LONG -> new DerivLongGroupingAggregatorFunction.Supplier(fn);
            case DOUBLE -> new DerivDoubleGroupingAggregatorFunction.Supplier(fn);
            default -> throw new IllegalArgumentException("Unsupported data type for deriv aggregation: " + type);
        };
    }
}
