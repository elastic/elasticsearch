/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.RateDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.RateIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.RateLongAggregatorFunctionSupplier;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Rate extends AggregateFunction implements OptionalArgument, ToAggregator {
    private static final TimeValue DEFAULT_UNIT = TimeValue.timeValueSeconds(1);

    private final Expression timestamp;
    private final Expression unit;

    @FunctionInfo(
        returnType = { "double" },
        description = "compute the rate of a counter field. Available in METRICS command only",
        isAggregation = true
    )

    public Rate(
        Source source,
        @Param(name = "field", type = { "counter_long|counter_integer|counter_double" }, description = "counter field") Expression field,
        Expression timestamp,
        @Param(optional = true, name = "unit", type = { "time_duration" }, description = "the unit") Expression unit
    ) {
        super(source, field, unit != null ? List.of(timestamp, unit) : List.of(timestamp));
        this.timestamp = timestamp;
        this.unit = unit;
    }

    public static Rate withUnresolvedTimestamp(Source source, Expression field, Expression unit) {
        return new Rate(source, field, new UnresolvedAttribute(source, "@timestamp"), unit);
    }

    public static Rate readRate(PlanStreamInput in) throws IOException {
        Source source = Source.readFrom(in);
        Expression field = in.readExpression();
        Expression timestamp = in.readExpression();
        Expression unit = in.readBoolean() ? in.readExpression() : null;
        return new Rate(source, field, timestamp, unit);
    }

    public static void writeRate(PlanStreamOutput out, Rate rate) throws IOException {
        rate.source().writeTo(out);
        out.writeExpression(rate.field());
        out.writeExpression(rate.timestamp);
        if (rate.unit != null) {
            out.writeBoolean(true);
            out.writeExpression(rate.unit);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    protected NodeInfo<Rate> info() {
        return NodeInfo.create(this, Rate::new, field(), timestamp, unit);
    }

    @Override
    public Rate replaceChildren(List<Expression> newChildren) {
        final Expression unit = newChildren.size() >= 3 ? newChildren.get(2) : null;
        return new Rate(source(), newChildren.get(0), newChildren.get(1), unit);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isType(
            field(),
            dt -> dt == DataType.COUNTER_LONG || dt == DataType.COUNTER_INTEGER || dt == DataType.COUNTER_DOUBLE,
            sourceText(),
            FIRST,
            "counter_long",
            "counter_integer",
            "counter_double"
        );
        if (unit != null) {
            resolution = resolution.and(
                isType(unit, dt -> dt.isInteger() || EsqlDataTypes.isTemporalAmount(dt), sourceText(), SECOND, "time_duration")
            );
        }
        return resolution;
    }

    long unitInMillis() {
        if (unit == null) {
            return DEFAULT_UNIT.millis();
        }
        if (unit.foldable() == false) {
            throw new IllegalArgumentException("function [" + sourceText() + "] has invalid unit [" + unit.sourceText() + "]");
        }
        final Object foldValue;
        try {
            foldValue = unit.fold();
        } catch (Exception e) {
            throw new IllegalArgumentException("function [" + sourceText() + "] has invalid unit [" + unit.sourceText() + "]");
        }
        if (foldValue instanceof Duration duration) {
            return duration.toMillis();
        } else {
            throw new IllegalArgumentException("function [" + sourceText() + "] has invalid unit [" + unit.sourceText() + "]");
        }
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        if (inputChannels.size() != 2 && inputChannels.size() != 3) {
            throw new IllegalArgumentException("rate() requires two or three channels; got " + inputChannels);
        }
        final long unitInMillis = unitInMillis();
        final DataType type = field().dataType();
        return switch (type) {
            case COUNTER_LONG -> new RateLongAggregatorFunctionSupplier(inputChannels, unitInMillis);
            case COUNTER_INTEGER -> new RateIntAggregatorFunctionSupplier(inputChannels, unitInMillis);
            case COUNTER_DOUBLE -> new RateDoubleAggregatorFunctionSupplier(inputChannels, unitInMillis);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public String toString() {
        if (unit != null) {
            return "rate(" + field() + "," + unit + ")";
        } else {
            return "rate(" + field() + ")";
        }
    }
}
