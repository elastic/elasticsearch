/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Delta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Deriv;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Idelta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Increase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Irate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MaxOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MinOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StddevOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Variance;
import org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceOverTime;
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Exp;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Signum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tanh;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A registry for PromQL functions that maps function names to their respective definitions.
 */
public class PromqlFunctionRegistry {

    // Common parameter definitions
    private static final ParamInfo RANGE_VECTOR = ParamInfo.child("v", PromqlDataType.RANGE_VECTOR, "Range vector input.");
    private static final ParamInfo INSTANT_VECTOR = ParamInfo.child("v", PromqlDataType.INSTANT_VECTOR, "Instant vector input.");
    private static final ParamInfo SCALAR = ParamInfo.child("s", PromqlDataType.SCALAR, "Scalar value.");
    private static final ParamInfo QUANTILE = ParamInfo.of("φ", PromqlDataType.SCALAR, "Quantile value (0 ≤ φ ≤ 1).");
    private static final ParamInfo TO_NEAREST = ParamInfo.optional(
        "to_nearest",
        PromqlDataType.SCALAR,
        "Round to nearest multiple of this value."
    );
    private static final ParamInfo MIN_SCALAR = ParamInfo.of("min", PromqlDataType.SCALAR, "Minimum value.");
    private static final ParamInfo MAX_SCALAR = ParamInfo.of("max", PromqlDataType.SCALAR, "Maximum value.");

    private static final FunctionDefinition[] FUNCTION_DEFINITIONS = new FunctionDefinition[] {
        //
        withinSeries(
            "delta",
            Delta::new,
            "Calculates the difference between the first and last value of each time series in a range vector.",
            "delta(cpu_temp_celsius[2h])"
        ),
        withinSeries(
            "idelta",
            Idelta::new,
            "Calculates the difference between the last two samples of each time series in a range vector.",
            "idelta(cpu_temp_celsius[5m])"
        ),
        withinSeries(
            "increase",
            Increase::new,
            "Calculates the increase in the time series in the range vector, adjusting for counter resets.",
            "increase(http_requests_total[5m])"
        ),
        withinSeries(
            "irate",
            Irate::new,
            "Calculates the per-second instant rate of increase based on the last two data points.",
            "irate(http_requests_total[5m])"
        ),
        withinSeries(
            "rate",
            Rate::new,
            "Calculates the per-second average rate of increase of the time series in the range vector.",
            "rate(http_requests_total[5m])"
        ),
        withinSeries(
            "first_over_time",
            FirstOverTime::new,
            "Returns the first value of each time series in the specified time range.",
            "first_over_time(http_requests_total[1h])"
        ),
        withinSeries(
            "last_over_time",
            LastOverTime::new,
            "Returns the most recent value of each time series in the specified time range.",
            "last_over_time(http_requests_total[1h])"
        ),
        withinSeries(
            "deriv",
            Deriv::new,
            "Calculates the per-second derivative of the time series using simple linear regression.",
            "deriv(node_memory_free_bytes[5m])"
        ),
        //
        withinSeriesOverTimeUnary(
            "avg_over_time",
            AvgOverTime::new,
            "Returns the average value of all points in the specified time range.",
            "avg_over_time(http_requests_total[5m])"
        ),
        withinSeriesOverTimeUnary(
            "count_over_time",
            CountOverTime::new,
            "Returns the count of all values in the specified time range.",
            "count_over_time(http_requests_total[5m])"
        ),
        withinSeriesOverTimeUnary(
            "max_over_time",
            MaxOverTime::new,
            "Returns the maximum value of all points in the specified time range.",
            "max_over_time(http_requests_total[5m])"
        ),
        withinSeriesOverTimeUnary(
            "min_over_time",
            MinOverTime::new,
            "Returns the minimum value of all points in the specified time range.",
            "min_over_time(http_requests_total[5m])"
        ),
        withinSeriesOverTimeUnary(
            "sum_over_time",
            SumOverTime::new,
            "Returns the sum of all values in the specified time range.",
            "sum_over_time(http_requests_total[5m])"
        ),
        withinSeriesOverTimeUnary(
            "stddev_over_time",
            StddevOverTime::new,
            "Returns the population standard deviation of the values in the specified time range.",
            "stddev_over_time(http_requests_total[5m])"
        ),
        withinSeriesOverTimeUnary(
            "stdvar_over_time",
            VarianceOverTime::new,
            "Returns the population standard variance of the values in the specified time range.",
            "stdvar_over_time(http_requests_total[5m])"
        ),
        withinSeriesOverTimeUnary(
            "absent_over_time",
            AbsentOverTime::new,
            "Returns 1 if the range vector has no elements, otherwise returns an empty vector.",
            "absent_over_time(nonexistent_metric[5m])"
        ),
        withinSeriesOverTimeUnary(
            "present_over_time",
            PresentOverTime::new,
            "Returns 1 if the range vector has any elements, otherwise returns an empty vector.",
            "present_over_time(http_requests_total[5m])"
        ),
        //
        withinSeriesOverTimeBinary(
            "quantile_over_time",
            PercentileOverTime::new,
            "Returns the φ-quantile (0 ≤ φ ≤ 1) of the values in the specified time range.",
            List.of(QUANTILE, RANGE_VECTOR),
            "quantile_over_time(0.5, http_requests_total[1h])"
        ),
        //
        acrossSeriesUnary("avg", Avg::new, "Calculates the average of the values across the input vector.", "avg(http_requests_total)"),
        acrossSeriesUnary("count", Count::new, "Counts the number of elements in the input vector.", "count(http_requests_total)"),
        acrossSeriesUnary("max", Max::new, "Returns the maximum value across the input vector.", "max(http_requests_total)"),
        acrossSeriesUnary("min", Min::new, "Returns the minimum value across the input vector.", "min(http_requests_total)"),
        acrossSeriesUnary("sum", Sum::new, "Calculates the sum of the values across the input vector.", "sum(http_requests_total)"),
        acrossSeriesUnary(
            "stddev",
            StdDev::new,
            "Calculates the population standard deviation across the input vector.",
            "stddev(http_requests_total)"
        ),
        acrossSeriesUnary(
            "stdvar",
            Variance::new,
            "Calculates the population standard variance across the input vector.",
            "stdvar(http_requests_total)"
        ),
        //
        acrossSeriesBinary(
            "quantile",
            Percentile::new,
            "Returns the φ-quantile (0 ≤ φ ≤ 1) of the values across the input vector.",
            List.of(QUANTILE, INSTANT_VECTOR),
            "quantile(0.9, http_request_duration_seconds)"
        ),
        //
        valueTransformationFunction(
            "ceil",
            Ceil::new,
            "Rounds the sample values of all elements up to the nearest integer.",
            "ceil(rate(http_requests_total[5m]))"
        ),
        valueTransformationFunction(
            "abs",
            Abs::new,
            "Returns the input vector with all sample values converted to their absolute value.",
            "abs(rate(http_requests_total[5m]))"
        ),
        valueTransformationFunction(
            "sgn",
            Signum::new,
            "Returns the sign of the sample values: -1 for negative, 0 for zero, and 1 for positive values.",
            "sgn(delta(queue_depth[5m]))"
        ),
        valueTransformationFunction(
            "exp",
            Exp::new,
            "Calculates the exponential function for all elements in the input vector.",
            "exp(rate(http_requests_total[5m]))"
        ),
        valueTransformationFunction(
            "sqrt",
            Sqrt::new,
            "Calculates the square root of all elements in the input vector.",
            "sqrt(http_requests_total)"
        ),
        valueTransformationFunction(
            "log10",
            Log10::new,
            "Calculates the decimal logarithm for all elements in the input vector.",
            "log10(http_requests_total)"
        ),
        valueTransformationFunction(
            "log2",
            (source, value) -> new Log(source, Literal.fromDouble(source, 2d), value),
            "Calculates the binary logarithm for all elements in the input vector.",
            "log2(memory_usage_bytes)"
        ),
        valueTransformationFunction(
            "ln",
            (source, value) -> new Log(source, value, null),
            "Calculates the natural logarithm for all elements in the input vector.",
            "ln(memory_usage_bytes)"
        ),
        valueTransformationFunction(
            "floor",
            Floor::new,
            "Rounds the sample values of all elements down to the nearest integer.",
            "floor(rate(http_requests_total[5m]))"
        ),
        valueTransformationFunctionOptionalArg("round", (source, value, toNearest) -> {
            if (toNearest == null) {
                return new Round(source, value, null);
            } else {
                // round to nearest multiple of toNearest: round(value / toNearest) * toNearest
                return new Mul(source, new Round(source, new Div(source, value, toNearest), null), toNearest);
            }
        },
            "Rounds the sample values to the nearest integer, or to the nearest multiple of the optional argument.",
            List.of(INSTANT_VECTOR, TO_NEAREST),
            "round(rate(http_requests_total[5m]))"
        ),
        //
        valueTransformationFunction("asin", Asin::new, "Calculates the arcsine of all elements in the input vector.", "asin(some_metric)"),
        valueTransformationFunction(
            "acos",
            Acos::new,
            "Calculates the arccosine of all elements in the input vector.",
            "acos(some_metric)"
        ),
        valueTransformationFunction(
            "atan",
            Atan::new,
            "Calculates the arctangent of all elements in the input vector.",
            "atan(some_metric)"
        ),
        valueTransformationFunction("cos", Cos::new, "Calculates the cosine of all elements in the input vector.", "cos(some_metric)"),
        valueTransformationFunction(
            "cosh",
            Cosh::new,
            "Calculates the hyperbolic cosine of all elements in the input vector.",
            "cosh(some_metric)"
        ),
        valueTransformationFunction(
            "acosh",
            Acosh::new,
            "Calculates the inverse hyperbolic cosine of all elements in the input vector.",
            "acosh(some_metric)"
        ),
        valueTransformationFunction(
            "sinh",
            Sinh::new,
            "Calculates the hyperbolic sine of all elements in the input vector.",
            "sinh(some_metric)"
        ),
        valueTransformationFunction("sin", Sin::new, "Calculates the sine of all elements in the input vector.", "sin(some_metric)"),
        valueTransformationFunction("tan", Tan::new, "Calculates the tangent of all elements in the input vector.", "tan(some_metric)"),
        valueTransformationFunction(
            "tanh",
            Tanh::new,
            "Calculates the hyperbolic tangent of all elements in the input vector.",
            "tanh(some_metric)"
        ),
        valueTransformationFunction(
            "deg",
            ToDegrees::new,
            "Converts input values from radians to degrees for all elements in the input vector.",
            "deg(some_metric)"
        ),
        valueTransformationFunction(
            "rad",
            ToRadians::new,
            "Converts input values from degrees to radians for all elements in the input vector.",
            "rad(some_metric)"
        ),
        valueTransformationFunctionBinary(
            "clamp_min",
            ClampMin::new,
            "Clamps the sample values of all elements to have a lower limit of min.",
            List.of(INSTANT_VECTOR, MIN_SCALAR),
            "clamp_min(http_requests_total, 0)"
        ),
        valueTransformationFunctionBinary(
            "clamp_max",
            ClampMax::new,
            "Clamps the sample values of all elements to have an upper limit of max.",
            List.of(INSTANT_VECTOR, MAX_SCALAR),
            "clamp_max(http_requests_total, 100)"
        ),
        valueTransformationFunctionTernary(
            "clamp",
            Clamp::new,
            "Clamps the sample values of all elements to be within [min, max].",
            List.of(INSTANT_VECTOR, MIN_SCALAR, MAX_SCALAR),
            "clamp(http_requests_total, 0, 100)"
        ),
        //
        vector("Returns the scalar as a vector with no labels.", "vector(1)"),
        scalarFunction("pi", (source) -> Literal.fromDouble(source, Math.PI), "Returns the value of pi.", "pi()"),
        scalarFunctionWithStep(
            "time",
            (source, step) -> new Div(source, new ToDouble(source, step), Literal.fromDouble(source, 1000.0)),
            "returns the number of seconds since January 1, 1970 UTC."
                + " Note that this does not actually return the current time, but the time at which the expression is to be evaluated.",
            "time()"
        ) };

    public static final PromqlFunctionRegistry INSTANCE = new PromqlFunctionRegistry();

    private final Map<String, FunctionDefinition> promqlFunctions = new HashMap<>();

    private PromqlFunctionRegistry() {
        for (FunctionDefinition def : FUNCTION_DEFINITIONS) {
            String normalized = normalize(def.name());
            promqlFunctions.put(normalized, def);
        }
    }

    /**
     * Represents the parameter count constraints for a PromQL function.
     */
    public record Arity(int min, int max) {

        // Common arity patterns as constants
        public static final Arity NONE = new Arity(0, 0);
        public static final Arity ONE = new Arity(1, 1);
        public static final Arity TWO = new Arity(2, 2);
        public static final Arity VARIADIC = new Arity(1, Integer.MAX_VALUE);

        public Arity {
            if (min < 0) {
                throw new IllegalArgumentException("min must be non-negative");
            }
            if (max < min) {
                throw new IllegalArgumentException("max must be >= min");
            }
        }

        public static Arity fixed(int count) {
            return switch (count) {
                case 0 -> NONE;
                case 1 -> ONE;
                case 2 -> TWO;
                default -> new Arity(count, count);
            };
        }

        public static Arity range(int min, int max) {
            return min == max ? fixed(min) : new Arity(min, max);
        }

        public static Arity atLeast(int min) {
            return min == 1 ? VARIADIC : new Arity(min, Integer.MAX_VALUE);
        }

        public static Arity optional(int max) {
            return new Arity(0, max);
        }

        public boolean validate(int paramCount) {
            return paramCount >= min && paramCount <= max;
        }
    }

    public record ParamInfo(String name, PromqlDataType type, String description, boolean optional, boolean child) {
        public static ParamInfo child(String name, PromqlDataType type, String description) {
            return new ParamInfo(name, type, description, false, true);
        }

        public static ParamInfo of(String name, PromqlDataType type, String description) {
            return new ParamInfo(name, type, description, false, false);
        }

        public static ParamInfo optional(String name, PromqlDataType type, String description) {
            return new ParamInfo(name, type, description, true, false);
        }
    }

    public record PromqlContext(Expression timestamp, Expression window, Expression step) {}

    @FunctionalInterface
    public interface EsqlFunctionBuilder {
        Expression build(Source source, Expression target, PromqlContext ctx, List<Expression> extraParams);
    }

    /**
     * Function definition record for registration and metadata.
     */
    public record FunctionDefinition(
        String name,
        FunctionType functionType,
        Arity arity,
        EsqlFunctionBuilder esqlBuilder,
        String description,
        List<ParamInfo> params,
        List<String> examples
    ) {
        public FunctionDefinition {
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(functionType, "functionType cannot be null");
            Objects.requireNonNull(arity, "arity cannot be null");
            Objects.requireNonNull(esqlBuilder, "esqlBuilder cannot be null");
            Objects.requireNonNull(description, "description cannot be null");
            Objects.requireNonNull(params, "params cannot be null");
            Objects.requireNonNull(examples, "examples cannot be null");
            if (arity.max() != params.size()) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Arity max %d does not match number of parameters %d for function %s",
                        arity.max(),
                        params.size(),
                        name
                    )
                );
            }
            if (params.isEmpty() == false && params.stream().filter(ParamInfo::child).count() != 1) {
                throw new IllegalArgumentException("If a function takes parameters, there must be exactly one child parameter");
            }
        }
    }

    @FunctionalInterface
    protected interface WithinSeries<T extends TimeSeriesAggregateFunction> {
        T build(Source source, Expression field, Expression window, Expression timestamp);
    }

    @FunctionalInterface
    protected interface OverTime<T extends TimeSeriesAggregateFunction> {
        T build(Source source, Expression field, Expression filter, Expression window);
    }

    @FunctionalInterface
    protected interface AcrossSeriesUnary<T extends AggregateFunction> {
        T build(Source source, Expression field);
    }

    @FunctionalInterface
    protected interface OverTimeBinary<T extends TimeSeriesAggregateFunction> {
        T build(Source source, Expression field, Expression filter, Expression window, Expression param);
    }

    @FunctionalInterface
    protected interface AcrossSeriesBinary<T extends AggregateFunction> {
        T build(Source source, Expression field, Expression filter, Expression window, Expression param);
    }

    @FunctionalInterface
    protected interface ValueTransformationFunction<T extends ScalarFunction> {
        T build(Source source, Expression value);
    }

    @FunctionalInterface
    protected interface ValueTransformationFunctionBinary<T extends Expression> {
        T build(Source source, Expression value, Expression arg1);
    }

    @FunctionalInterface
    protected interface ValueTransformationFunctionTernary<T extends Expression> {
        T build(Source source, Expression value, Expression arg1, Expression arg2);
    }

    @FunctionalInterface
    protected interface ScalarFunctionBuilder {
        Expression build(Source source);
    }

    private static FunctionDefinition withinSeries(String name, WithinSeries<?> builder, String description, String example) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.ONE,
            (source, target, ctx, extraParams) -> builder.build(source, target, ctx.window(), ctx.timestamp()),
            description,
            List.of(RANGE_VECTOR),
            List.of(example)
        );
    }

    private static FunctionDefinition withinSeriesOverTimeUnary(String name, OverTime<?> builder, String description, String example) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.ONE,
            (source, target, ctx, extraParams) -> builder.build(source, target, Literal.TRUE, ctx.window()),
            description,
            List.of(RANGE_VECTOR),
            List.of(example)
        );
    }

    private static FunctionDefinition withinSeriesOverTimeBinary(
        String name,
        OverTimeBinary<?> builder,
        String description,
        List<ParamInfo> params,
        String example
    ) {
        return new FunctionDefinition(name, FunctionType.WITHIN_SERIES_AGGREGATION, Arity.TWO, (source, target, ctx, extraParams) -> {
            Expression param = extraParams.getFirst();
            return builder.build(source, target, Literal.TRUE, ctx.window(), param);
        }, description, params, List.of(example));
    }

    private static FunctionDefinition acrossSeriesUnary(String name, AcrossSeriesUnary<?> builder, String description, String example) {
        return new FunctionDefinition(
            name,
            FunctionType.ACROSS_SERIES_AGGREGATION,
            Arity.ONE,
            (source, target, ctx, extraParams) -> builder.build(source, target),
            description,
            List.of(INSTANT_VECTOR),
            List.of(example)
        );
    }

    private static FunctionDefinition acrossSeriesBinary(
        String name,
        AcrossSeriesBinary<?> builder,
        String description,
        List<ParamInfo> params,
        String example
    ) {
        return new FunctionDefinition(name, FunctionType.ACROSS_SERIES_AGGREGATION, Arity.TWO, (source, target, ctx, extraParams) -> {
            Expression param = extraParams.getFirst();
            return builder.build(source, target, Literal.TRUE, ctx.window(), param);
        }, description, params, List.of(example));
    }

    private static FunctionDefinition valueTransformationFunction(
        String name,
        ValueTransformationFunction<?> builder,
        String description,
        String example
    ) {
        return new FunctionDefinition(
            name,
            FunctionType.VALUE_TRANSFORMATION,
            Arity.ONE,
            (source, target, ctx, extraParams) -> builder.build(source, target),
            description,
            List.of(INSTANT_VECTOR),
            List.of(example)
        );
    }

    private static FunctionDefinition valueTransformationFunctionBinary(
        String name,
        ValueTransformationFunctionBinary<?> builder,
        String description,
        List<ParamInfo> params,
        String example
    ) {
        return new FunctionDefinition(
            name,
            FunctionType.VALUE_TRANSFORMATION,
            Arity.TWO,
            (source, target, ctx, extraParams) -> builder.build(source, target, extraParams.get(0)),
            description,
            params,
            List.of(example)
        );
    }

    private static FunctionDefinition valueTransformationFunctionTernary(
        String name,
        ValueTransformationFunctionTernary<?> builder,
        String description,
        List<ParamInfo> params,
        String example
    ) {
        return new FunctionDefinition(
            name,
            FunctionType.VALUE_TRANSFORMATION,
            Arity.fixed(3),
            (source, target, ctx, extraParams) -> builder.build(source, target, extraParams.get(0), extraParams.get(1)),
            description,
            params,
            List.of(example)
        );
    }

    private static FunctionDefinition valueTransformationFunctionOptionalArg(
        String name,
        ValueTransformationFunctionBinary<?> builder,
        String description,
        List<ParamInfo> params,
        String example
    ) {
        return new FunctionDefinition(
            name,
            FunctionType.VALUE_TRANSFORMATION,
            Arity.range(1, 2),
            (source, target, ctx, extraParams) -> builder.build(source, target, extraParams.isEmpty() ? null : extraParams.getFirst()),
            description,
            params,
            List.of(example)
        );
    }

    private static FunctionDefinition vector(String description, String example) {
        return new FunctionDefinition(
            "vector",
            FunctionType.VECTOR_CONVERSION,
            Arity.ONE,
            (source, target, ctx, extraParams) -> target,
            description,
            List.of(SCALAR),
            List.of(example)
        );
    }

    private static FunctionDefinition scalarFunction(String name, ScalarFunctionBuilder builder, String description, String example) {
        return new FunctionDefinition(
            name,
            FunctionType.SCALAR,
            Arity.NONE,
            (source, target, ctx, extraParams) -> builder.build(source),
            description,
            List.of(),
            List.of(example)
        );
    }

    @FunctionalInterface
    protected interface ScalarFunctionWithStepBuilder {
        Expression build(Source source, Expression step);
    }

    private static FunctionDefinition scalarFunctionWithStep(
        String name,
        ScalarFunctionWithStepBuilder builder,
        String description,
        String example
    ) {
        return new FunctionDefinition(
            name,
            FunctionType.SCALAR,
            Arity.NONE,
            (source, target, ctx, extraParams) -> builder.build(source, ctx.step()),
            description,
            List.of(),
            List.of(example)
        );
    }

    // PromQL function names not yet implemented
    // https://github.com/elastic/metrics-program/issues/39
    private static final Set<String> NOT_IMPLEMENTED = Set.of(
        // Across-series aggregations (not yet available in ESQL)
        "bottomk",
        "topk",
        "group",
        "count_values",

        // Range vector functions (not yet implemented)
        "changes",
        "holt_winters",
        "mad_over_time",
        "predict_linear",
        "resets",

        // Instant vector functions
        "absent",
        "scalar",
        "sort",
        "sort_desc",

        // Trigonometric functions
        "asinh",
        "atanh",

        // Time functions
        "day_of_month",
        "day_of_week",
        "day_of_year",
        "days_in_month",
        "hour",
        "minute",
        "month",
        "timestamp",
        "year",

        // Label manipulation functions
        "label_join",
        "label_replace",

        // Histogram functions
        "histogram_avg",
        "histogram_count",
        "histogram_fraction",
        "histogram_quantile",
        "histogram_stddev",
        "histogram_stdvar",
        "histogram_sum"
    );

    private String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    public Collection<FunctionDefinition> allFunctions() {
        return new ArrayList<>(promqlFunctions.values());
    }

    /**
     * Retrieves the function definition metadata for the given function name.
     */
    public FunctionDefinition functionMetadata(String name) {
        String normalized = normalize(name);
        return promqlFunctions.get(normalized);
    }

    public void checkFunction(Source source, String name) {
        String normalized = normalize(name);

        if (promqlFunctions.containsKey(normalized) == false) {
            throw new ParsingException(source, "Function [{}] does not exist", name);
        }

        if (NOT_IMPLEMENTED.contains(normalized)) {
            throw new ParsingException(source, "Function [{}] is not yet implemented", name);
        }
    }

    public Expression buildEsqlFunction(String name, Source source, Expression target, PromqlContext ctx, List<Expression> extraParams) {
        checkFunction(source, name);
        FunctionDefinition metadata = functionMetadata(name);
        try {
            return metadata.esqlBuilder().build(source, target, ctx, extraParams);
        } catch (Exception e) {
            throw new ParsingException(source, "Error building ESQL function for [{}]: {}", name, e.getMessage());
        }
    }
}
