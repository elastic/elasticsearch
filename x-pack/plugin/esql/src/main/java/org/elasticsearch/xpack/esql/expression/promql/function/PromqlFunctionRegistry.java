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
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Exp;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tanh;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.parser.ParsingException;

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
    private static final FunctionDefinition[] FUNCTION_DEFINITIONS = new FunctionDefinition[] {
        withinSeries("delta", Delta::new),
        withinSeries("idelta", Idelta::new),
        withinSeries("increase", Increase::new),
        withinSeries("irate", Irate::new),
        withinSeries("rate", Rate::new),
        withinSeries("first_over_time", FirstOverTime::new),
        withinSeries("last_over_time", LastOverTime::new),
        withinSeries("deriv", Deriv::new),

        withinSeriesOverTimeUnary("avg_over_time", AvgOverTime::new),
        withinSeriesOverTimeUnary("count_over_time", CountOverTime::new),
        withinSeriesOverTimeUnary("max_over_time", MaxOverTime::new),
        withinSeriesOverTimeUnary("min_over_time", MinOverTime::new),
        withinSeriesOverTimeUnary("sum_over_time", SumOverTime::new),
        withinSeriesOverTimeUnary("stddev_over_time", StddevOverTime::new),
        withinSeriesOverTimeUnary("stdvar_over_time", VarianceOverTime::new),
        withinSeriesOverTimeUnary("absent_over_time", AbsentOverTime::new),
        withinSeriesOverTimeUnary("present_over_time", PresentOverTime::new),
        withinSeriesOverTimeBinary("quantile_over_time", PercentileOverTime::new),

        acrossSeriesUnary("avg", Avg::new),
        acrossSeriesUnary("count", Count::new),
        acrossSeriesUnary("max", Max::new),
        acrossSeriesUnary("min", Min::new),
        acrossSeriesUnary("sum", Sum::new),
        acrossSeriesUnary("stddev", StdDev::new),
        acrossSeriesUnary("stdvar", Variance::new),
        acrossSeriesBinary("quantile", Percentile::new),

        valueTransformationFunction("ceil", Ceil::new),
        valueTransformationFunction("abs", Abs::new),
        valueTransformationFunction("exp", Exp::new),
        valueTransformationFunction("sqrt", Sqrt::new),
        valueTransformationFunction("log10", Log10::new),
        valueTransformationFunction("floor", Floor::new),
        valueTransformationFunctionOptionalArg("round", (source, value, toNearest) -> {
            if (toNearest == null) {
                return new Round(source, value, null);
            } else {
                // round to nearest multiple of toNearest: round(value / toNearest) * toNearest
                return new Mul(source, new Round(source, new Div(source, value, toNearest), null), toNearest);
            }
        }),

        valueTransformationFunction("asin", Asin::new),
        valueTransformationFunction("acos", Acos::new),
        valueTransformationFunction("atan", Atan::new),
        valueTransformationFunction("cos", Cos::new),
        valueTransformationFunction("cosh", Cosh::new),
        valueTransformationFunction("sinh", Sinh::new),
        valueTransformationFunction("sin", Sin::new),
        valueTransformationFunction("tan", Tan::new),
        valueTransformationFunction("tanh", Tanh::new),

        vector(),

        scalarFunction("pi", (source) -> Literal.fromDouble(source, Math.PI)) };

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

    @FunctionalInterface
    public interface EsqlFunctionBuilder {
        Expression build(Source source, Expression target, Expression timestamp, Expression window, List<Expression> extraParams);
    }

    /**
     * Function definition record for registration and metadata.
     */
    public record FunctionDefinition(String name, FunctionType functionType, Arity arity, EsqlFunctionBuilder esqlBuilder) {
        public FunctionDefinition {
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(functionType, "functionType cannot be null");
            Objects.requireNonNull(arity, "arity cannot be null");
            Objects.requireNonNull(esqlBuilder, "esqlBuilder cannot be null");
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
    protected interface ValueTransformationFunctionBinary<T extends ScalarFunction> {
        T build(Source source, Expression value, Expression arg1);
    }

    @FunctionalInterface
    protected interface ScalarFunctionBuilder {
        Expression build(Source source);
    }

    private static FunctionDefinition withinSeries(String name, WithinSeries<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.ONE,
            (source, target, timestamp, window, extraParams) -> {
                return builder.build(source, target, window, timestamp);
            }
        );
    }

    private static FunctionDefinition withinSeriesOverTimeUnary(String name, OverTime<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.ONE,
            (source, target, timestamp, window, extraParams) -> {
                return builder.build(source, target, Literal.TRUE, window);
            }
        );
    }

    private static FunctionDefinition withinSeriesOverTimeBinary(String name, OverTimeBinary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.TWO,
            (source, target, timestamp, window, extraParams) -> {
                Expression param = extraParams.getFirst();
                return builder.build(source, target, Literal.TRUE, window, param);
            }
        );
    }

    private static FunctionDefinition acrossSeriesUnary(String name, AcrossSeriesUnary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.ACROSS_SERIES_AGGREGATION,
            Arity.ONE,
            (source, target, timestamp, window, extraParams) -> {
                return builder.build(source, target);
            }
        );
    }

    private static FunctionDefinition acrossSeriesBinary(String name, AcrossSeriesBinary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.ACROSS_SERIES_AGGREGATION,
            Arity.TWO,
            (source, target, timestamp, window, extraParams) -> {
                Expression param = extraParams.getFirst();
                return builder.build(source, target, Literal.TRUE, window, param);
            }
        );
    }

    private static FunctionDefinition valueTransformationFunction(String name, ValueTransformationFunction<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.VALUE_TRANSFORMATION,
            Arity.ONE,
            (source, target, timestamp, window, extraParams) -> builder.build(source, target)
        );
    }

    private static FunctionDefinition valueTransformationFunctionOptionalArg(String name, ValueTransformationFunctionBinary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.VALUE_TRANSFORMATION,
            Arity.range(1, 2),
            (source, target, timestamp, window, extraParams) -> builder.build(
                source,
                target,
                extraParams.isEmpty() ? null : extraParams.getFirst()
            )
        );
    }

    private static FunctionDefinition vector() {
        return new FunctionDefinition(
            "vector",
            FunctionType.VECTOR_CONVERSION,
            Arity.ONE,
            (source, target, timestamp, window, extraParams) -> target
        );
    }

    private static FunctionDefinition scalarFunction(String name, ScalarFunctionBuilder builder) {
        return new FunctionDefinition(
            name,
            FunctionType.SCALAR,
            Arity.NONE,
            (source, target, timestamp, window, extraParams) -> builder.build(source)
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
        "clamp",
        "clamp_max",
        "clamp_min",
        "ln",
        "log2",
        "scalar",
        "sgn",
        "sort",
        "sort_desc",

        // Trigonometric functions
        "acosh",
        "asinh",
        "atanh",
        "deg",
        "rad",

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
        "histogram_sum",
        // Scalar functions
        "time"
    );

    private String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
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

    public Expression buildEsqlFunction(
        String name,
        Source source,
        Expression target,
        Expression timestamp,
        Expression window,
        List<Expression> extraParams
    ) {
        checkFunction(source, name);
        FunctionDefinition metadata = functionMetadata(name);
        try {
            return metadata.esqlBuilder().build(source, target, timestamp, window, extraParams);
        } catch (Exception e) {
            throw new ParsingException(source, "Error building ESQL function for [{}]: {}", name, e.getMessage());
        }
    }
}
