/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Delta;
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
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

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
        //
        overTime("avg_over_time", AvgOverTime::new),
        overTime("count_over_time", CountOverTime::new),
        overTime("max_over_time", MaxOverTime::new),
        overTime("min_over_time", MinOverTime::new),
        overTime("sum_over_time", SumOverTime::new),
        overTime("stddev_over_time", StddevOverTime::new),
        overTime("stdvar_over_time", VarianceOverTime::new),
        overTime("absent_over_time", AbsentOverTime::new),
        overTime("present_over_time", PresentOverTime::new),
        //
        overTimeBinary("quantile_over_time", PercentileOverTime::new),
        //
        acrossSeries("avg", Avg::new),
        acrossSeries("count", Count::new),
        acrossSeries("max", Max::new),
        acrossSeries("min", Min::new),
        acrossSeries("sum", Sum::new),
        acrossSeries("stddev", StdDev::new),
        acrossSeries("stdvar", Variance::new),
        //
        acrossSeriesBinary("quantile", Percentile::new)
    };

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

    /**
     * Function definition record for registration and metadata.
     */
    public record FunctionDefinition(
        String name,
        FunctionType functionType,
        Arity arity,
        BiFunction<Source, List<Expression>, Function> esqlBuilder
    ) {
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
    protected interface OverTimeBinary<T extends TimeSeriesAggregateFunction> {
        T build(Source source, Expression field, Expression filter, Expression window, Expression param);
    }

    @FunctionalInterface
    protected interface AcrossSeriesUnary<T extends AggregateFunction> {
        T build(Source source, Expression field);
    }

    @FunctionalInterface
    protected interface AcrossSeriesBinary<T extends AggregateFunction> {
        T build(Source source, Expression field, Expression param);
    }

    private static FunctionDefinition withinSeries(String name, WithinSeries<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.range(1,3),
            (source, params) -> {
                Expression field = params.get(0);
                Expression timestamp = params.get(1);
                Expression window = params.get(2);
                return builder.build(source, field, window, timestamp);
            }
        );
    }

    private static FunctionDefinition overTime(String name, OverTime<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.range(1,2),
            (source, params) -> {
                Expression field = params.get(0);
                Expression window = params.get(2);
                return builder.build(source, field, Literal.TRUE, window);
            }
        );
    }

    private static FunctionDefinition overTimeBinary(String name, OverTimeBinary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.TWO,
            (source, params) -> {
                Expression field = params.get(0);
                Expression percentile = params.get(1);
                Expression window = params.get(2);
                return builder.build(source, field, Literal.TRUE, window, percentile);
            }
        );
    }

    private static FunctionDefinition acrossSeries(String name, AcrossSeriesUnary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.ACROSS_SERIES_AGGREGATION,
            Arity.ONE,
            (source, params) -> {
                Expression field = params.get(0);
                return builder.build(source, field);
            }
        );
    }

    private static FunctionDefinition acrossSeriesBinary(String name, AcrossSeriesBinary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.ACROSS_SERIES_AGGREGATION,
            Arity.TWO,
            (source, params) -> {
                Expression param = params.get(0);
                Expression field = params.get(1);
                return builder.build(source, field, param);
            }
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
        "deriv",
        "holt_winters",
        "mad_over_time",
        "predict_linear",
        "resets",

        // Instant vector functions
        "abs",
        "absent",
        "ceil",
        "clamp",
        "clamp_max",
        "clamp_min",
        "exp",
        "floor",
        "ln",
        "log2",
        "log10",
        "round",
        "scalar",
        "sgn",
        "sort",
        "sort_desc",
        "sqrt",

        // Trigonometric functions
        "acos",
        "acosh",
        "asin",
        "asinh",
        "atan",
        "atanh",
        "cos",
        "cosh",
        "deg",
        "rad",
        "sin",
        "sinh",
        "tan",
        "tanh",

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

        // Special functions
        "histogram_avg",
        "histogram_count",
        "histogram_fraction",
        "histogram_quantile",
        "histogram_stddev",
        "histogram_stdvar",
        "histogram_sum",
        "pi",
        "time",
        "vector"
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

    /**
     * Returns true if function exists and is implemented, false if unknown, null if known but not implemented.
     */
    public void checkFunction(Source source, String name) {
        String normalized = normalize(name);

        if (promqlFunctions.containsKey(normalized) == false) {
            throw new ParsingException(source, "Function [{}] does not exist", name);
        }

        if (NOT_IMPLEMENTED.contains(normalized)) {
            throw new ParsingException(source, "Function [{}] is not yet implemented", name);
        }
    }

    public Function buildEsqlFunction(String name, Source source, List<Expression> params) {
        checkFunction(source, name);
        FunctionDefinition metadata = functionMetadata(name);
        try {
            return metadata.esqlBuilder().apply(source, params);
        } catch (Exception e) {
            throw new ParsingException(source, "Error building ESQL function for [{}]: {}", name, e.getMessage());
        }
    }
}
