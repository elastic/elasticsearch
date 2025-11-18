/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
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
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDevOverTime;
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

public class PromqlFunctionRegistry {
    public static final PromqlFunctionRegistry INSTANCE = new PromqlFunctionRegistry();
    private final Map<String, FunctionDefinition> promqlFunctions = new HashMap<>();

    public PromqlFunctionRegistry() {
        register(functionDefinitions());
    }

    /**
     * Define all PromQL functions with their metadata and ESQL constructors.
     * This centralizes function definitions and enables proper validation.
     */
    private static FunctionDefinition[][] functionDefinitions() {
        return new FunctionDefinition[][] {
            // Counter-based range functions (require timestamp for rate calculations)
            new FunctionDefinition[] {
                withinSeries("delta", Delta::new),
                withinSeries("idelta", Idelta::new),
                withinSeries("increase", Increase::new),
                withinSeries("irate", Irate::new),
                withinSeries("rate", Rate::new) },
            // Aggregation range functions
            new FunctionDefinition[] {
                withinSeriesOverTimeWithWindow("avg_over_time", AvgOverTime::new),
                withinSeriesOverTime("count_over_time", CountOverTime::new),
                withinSeriesOverTime("max_over_time", MaxOverTime::new),
                withinSeriesOverTime("min_over_time", MinOverTime::new),
                withinSeriesOverTime("sum_over_time", SumOverTime::new),
                withinSeriesOverTime("stddev_over_time", StdDevOverTime::new),
                withinSeriesOverTime("stdvar_over_time", VarianceOverTime::new) },
            // Selection range functions (require timestamp)
            new FunctionDefinition[] {
                withinSeries("first_over_time", FirstOverTime::new),
                withinSeries("last_over_time", LastOverTime::new) },
            // Presence range functions
            new FunctionDefinition[] {
                withinSeriesOverTime("absent_over_time", AbsentOverTime::new),
                withinSeriesOverTime("present_over_time", PresentOverTime::new) },
            // Range functions with parameters
            new FunctionDefinition[] { withinSeriesOverTime("quantile_over_time", PercentileOverTime::new) },
            // Across-series aggregations (basic - single field parameter)
            new FunctionDefinition[] {
                acrossSeries("avg", Avg::new),
                acrossSeries("count", Count::new),
                acrossSeries("max", Max::new),
                acrossSeries("min", Min::new),
                acrossSeries("sum", Sum::new),
                acrossSeries("stddev", StdDev::new),
                acrossSeries("stdvar", Variance::new) },
            // Across-series aggregations with parameters
            new FunctionDefinition[] { acrossSeriesBinary("quantile", Percentile::new) } };
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
        T build(Source source, Expression field, Expression timestamp);
    }

    @FunctionalInterface
    protected interface WithinSeriesWindow<T extends TimeSeriesAggregateFunction> {
        T build(Source source, Expression field, Expression window, Expression timestamp);
    }

    @FunctionalInterface
    protected interface OverTimeWithinSeries<T extends TimeSeriesAggregateFunction> {
        T build(Source source, Expression valueField);
    }

    @FunctionalInterface
    protected interface OverTimeWithinSeriesBinary<T extends TimeSeriesAggregateFunction> {
        T build(Source source, Expression valueField, Expression param);
    }

    @FunctionalInterface
    protected interface AcrossSeriesUnary<T extends AggregateFunction> {
        T build(Source source, Expression field);
    }

    @FunctionalInterface
    protected interface AcrossSeriesBinary<T extends AggregateFunction> {
        T build(Source source, Expression field, Expression param);
    }

    private static FunctionDefinition withinSeriesOverTime(String name, OverTimeWithinSeries<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.ONE,
            (source, params) -> builder.build(source, params.get(0))
        );
    }

    private static FunctionDefinition withinSeriesOverTime(String name, OverTimeWithinSeriesBinary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.TWO,
            (source, params) -> builder.build(source, params.get(0), params.get(1))
        );
    }

    // NB: There's no longer a single argument constructor so accept the dual one while passing on a NO_WINDOW
    private static FunctionDefinition withinSeriesOverTimeWithWindow(String name, OverTimeWithinSeriesBinary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.WITHIN_SERIES_AGGREGATION,
            Arity.ONE,
            (source, params) -> builder.build(source, params.get(0), AggregateFunction.NO_WINDOW)
        );
    }

    private static FunctionDefinition withinSeries(String name, WithinSeries<?> builder) {
        return new FunctionDefinition(name, FunctionType.WITHIN_SERIES_AGGREGATION, Arity.ONE, (source, params) -> {
            Expression valueField = params.get(0);
            Expression timestampField = params.get(1);
            return builder.build(source, valueField, timestampField);
        });
    }

    private static FunctionDefinition withinSeries(String name, WithinSeriesWindow<?> builder) {
        return new FunctionDefinition(name, FunctionType.WITHIN_SERIES_AGGREGATION, Arity.ONE, (source, params) -> {
            Expression valueField = params.get(0);
            Expression timestampField = params.get(1);
            return builder.build(source, valueField, AggregateFunction.NO_WINDOW, timestampField);
        });
    }

    private static FunctionDefinition acrossSeries(String name, AcrossSeriesUnary<?> builder) {
        return new FunctionDefinition(
            name,
            FunctionType.ACROSS_SERIES_AGGREGATION,
            Arity.ONE,
            (source, params) -> builder.build(source, params.get(0))
        );
    }

    private static FunctionDefinition acrossSeriesBinary(String name, AcrossSeriesBinary<?> builder) {
        return new FunctionDefinition(name, FunctionType.ACROSS_SERIES_AGGREGATION, Arity.TWO, (source, params) -> {
            Expression param = params.get(0);  // First param (k, quantile, etc.)
            Expression field = params.get(1);   // Second param (the vector field)
            return builder.build(source, field, param);
        });
    }

    private void register(FunctionDefinition[][] definitionGroups) {
        for (FunctionDefinition[] group : definitionGroups) {
            for (FunctionDefinition def : group) {
                String normalized = normalize(def.name());
                promqlFunctions.put(normalized, def);
            }
        }
    }

    // PromQL function names not yet implemented
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

    public Boolean functionExists(String name) {
        String normalized = normalize(name);
        if (promqlFunctions.containsKey(normalized)) {
            return true;
        }
        if (NOT_IMPLEMENTED.contains(normalized)) {
            return null;
        }
        return false;
    }

    public FunctionDefinition functionMetadata(String name) {
        String normalized = normalize(name);
        FunctionDefinition metadata = promqlFunctions.get(normalized);
        return metadata;
    }

    public Function buildEsqlFunction(String name, Source source, List<Expression> params) {
        FunctionDefinition metadata = functionMetadata(name);

        try {
            return metadata.esqlBuilder().apply(source, params);
        } catch (Exception e) {
            throw new ParsingException(source, "Error building ESQL function for [{}]: {}", name, e.getMessage());
        }
    }
}
