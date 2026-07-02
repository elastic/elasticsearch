/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.Version;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.type.DataType.isDateNanos;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNull;

/**
 * Function definition record for registration and metadata.
 */
public final class PromqlFunctionDefinition {

    private final String name;
    private final FunctionType functionType;
    private final PromqlFunctionArity arity;
    private final FunctionBuilder esqlBuilder;
    private final String description;
    private final String extendedDescription;
    private final List<PromqlParamInfo> params;
    private final List<String> examples;
    private final CounterSupport counterSupport;
    private final String differenceFromPrometheus;
    private final List<StackAvailability> stack;

    @FunctionalInterface
    public interface FunctionBuilder {
        Expression build(Source source, Expression target, PromqlFunctionRegistry.PromqlContext ctx, List<Expression> extraParams);
    }

    /**
     * Describes whether a PromQL function supports counter metric types.
     * <p>
     * This is an ES|QL implementation detail — in real PromQL, all functions work with any numeric type.
     * ES|QL distinguishes counter types from plain numerics internally, and some ESQL functions only
     * accept one or the other.
     */
    public enum CounterSupport {
        /** Only accepts counter types (e.g., rate, increase, irate). */
        REQUIRED,
        /** Accepts both counter and non-counter types. */
        SUPPORTED,
        /** Only accepts non-counter numeric types. */
        UNSUPPORTED
    }

    /**
     * Builds an ES|QL expression for a PromQL date/time extraction function (e.g. year, month, day_of_month).
     */
    @FunctionalInterface
    public interface DateTimeFunctionBuilder {
        Expression build(Source source, Expression date, Configuration configuration);
    }

    @FunctionalInterface
    public interface PromqlBinaryOptionalValueTransformation {
        Expression build(Source source, Expression value, Expression toNearest, Configuration configuration);
    }

    public record PromqlParamInfo(String name, PromqlDataType type, String description, boolean optional, boolean child) {
        public static PromqlParamInfo child(String name, PromqlDataType type, String description) {
            return new PromqlParamInfo(name, type, description, false, true);
        }

        public static PromqlParamInfo of(String name, PromqlDataType type, String description) {
            return new PromqlParamInfo(name, type, description, false, false);
        }

        public static PromqlParamInfo optional(String name, PromqlDataType type, String description) {
            return new PromqlParamInfo(name, type, description, true, false);
        }

        /**
         * Creates a parameter that is both optional and acts as the primary child expression for the function.
         */
        public static PromqlParamInfo optionalChild(String name, PromqlDataType type, String description) {
            return new PromqlParamInfo(name, type, description, true, true);
        }
    }

    /**
     * Represents the parameter count constraints for a PromQL function.
     */
    public record PromqlFunctionArity(int min, int max) {

        // Common arity patterns as constants
        public static final PromqlFunctionDefinition.PromqlFunctionArity NONE = new PromqlFunctionDefinition.PromqlFunctionArity(0, 0);
        public static final PromqlFunctionDefinition.PromqlFunctionArity ONE = new PromqlFunctionDefinition.PromqlFunctionArity(1, 1);
        public static final PromqlFunctionDefinition.PromqlFunctionArity TWO = new PromqlFunctionDefinition.PromqlFunctionArity(2, 2);

        public PromqlFunctionArity {
            if (min < 0) {
                throw new IllegalArgumentException("min must be non-negative");
            }
            if (max < min) {
                throw new IllegalArgumentException("max must be >= min");
            }
        }

        private static PromqlFunctionDefinition.PromqlFunctionArity fixed(int count) {
            return switch (count) {
                case 0 -> NONE;
                case 1 -> ONE;
                case 2 -> TWO;
                default -> new PromqlFunctionDefinition.PromqlFunctionArity(count, count);
            };
        }

        private static PromqlFunctionDefinition.PromqlFunctionArity range(int min, int max) {
            return min == max ? fixed(min) : new PromqlFunctionDefinition.PromqlFunctionArity(min, max);
        }

        private static PromqlFunctionDefinition.PromqlFunctionArity optional(int max) {
            return new PromqlFunctionDefinition.PromqlFunctionArity(0, max);
        }
    }

    private PromqlFunctionDefinition(
        String name,
        FunctionType functionType,
        PromqlFunctionArity arity,
        FunctionBuilder esqlBuilder,
        String description,
        String extendedDescription,
        List<PromqlParamInfo> params,
        List<String> examples,
        CounterSupport counterSupport,
        String differenceFromPrometheus,
        List<StackAvailability> stack
    ) {
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(functionType, "functionType cannot be null");
        Objects.requireNonNull(arity, "arity cannot be null");
        Objects.requireNonNull(esqlBuilder, "esqlBuilder cannot be null");
        Objects.requireNonNull(description, "description cannot be null");
        Objects.requireNonNull(params, "params cannot be null");
        Objects.requireNonNull(examples, "examples cannot be null");
        Objects.requireNonNull(counterSupport, "counterSupport cannot be null");
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
        if (params.isEmpty() == false && params.stream().filter(PromqlParamInfo::child).count() != 1) {
            throw new IllegalArgumentException("If a function takes parameters, there must be exactly one child parameter");
        }
        this.name = name;
        this.functionType = functionType;
        this.arity = arity;
        this.esqlBuilder = esqlBuilder;
        this.description = description;
        // Optional: extra description paragraph rendered only on the function's own page, not in the brief overview.
        this.extendedDescription = extendedDescription;
        this.params = params;
        this.examples = examples;
        this.counterSupport = counterSupport;
        // Optional: only set for functions whose Elasticsearch behavior diverges from the Prometheus reference.
        this.differenceFromPrometheus = differenceFromPrometheus;
        // Stack availability rendered into the docs applies_to badge. Empty until declared; the docs generator rejects
        // any registered function that leaves this unset.
        this.stack = stack;
    }

    public String name() {
        return name;
    }

    public FunctionType functionType() {
        return functionType;
    }

    public PromqlFunctionArity arity() {
        return arity;
    }

    public FunctionBuilder esqlBuilder() {
        return esqlBuilder;
    }

    public String description() {
        return description;
    }

    /**
     * Additional description detail rendered only on the function's own reference page (after the brief summary), not
     * in the per-category functions overview. Used to keep the overview summaries short while documenting fuller
     * behavior on the dedicated page. {@code null} when the function has no extra detail.
     */
    public String extendedDescription() {
        return extendedDescription;
    }

    public List<PromqlParamInfo> params() {
        return params;
    }

    public List<String> examples() {
        return examples;
    }

    public CounterSupport counterSupport() {
        return counterSupport;
    }

    /**
     * The "Differences from Prometheus" note rendered in the generated function docs, or {@code null} when the
     * function matches the Prometheus reference behavior.
     */
    public String differenceFromPrometheus() {
        return differenceFromPrometheus;
    }

    /**
     * The {@code stack} availability entries rendered into this function's docs {@code applies_to} badge (for example
     * {@code preview 9.4, ga 9.5}). Empty when the function has not declared its availability, which the docs generator
     * treats as an error so every registered function must declare one.
     */
    public List<StackAvailability> stack() {
        return stack;
    }

    @Override
    public String toString() {
        return name;
    }

    public static final PromqlParamInfo RANGE_VECTOR = PromqlParamInfo.child("v", PromqlDataType.RANGE_VECTOR, "Range vector input.");
    public static final PromqlParamInfo INSTANT_VECTOR = PromqlParamInfo.child("v", PromqlDataType.INSTANT_VECTOR, "Instant vector input.");
    public static final PromqlParamInfo INSTANT_VECTOR_OPTIONAL = PromqlParamInfo.optionalChild(
        "v",
        PromqlDataType.INSTANT_VECTOR,
        "Optional instant vector input. If omitted, evaluation timestamp is used."
    );
    public static final PromqlParamInfo SCALAR = PromqlParamInfo.child("s", PromqlDataType.SCALAR, "Scalar value.");
    public static final PromqlParamInfo QUANTILE = PromqlParamInfo.of("φ", PromqlDataType.SCALAR, "Quantile value (0 ≤ φ ≤ 1).");
    public static final PromqlParamInfo TO_NEAREST = PromqlParamInfo.optional(
        "to_nearest",
        PromqlDataType.SCALAR,
        "Round to nearest multiple of this value."
    );
    public static final PromqlParamInfo MIN_SCALAR = PromqlParamInfo.of("min", PromqlDataType.SCALAR, "Minimum value.");
    public static final PromqlParamInfo MAX_SCALAR = PromqlParamInfo.of("max", PromqlDataType.SCALAR, "Maximum value.");

    /**
     * Shared extended-description fragment for the counter rate family ({@code rate}, {@code irate}, {@code increase}),
     * set as each function's {@link Builder#extendedDescription} so it appears only on the dedicated function page, not
     * in the brief functions overview. Documents Elasticsearch behavior that is consistent with Prometheus.
     */
    public static final String COUNTER_RATE_BEHAVIOR =
        "Requires a counter input; non-counter inputs are automatically coerced with `to_counter`. The metric's "
            + "configured temporality (cumulative or delta) is honored. The result is always a `double`.";

    /**
     * Difference note for {@code rate} and {@code increase}: unlike Prometheus, which extrapolates within each
     * (overlapping) range window, Elasticsearch evaluates these over fixed time buckets and interpolates the counter
     * value at interior bucket boundaries from the adjacent buckets' samples. See
     * {@code RateDoubleGroupingAggregatorFunction}.
     */
    public static final String RATE_INCREASE_NOTE =
        "{{es}} computes the value over fixed time buckets and, at bucket boundaries, interpolates the counter value "
            + "from the adjacent buckets' samples, falling back to Prometheus-style extrapolation wherever an adjacent "
            + "bucket has no samples (the series edges and any gaps). Prometheus instead extrapolates within each range "
            + "window, so results can differ slightly.";
    /**
     * Shared extended-description fragment for the gauge family ({@code delta}, {@code idelta}, {@code deriv}), set as
     * each function's {@link Builder#extendedDescription} so it appears only on the dedicated function page, not in the
     * brief functions overview. Documents the transparent {@code to_gauge} coercion, which matches Prometheus semantics
     * and so is not a "Differences from Prometheus" note. (Native histogram support is covered by the page-level
     * limitations note rather than repeated here.)
     */
    public static final String GAUGE_FAMILY_BEHAVIOR =
        "Operates on gauges: counter inputs are automatically and transparently converted to a gauge with `to_gauge`. "
            + "The result is always a `double`.";
    public static final String FIRST_LAST_NOTE =
        "Accepts additional {{es}} field types (for example `keyword`, `ip`, and `date`) and returns counter inputs "
            + "unchanged rather than rejecting or converting them.";
    public static final String COUNT_NOTE = "Returns a `long` integer count rather than a floating-point value.";
    public static final String LOG_DOMAIN_NOTE =
        "For an input of zero or a negative number, {{es}} returns `null` and emits a warning, rather than the "
            + "`-Inf` (for zero) or `NaN` (for negatives) that Prometheus returns.";
    public static final String DOMAIN_PLUS_MINUS_ONE_NOTE =
        "For inputs outside the range [-1, 1], {{es}} returns `null` and emits a warning, rather than the `NaN` that "
            + "Prometheus returns.";
    public static final String OVERFLOW_NOTE =
        "On numeric overflow for large-magnitude inputs, {{es}} returns `null` and emits a warning, rather than the "
            + "`±Inf` that Prometheus returns.";
    public static final String QUANTILE_NOTE =
        "Computed using the {{es}} t-digest percentile aggregation, so results are approximate and may differ slightly "
            + "from Prometheus's exact linear interpolation, particularly for small sample sets.";

    /**
     * Stack (versioned Elasticsearch) releases that PromQL function documentation can reference. Kept as a small closed
     * set so individual function definitions cannot introduce free-text version strings; add a constant here when a new
     * release starts shipping PromQL functions. The rendered label is derived from {@link Version} so it cannot drift.
     */
    public enum PromqlDocsVersion {
        V_9_4(Version.V_9_4_0),
        V_9_5(Version.V_9_5_0);

        private final Version version;

        PromqlDocsVersion(Version version) {
            this.version = version;
        }

        /** Major.minor label used inside an {@code applies_to} badge, for example {@code "9.4"}. */
        String docsLabel() {
            return version.major + "." + version.minor;
        }
    }

    /**
     * A single {@code stack} availability entry for a function's docs badge: a lifecycle state and the release it
     * applies from, for example {@code preview 9.4} or {@code ga 9.5}.
     */
    public record StackAvailability(FunctionAppliesToLifecycle lifecycle, PromqlDocsVersion since) {
        /**
         * Renders this entry as it appears inside a {@code stack:} badge, for example {@code "preview 9.4"}.
         * */
        String appliesTo() {
            return lifecycle.name().toLowerCase(Locale.ROOT) + " " + since.docsLabel();
        }
    }

    /**
     * Convenience factory for a {@code preview} stack availability entry.
     */
    public static StackAvailability preview(PromqlDocsVersion version) {
        return new StackAvailability(FunctionAppliesToLifecycle.PREVIEW, version);
    }

    /**
     * Convenience factory for a {@code ga} stack availability entry.
     */
    public static StackAvailability ga(PromqlDocsVersion version) {
        return new StackAvailability(FunctionAppliesToLifecycle.GA, version);
    }

    /**
     * Stack availability for PromQL functions that shipped as a preview in 9.4 and became generally available in 9.5.
     */
    public static final List<StackAvailability> STACK_PREVIEW_9_4_GA_9_5 = List.of(
        preview(PromqlDocsVersion.V_9_4),
        ga(PromqlDocsVersion.V_9_5)
    );

    /**
     * Stack availability for PromQL functions first implemented (and generally available) in 9.5.
     */
    public static final List<StackAvailability> STACK_GA_9_5 = List.of(ga(PromqlDocsVersion.V_9_5));

    /**
     * Scales a PromQL quantile φ (in the range [0, 1]) to the percentile value (in the range [0, 100]) expected by
     * the ES|QL {@code PERCENTILE} aggregation that PromQL {@code quantile} and {@code quantile_over_time} translate
     * into. Without this scaling, e.g. {@code quantile(1.0, x)} would collapse to the 0.01th percentile (≈ the
     * minimum) instead of returning the maximum.
     */
    public static Expression quantileToPercentile(Source source, Expression phi) {
        return new Mul(source, phi, Literal.fromDouble(source, 100.0));
    }

    /**
     * Create a builder for a {@link PromqlFunctionDefinition}.
     */
    public static Builder def() {
        return new Builder();
    }

    /**
     * A builder for {@link PromqlFunctionDefinition}s. Get one from {@link #def}.
     */
    public static class Builder {
        private final List<String> examples = new ArrayList<>();
        private FunctionType functionType;
        private PromqlFunctionArity arity;
        private FunctionBuilder builder;
        private String description;
        private String extendedDescription;
        private List<PromqlParamInfo> params;
        private CounterSupport counterSupport = CounterSupport.UNSUPPORTED;
        private String differenceFromPrometheus;
        private List<StackAvailability> stack = List.of();

        public PromqlFunctionDefinition.Builder counterSupport(CounterSupport counterSupport) {
            this.counterSupport = counterSupport;
            return this;
        }

        public PromqlFunctionDefinition.Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Adds a description paragraph rendered only on the function's own reference page, after the brief summary. Use
         * this for behavior detail that should not bloat the short summaries shown in the functions overview. Omit for
         * functions whose full description fits in {@link #description}.
         */
        public PromqlFunctionDefinition.Builder extendedDescription(String extendedDescription) {
            this.extendedDescription = extendedDescription;
            return this;
        }

        /**
         * Documents how this function's Elasticsearch behavior diverges from the Prometheus reference. Rendered as a
         * "Differences from Prometheus" section in the generated docs. Omit for functions that match Prometheus.
         */
        public PromqlFunctionDefinition.Builder differenceFromPrometheus(String differenceFromPrometheus) {
            this.differenceFromPrometheus = differenceFromPrometheus;
            return this;
        }

        /**
         * Declares the {@code stack} availability rendered into the function's docs {@code applies_to} badge. Use the
         * {@link PromqlFunctionDefinition#STACK_PREVIEW_9_4_GA_9_5} / {@link PromqlFunctionDefinition#STACK_GA_9_5}
         * presets for the common cases. Serverless availability is intentionally not declared here: implemented PromQL
         * functions are generally available in serverless, which is stated once at the docs page level rather than
         * repeated per function. (When a function first ships as a serverless preview, add a serverless entry here.)
         */
        public PromqlFunctionDefinition.Builder stack(List<StackAvailability> stack) {
            this.stack = stack;
            return this;
        }

        public PromqlFunctionDefinition.Builder example(String example) {
            this.examples.add(example);
            return this;
        }

        public PromqlFunctionDefinition.Builder unaryValueTransformation(BiFunction<Source, Expression, ? extends Expression> ctorRef) {
            this.functionType = FunctionType.VALUE_TRANSFORMATION;
            this.arity = PromqlFunctionArity.ONE;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.apply(source, target);
            this.params = List.of(INSTANT_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder binaryValueTransformation(
            PromqlParamInfo p,
            FunctionDefinition.BinaryBuilder<? extends Expression> ctorRef
        ) {
            this.functionType = FunctionType.VALUE_TRANSFORMATION;
            this.arity = PromqlFunctionArity.TWO;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(source, target, extraParams.getFirst());
            this.params = List.of(INSTANT_VECTOR, p);
            return this;
        }

        public PromqlFunctionDefinition.Builder binaryOptionalValueTransformation(
            PromqlParamInfo p,
            PromqlBinaryOptionalValueTransformation ctorRef
        ) {
            this.functionType = FunctionType.VALUE_TRANSFORMATION;
            this.arity = PromqlFunctionArity.range(1, 2);
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(
                source,
                target,
                extraParams.isEmpty() ? null : extraParams.getFirst(),
                ctx.configuration()
            );
            this.params = List.of(INSTANT_VECTOR, p);
            return this;
        }

        public PromqlFunctionDefinition.Builder ternaryValueTransformation(
            PromqlParamInfo p1,
            PromqlParamInfo p2,
            FunctionDefinition.TernaryBuilder<? extends Expression> ctorRef
        ) {
            this.functionType = FunctionType.VALUE_TRANSFORMATION;
            this.arity = PromqlFunctionArity.fixed(3);
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(source, target, extraParams.getFirst(), extraParams.get(1));
            this.params = List.of(INSTANT_VECTOR, p1, p2);
            return this;
        }

        public PromqlFunctionDefinition.Builder withinSeries(FunctionDefinition.TernaryBuilder<? extends Expression> ctorRef) {
            this.functionType = FunctionType.WITHIN_SERIES_AGGREGATION;
            this.arity = PromqlFunctionArity.ONE;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(source, target, ctx.window(), ctx.timestamp());
            this.params = List.of(RANGE_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder withinSeries(
            PromqlParamInfo paramInfo,
            FunctionDefinition.QuaternaryBuilder<? extends Expression> ctorRef
        ) {
            this.functionType = FunctionType.WITHIN_SERIES_AGGREGATION;
            this.arity = PromqlFunctionArity.TWO;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(
                source,
                target,
                extraParams.getFirst(),
                ctx.window(),
                ctx.timestamp()
            );
            this.params = List.of(paramInfo, RANGE_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder withinSeriesOverTime(FunctionDefinition.TernaryBuilder<? extends Expression> ctorRef) {
            this.functionType = FunctionType.WITHIN_SERIES_AGGREGATION;
            this.arity = PromqlFunctionArity.ONE;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(source, target, Literal.TRUE, ctx.window());
            this.params = List.of(RANGE_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder withinSeriesOverTimeBinary(
            PromqlParamInfo paramInfo,
            FunctionDefinition.QuaternaryBuilder<? extends Expression> ctorRef
        ) {
            this.functionType = FunctionType.WITHIN_SERIES_AGGREGATION;
            this.arity = PromqlFunctionArity.TWO;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(
                source,
                target,
                Literal.TRUE,
                ctx.window(),
                extraParams.getFirst()
            );
            this.params = List.of(paramInfo, RANGE_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder acrossSeries(BiFunction<Source, Expression, ? extends Expression> ctorRef) {
            this.functionType = FunctionType.ACROSS_SERIES_AGGREGATION;
            this.arity = PromqlFunctionArity.ONE;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.apply(source, target);
            this.params = List.of(INSTANT_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder acrossSeriesBinary(
            PromqlParamInfo paramInfo,
            FunctionDefinition.QuaternaryBuilder<? extends Expression> ctorRef
        ) {
            this.functionType = FunctionType.ACROSS_SERIES_AGGREGATION;
            this.arity = PromqlFunctionArity.TWO;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(
                source,
                target,
                Literal.TRUE,
                ctx.window(),
                extraParams.getFirst()
            );
            this.params = List.of(paramInfo, INSTANT_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder histogramUnary(BiFunction<Source, Expression, ? extends Expression> ctorRef) {
            this.functionType = FunctionType.HISTOGRAM;
            this.arity = PromqlFunctionArity.ONE;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.apply(source, target);
            this.params = List.of(INSTANT_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder histogramBinary(PromqlParamInfo paramInfo, FunctionBuilder builder) {
            this.functionType = FunctionType.HISTOGRAM;
            this.arity = PromqlFunctionArity.TWO;
            this.builder = builder;
            this.params = List.of(paramInfo, INSTANT_VECTOR);
            return this;
        }

        public PromqlFunctionDefinition.Builder scalar(Function<Source, ? extends Expression> ctorRef) {
            this.functionType = FunctionType.SCALAR;
            this.arity = PromqlFunctionArity.NONE;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.apply(source);
            this.params = List.of();
            return this;
        }

        public PromqlFunctionDefinition.Builder scalarWithStep(BiFunction<Source, Expression, ? extends Expression> ctorRef) {
            this.functionType = FunctionType.SCALAR;
            this.arity = PromqlFunctionArity.NONE;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.apply(source, ctx.step());
            this.params = List.of();
            return this;
        }

        /**
         * Builds a date/time extraction function that accepts an optional instant vector argument.
         * When no argument is provided, the evaluation step timestamp (or fallback timestamp) is used.
         * When the argument is a numeric value (seconds since epoch), it is converted to a datetime first.
         */
        public PromqlFunctionDefinition.Builder dateTime(DateTimeFunctionBuilder ctorRef) {
            this.functionType = FunctionType.TIME_EXTRACTION;
            this.arity = PromqlFunctionArity.optional(1);
            this.builder = (source, target, ctx, extraParams) -> {
                var step = ctx.step();
                var defaultTimestamp = (step == null || isNull(step.dataType())) ? ctx.timestamp() : step;
                var date = target == null ? defaultTimestamp : target;

                if (isDateTime(date.dataType()) || isDateNanos(date.dataType())) {
                    return ctorRef.build(source, date, ctx.configuration());
                } else {
                    return ctorRef.build(
                        source,
                        new ToDatetime(
                            source,
                            new Mul(source, new ToDouble(source, date), Literal.fromDouble(source, 1000.0)),
                            ctx.configuration().withSetting(QuerySettings.TIME_ZONE, ZoneOffset.UTC)
                        ),
                        ctx.configuration()
                    );
                }
            };
            this.params = List.of(INSTANT_VECTOR_OPTIONAL);
            return this;
        }

        /**
         * Builds a function that converts a scalar into a vector. There should only ever
         * be one of these functions. It's built in {@link PromqlBuiltinFunctionDefinitions}.
         * So this is package private.
         */
        PromqlFunctionDefinition.Builder vectorConversion() {
            this.functionType = FunctionType.VECTOR_CONVERSION;
            this.arity = PromqlFunctionArity.ONE;
            this.builder = (source, target, ctx, extraParams) -> target;
            this.params = List.of(SCALAR);
            return this;
        }

        public PromqlFunctionDefinition.Builder scalarConversion(BiFunction<Source, Expression, ? extends Expression> ctorRef) {
            this.functionType = FunctionType.SCALAR_CONVERSION;
            this.arity = PromqlFunctionArity.ONE;
            this.builder = (source, target, ctx, extraParams) -> ctorRef.apply(source, target);
            this.params = List.of(INSTANT_VECTOR);
            return this;
        }

        /**
         * Build the {@link PromqlFunctionDefinition} with the given primary name.
         */
        public PromqlFunctionDefinition name(String name) {
            return new PromqlFunctionDefinition(
                name,
                functionType,
                arity,
                builder,
                description,
                extendedDescription,
                params,
                examples,
                counterSupport,
                differenceFromPrometheus,
                stack
            );
        }
    }

}
