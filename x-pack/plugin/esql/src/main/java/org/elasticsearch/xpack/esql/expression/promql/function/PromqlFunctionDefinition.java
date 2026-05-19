/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
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
    private final List<PromqlParamInfo> params;
    private final List<String> examples;
    private final CounterSupport counterSupport;

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
        List<PromqlParamInfo> params,
        List<String> examples,
        CounterSupport counterSupport
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
        this.params = params;
        this.examples = examples;
        this.counterSupport = counterSupport;
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

    public List<PromqlParamInfo> params() {
        return params;
    }

    public List<String> examples() {
        return examples;
    }

    public CounterSupport counterSupport() {
        return counterSupport;
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
        private List<PromqlParamInfo> params;
        private CounterSupport counterSupport = CounterSupport.UNSUPPORTED;

        public PromqlFunctionDefinition.Builder counterSupport(CounterSupport counterSupport) {
            this.counterSupport = counterSupport;
            return this;
        }

        public PromqlFunctionDefinition.Builder description(String description) {
            this.description = description;
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
            FunctionDefinition.BinaryBuilder<? extends Expression> ctorRef
        ) {
            this.functionType = FunctionType.VALUE_TRANSFORMATION;
            this.arity = PromqlFunctionArity.range(1, 2);
            this.builder = (source, target, ctx, extraParams) -> ctorRef.build(
                source,
                target,
                extraParams.isEmpty() ? null : extraParams.getFirst()
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
                            ctx.configuration().withZoneId(ZoneOffset.UTC)
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
            return new PromqlFunctionDefinition(name, functionType, arity, builder, description, params, examples, counterSupport);
        }
    }

}
