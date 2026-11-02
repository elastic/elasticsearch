/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.Count.zeroCountAsNull;

/**
 * Represents a PromQL function call in the logical plan.
 *
 * This is a surrogate logical plan that encapsulates a PromQL function invocation
 * and delegates to the PromqlFunctionRegistry for validation and ESQL function construction.
 */
public abstract sealed class PromqlFunctionCall extends UnaryPlan implements PromqlPlan permits AcrossSeriesAggregate,
    AcrossSeriesReduction, HistogramFunctionCall, MetadataManipulationFunction, ScalarConversionFunction, SortFunction,
    WithinSeriesAggregate, ValueTransformationFunction, VectorConversionFunction {
    // implements TelemetryAware {

    private final List<Expression> parameters;
    private final PromqlFunctionDefinition definition;

    public PromqlFunctionCall(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child);
        this.parameters = parameters != null ? parameters : List.of();
        this.definition = definition;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PromqlFunctionCall does not support serialization");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PromqlFunctionCall does not support serialization");
    }

    public String functionName() {
        return definition.name();
    }

    public List<Expression> parameters() {
        return parameters;
    }

    public PromqlFunctionDefinition definition() {
        return definition;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(parameters);
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_FUNCTION_CALL";
    // }

    @Override
    public int hashCode() {
        return Objects.hash(child(), parameters, definition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PromqlFunctionCall other = (PromqlFunctionCall) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(parameters, other.parameters)
            && Objects.equals(definition, other.definition);
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    /**
     * Builds the ES|QL expression that implements this PromQL function call.
     * <p>
     * The builder returns an {@link Expression}: a value expression for scalar/aggregate/value-transformation
     * functions, or an {@code Order} (possibly {@code null} when unordered) for the order-statistic reductions
     * ({@code topk}, {@code bottomk}, {@code limitk}), consumed by the translator. Functions lowered to plan nodes
     * instead ({@code limit_ratio}) are translated directly and their builders throw.
     *
     * @param target the primary input expression (child vector or scalar), or {@code null} for zero-argument functions
     * @param ctx    the PromQL evaluation context (timestamp, window, step, configuration)
     */
    public Expression buildEsqlFunction(Expression target, PromqlFunctionRegistry.PromqlContext ctx) {
        try {
            // PromQL accepts any numeric range vector. ES|QL distinguishes counter from gauge types
            // internally, so plain numerics are wrapped with to_counter() for counter-required
            // functions and counter metrics are wrapped with to_gauge() for gauge-only functions.
            if (target != null && target.resolved() && target.dataType().isHistogram() == false) {
                var counterSupport = definition.counterSupport();
                if (counterSupport == PromqlFunctionDefinition.CounterSupport.REQUIRED && DataType.isCounter(target.dataType()) == false) {
                    target = new ToCounter(source(), target);
                } else if (counterSupport == PromqlFunctionDefinition.CounterSupport.UNSUPPORTED && DataType.isCounter(target.dataType())) {
                    target = new ToGauge(source(), target);
                }
            }
            return definition.esqlBuilder().build(source(), target, ctx, parameters());
        } catch (Exception e) {
            throw new ParsingException(source(), "Error building ESQL function for [{}]: {}", functionName(), e.getMessage());
        }
    }

    public abstract FunctionType functionType();

    /**
     * {@inheritDoc}
     * <p>
     * Re-declared abstract on the {@link PromqlFunctionCall} hierarchy so every PromQL function node classifies itself
     * explicitly instead of silently inheriting the transparent default: adding a new function node fails to compile until
     * its relabel-placement semantics are decided.
     */
    @Override
    public abstract boolean isIdentityTransparent();

    @Override
    public final PromqlDataType returnType() {
        return functionType().outputType;
    }

    /**
     * Translates a generic PromQL function call (rate, ceil, abs, etc.) into an expression over the child's value.
     * Shared by the function families that need no special lowering; each still declares its own
     * {@link #translate} so a new family cannot inherit this lowering by accident.
     */
    protected final IntermediateResult translateValueFunction(TranslationContext context) {
        return applyValueFunction(context, context.translate(child()));
    }

    /** Applies this function to the value of its already translated child. */
    protected final IntermediateResult applyValueFunction(TranslationContext context, IntermediateResult child) {
        if (child.kind().constant) {
            return child;
        }
        Expression window = AggregateFunction.NO_WINDOW;
        if (child() instanceof RangeSelector rangeSelector) {
            window = context.cmd().resolveRangeWindow(rangeSelector.range());
        }
        var promqlCtx = new PromqlContext(context.time(), window, child.step(), context.configuration());
        return context.eval(child, emptyRangeAsNull(buildEsqlFunction(child.value(), promqlCtx), context.time()));
    }

    /**
     * A range function over a series without a sample in the window has no element in Prometheus. A series can still
     * reach the aggregate without one: its rows carry only other fields, or an operand's matchers reject them inside an
     * aggregate shared with another operand. There {@code count_over_time} reads 0 and {@code present_over_time} false;
     * both map to null so the element drops like any other without a value.
     * <p>
     * {@code absent_over_time} is the mirror: it has an element only for a series without a sample, so only true stays.
     * A series the operand never selects reads true too - its matchers reject every row, or only another operand's metric
     * has values - so true stays only for a series the operand selects.
     */
    private static Expression emptyRangeAsNull(Expression function, Expression time) {
        if (function instanceof CountOverTime) {
            return zeroCountAsNull(function);
        }
        if (function instanceof PresentOverTime present) {
            return new Case(present.source(), present, List.of(Literal.TRUE, Literal.NULL));
        }
        if (function instanceof AbsentOverTime absent) {
            Expression absentFromSelected = new And(absent.source(), selectedBy(absent, time), absent);
            return new Case(absent.source(), absentFromSelected, List.of(Literal.TRUE, Literal.NULL));
        }
        return function;
    }

    /**
     * Whether the operand of {@code absent} reads a row of the series in the window; the operand's matchers apply once its
     * filter attaches to its time-series functions. A metric selector selects a series only where the metric has a value:
     * rows are read for every metric the aggregate references
     * ({@link org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics}), so a row without a value of
     * this one can come from another operand's metric. For a metric field {@code absent} therefore never stays true.
     */
    private static Expression selectedBy(AbsentOverTime absent, Expression time) {
        if (absent.field() instanceof Attribute field && field.resolved() && field.isMetric()) {
            return new Not(absent.source(), absent);
        }
        return new PresentOverTime(absent.source(), time, Literal.TRUE, absent.window());
    }
}
