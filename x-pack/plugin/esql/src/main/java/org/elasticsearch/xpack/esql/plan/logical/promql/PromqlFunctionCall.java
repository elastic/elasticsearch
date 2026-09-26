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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;

/**
 * Represents a PromQL function call in the logical plan.
 *
 * This is a surrogate logical plan that encapsulates a PromQL function invocation
 * and delegates to the PromqlFunctionRegistry for validation and ESQL function construction.
 */
public abstract sealed class PromqlFunctionCall extends UnaryPlan implements PromqlPlan permits AcrossSeriesAggregate,
    AcrossSeriesReduction, HistogramFunctionCall, MetadataManipulationFunction, ScalarConversionFunction, WithinSeriesAggregate,
    ValueTransformationFunction, VectorConversionFunction {
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
     * Whether the result drops {@code __name__} from the series identity. Prometheus drops the metric name from the result
     * of every function, except the label functions (they return the input series relabeled), {@code topk}/{@code bottomk}/
     * {@code limitk} (they return input series unchanged) and {@code last_over_time}, which acts like an offset.
     */
    public boolean dropsMetricName() {
        return true;
    }

    /**
     * Builds the ES|QL expression that implements this PromQL function call.
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

    /**
     * A function over its argument's value ({@code rate}, {@code abs}, ...): the child translates under the same
     * requirement and the function is an expression over its value; the labels pass through unchanged. A function that
     * {@link #dropsMetricName() drops the metric name} requires nothing of {@code __name__} below itself, so the child's
     * packings already exclude it, and discards the column above.
     */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        List<String> name = List.of(LabelMatcher.NAME);
        TranslationConstraint required = translation.required();
        // IN: required - `__name__` when the function drops the metric name
        TranslationResult child = translation.translate(child(), dropsMetricName() ? sub(required, of(name)) : required);
        if (child.isEmpty()) {
            return child;
        }
        Expression function = buildEsqlFunction(child.value(), translation.promqlContext(child, window(translation.cmd())));
        // OUT: child's labels - `__name__` when dropped, the function as the value
        TranslationResult result = translation.eval(child, function);
        return dropsMetricName() ? result.drop(name) : result;
    }

    /** The lookback window of a range-vector argument; none for an instant vector. */
    protected Expression window(PromqlCommand cmd) {
        if (child() instanceof RangeSelector rangeSelector) {
            return isImplicitRangePlaceholder(rangeSelector.range()) ? cmd.resolveImplicitRangeWindow() : rangeSelector.range();
        }
        return AggregateFunction.NO_WINDOW;
    }

    private static boolean isImplicitRangePlaceholder(Expression range) {
        return range.foldable()
            && range.fold(FoldContext.small()) instanceof Duration duration
            && duration.equals(PromqlLogicalPlanBuilder.IMPLICIT_RANGE_PLACEHOLDER);
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
}
