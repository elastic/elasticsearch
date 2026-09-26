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
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

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
     * requirement and the function is an expression over its value; the labels pass through unchanged.
     */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        // IN: required, unchanged
        TranslationResult child = translation.translate(child(), translation.required());
        if (child.kind().constant) {
            return child;
        }
        Expression function = buildEsqlFunction(child.value(), translation.promqlContext(child, window(translation.cmd())));
        // OUT: child's labels, the function as the value
        return translation.eval(child, function);
    }

    /** The lookback window of a range-vector argument; none for an instant vector. */
    private Expression window(PromqlCommand cmd) {
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
