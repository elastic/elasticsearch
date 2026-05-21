/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ValueTransformationFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.VectorConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Resolves {@link UnresolvedPromqlFunction} nodes inside a {@link PromqlCommand}'s
 * plan tree into their concrete plan-node equivalents
 * ({@link WithinSeriesAggregate}, {@link AcrossSeriesAggregate},
 * {@link ValueTransformationFunction}, etc.).
 */
public class ResolvePromqlFunctions extends ParameterizedAnalyzerRule<PromqlCommand, AnalyzerContext> {

    @Override
    protected LogicalPlan rule(PromqlCommand promql, AnalyzerContext context) {
        LogicalPlan resolved = promql.promqlPlan().transformUp(UnresolvedPromqlFunction.class, ResolvePromqlFunctions::resolveFunction);
        return promql.withPromqlPlan(resolved);
    }

    static LogicalPlan resolveFunction(UnresolvedPromqlFunction unresolved) {
        String name = unresolved.functionName();

        PromqlFunctionDefinition metadata = PromqlFunctionRegistry.INSTANCE.functionMetadata(name);
        if (PromqlFunctionRegistry.INSTANCE.isNotImplemented(name)) {
            throw new VerificationException(List.of(Failure.fail(unresolved, "Function [{}] is not yet implemented", name)));
        }
        if (metadata == null) {
            throw new VerificationException(List.of(Failure.fail(unresolved, "Unknown PromQL function [{}]", name)));
        }

        List<LogicalPlan> rawParams = unresolved.rawParams();
        int paramCount = rawParams.size();
        String arityMessage = "Invalid number of parameters for function [{}], required [{}], found [{}]";
        if (paramCount < metadata.arity().min()) {
            throw new ParsingException(unresolved.source(), arityMessage, name, metadata.arity().min(), paramCount);
        }
        if (paramCount > metadata.arity().max()) {
            throw new ParsingException(unresolved.source(), arityMessage, name, metadata.arity().max(), paramCount);
        }

        LogicalPlan child = null;
        List<Expression> extraParams = new ArrayList<>(Math.max(0, rawParams.size() - 1));
        List<PromqlFunctionDefinition.PromqlParamInfo> functionParams = metadata.params();
        for (int i = 0; i < functionParams.size() && rawParams.size() > i; i++) {
            PromqlFunctionDefinition.PromqlParamInfo expectedParam = functionParams.get(i);
            LogicalPlan providedParam = rawParams.get(i);
            PromqlDataType actualType = PromqlPlan.getType(providedParam);
            PromqlDataType expectedType = expectedParam.type();
            if (actualType != expectedType) {
                if (expectedType == PromqlDataType.RANGE_VECTOR && providedParam instanceof InstantSelector selector) {
                    providedParam = convertToRangeSelector(selector);
                } else {
                    throw new VerificationException(
                        List.of(
                            Failure.fail(unresolved, "expected type {} in call to function [{}], got {}", expectedType, name, actualType)
                        )
                    );
                }
            }
            if (expectedParam.child()) {
                child = providedParam;
            } else if (providedParam instanceof LiteralSelector literalSelector) {
                extraParams.add(literalSelector.literal());
            } else {
                throw new VerificationException(
                    List.of(
                        Failure.fail(
                            unresolved,
                            "expected literal parameter in call to function [{}], got {}",
                            name,
                            providedParam.nodeName()
                        )
                    )
                );
            }
        }

        AcrossSeriesAggregate.Grouping grouping = unresolved.grouping();
        if (grouping != null) {
            if (metadata.functionType() != FunctionType.ACROSS_SERIES_AGGREGATION) {
                throw new VerificationException(
                    List.of(
                        Failure.fail(
                            unresolved,
                            "[{}] clause not allowed on non-aggregation function [{}]",
                            grouping.name().toLowerCase(Locale.ROOT),
                            name
                        )
                    )
                );
            }
            return new AcrossSeriesAggregate(unresolved.source(), child, metadata, extraParams, grouping, unresolved.groupingKeys());
        }

        return switch (metadata.functionType()) {
            case ACROSS_SERIES_AGGREGATION -> new AcrossSeriesAggregate(
                unresolved.source(),
                child,
                metadata,
                extraParams,
                AcrossSeriesAggregate.Grouping.NONE,
                List.of()
            );
            case WITHIN_SERIES_AGGREGATION -> new WithinSeriesAggregate(unresolved.source(), child, metadata, extraParams);
            case VALUE_TRANSFORMATION -> new ValueTransformationFunction(unresolved.source(), child, metadata, extraParams);
            case VECTOR_CONVERSION -> new VectorConversionFunction(unresolved.source(), child, metadata, extraParams);
            case SCALAR_CONVERSION -> new ScalarConversionFunction(unresolved.source(), child, metadata, extraParams);
            case SCALAR, TIME_EXTRACTION -> child == null
                ? new ScalarFunction(unresolved.source(), name)
                : new ValueTransformationFunction(unresolved.source(), child, metadata, extraParams);
            default -> throw new VerificationException(
                List.of(Failure.fail(unresolved, "Unsupported function type [{}] for function [{}]", metadata.functionType(), name))
            );
        };
    }

    /**
     * In contrast to strict PromQL, we allow using instant vector selectors where range vectors are
     * expected, by implicitly treating them as range vectors with a default range.
     */
    private static LogicalPlan convertToRangeSelector(InstantSelector selector) {
        return new RangeSelector(
            selector.source(),
            selector.child(),
            selector.series(),
            selector.labels(),
            selector.labelMatchers(),
            Literal.timeDuration(selector.source(), PromqlLogicalPlanBuilder.IMPLICIT_RANGE_PLACEHOLDER),
            selector.evaluation()
        );
    }
}
