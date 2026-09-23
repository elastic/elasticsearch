/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesReduction;
import org.elasticsearch.xpack.esql.plan.logical.promql.MetadataManipulationFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
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
 * {@link AcrossSeriesReduction}, {@link ValueTransformationFunction}, etc.).
 */
public class ResolvePromqlFunctions extends ParameterizedAnalyzerRule<PromqlCommand, AnalyzerContext> {

    @Override
    protected LogicalPlan rule(PromqlCommand promql, AnalyzerContext context) {
        PromqlFunctionRegistry registry = context.promqlFunctionRegistry();
        LogicalPlan resolved = promql.promqlPlan()
            .transformUp(UnresolvedPromqlFunction.class, unresolved -> resolveFunction(unresolved, registry));
        return promql.withPromqlPlan(resolved);
    }

    static LogicalPlan resolveFunction(UnresolvedPromqlFunction unresolved, PromqlFunctionRegistry registry) {
        String name = unresolved.functionName();

        PromqlFunctionDefinition metadata = registry.functionMetadata(name);
        if (registry.isNotImplemented(name)) {
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
        for (int i = 0; i < rawParams.size(); i++) {
            // Variadic functions reuse their final parameter descriptor for any additional arguments;
            // arity checks above guarantee safe indexing for fixed-arity functions.
            PromqlFunctionDefinition.PromqlParamInfo expectedParam = i < functionParams.size()
                ? functionParams.get(i)
                : functionParams.getLast();
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
            if (metadata.functionType() != FunctionType.ACROSS_SERIES_AGGREGATION
                && metadata.functionType() != FunctionType.ACROSS_SERIES_REDUCTION) {
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
            if (metadata.functionType() == FunctionType.ACROSS_SERIES_REDUCTION) {
                return new AcrossSeriesReduction(unresolved.source(), child, metadata, extraParams, grouping, unresolved.groupingKeys());
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
            case ACROSS_SERIES_REDUCTION -> new AcrossSeriesReduction(
                unresolved.source(),
                child,
                metadata,
                extraParams,
                AcrossSeriesAggregate.Grouping.NONE,
                List.of()
            );
            case HISTOGRAM -> {
                var classicHistogramHandler = metadata.classicHistogramHandler();
                yield classicHistogramHandler == null
                    ? new ValueTransformationFunction(unresolved.source(), child, metadata, extraParams)
                    : classicHistogramHandler.build(unresolved.source(), child, metadata, extraParams);
            }
            case WITHIN_SERIES_AGGREGATION -> new WithinSeriesAggregate(unresolved.source(), child, metadata, extraParams);
            case VALUE_TRANSFORMATION -> new ValueTransformationFunction(unresolved.source(), child, metadata, extraParams);
            case VECTOR_CONVERSION -> new VectorConversionFunction(unresolved.source(), child, metadata, extraParams);
            case SCALAR_CONVERSION -> new ScalarConversionFunction(unresolved.source(), child, metadata, extraParams);
            case SCALAR, TIME_EXTRACTION -> child == null
                ? new ScalarFunction(unresolved.source(), metadata)
                : new ValueTransformationFunction(unresolved.source(), child, metadata, extraParams);
            case METADATA_MANIPULATION -> resolveMetadataManipulation(unresolved, child, metadata, extraParams);
            default -> throw new VerificationException(
                List.of(Failure.fail(unresolved, "Unsupported function type [{}] for function [{}]", metadata.functionType(), name))
            );
        };
    }

    /**
     * Resolves a {@code label_replace}/{@code label_join} call into a {@link MetadataManipulationFunction}, after validating
     * the arguments the way Prometheus does: the child must be an instant vector, the destination must be a valid label name,
     * and (for {@code label_replace}) the regular expression must compile. Structural constraints of the v1 scope (the
     * derived destination must be consumed by an enclosing {@code by(...)}) are enforced later, in
     * {@code PromqlCommand#verify} and the analyzer.
     */
    private static LogicalPlan resolveMetadataManipulation(
        UnresolvedPromqlFunction unresolved,
        LogicalPlan child,
        PromqlFunctionDefinition metadata,
        List<Expression> extraParams
    ) {
        String name = unresolved.functionName();
        if (child == null) {
            throw new VerificationException(
                List.of(Failure.fail(unresolved, "[{}] requires an instant vector as its first argument", name))
            );
        }
        // extraParams are the keyword-literal arguments after the child: [dst, ...]. The destination is validated here; the
        // remaining arguments (replacement/regex or separator/sources) are consumed during translation.
        String destination = literalString(extraParams.getFirst());
        if (PromqlLabels.isValidLabelName(destination) == false) {
            throw new VerificationException(
                List.of(Failure.fail(unresolved, "invalid destination label name [{}] in call to function [{}]", destination, name))
            );
        }
        if (metadata == PromqlBuiltinFunctionDefinitions.LABEL_JOIN) {
            // Prometheus (funcLabelJoin) validates every source label name, unlike label_replace which validates only the
            // destination. Mirror that here so an invalid source is rejected at analysis time rather than silently read as the
            // empty string. The source labels are the arguments after [dst, separator], i.e. extraParams[2..].
            for (int i = 2; i < extraParams.size(); i++) {
                String source = literalString(extraParams.get(i));
                if (PromqlLabels.isValidLabelName(source) == false) {
                    throw new VerificationException(
                        List.of(Failure.fail(unresolved, "invalid source label name [{}] in call to function [{}]", source, name))
                    );
                }
            }
        }
        if (metadata == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE) {
            String regex = literalString(extraParams.get(3));
            // Validate exactly as the evaluator/Prometheus compiles it, so a bad pattern fails at analysis rather than
            // execution. The anchoring and RE2/J compilation live in RegexExpand, keeping that dependency out of here.
            String regexError = RegexExpand.validateRegex(regex);
            if (regexError != null) {
                throw new VerificationException(
                    List.of(
                        Failure.fail(unresolved, "invalid regular expression [{}] in call to function [{}]: {}", regex, name, regexError)
                    )
                );
            }
        }
        return new MetadataManipulationFunction(unresolved.source(), child, metadata, extraParams);
    }

    private static String literalString(Expression e) {
        return BytesRefs.toString(((Literal) e).value());
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
