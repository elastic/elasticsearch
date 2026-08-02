/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesReduction;
import org.elasticsearch.xpack.esql.plan.logical.promql.HistogramQuantile;
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

        // Label metadata-manipulation functions take an instant-vector child plus KEYWORD-literal arguments, so they bypass
        // the generic PromqlDataType loop below (which would reject the string literals, whose selector type is SCALAR).
        if (metadata.functionType() == FunctionType.METADATA_MANIPULATION) {
            return resolveMetadataManipulation(unresolved, metadata, rawParams);
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
            case HISTOGRAM -> metadata == PromqlHistogramQuantile.PROMQL_DEFINITION
                ? new HistogramQuantile(unresolved.source(), child, metadata, extraParams)
                : new ValueTransformationFunction(unresolved.source(), child, metadata, extraParams);
            case WITHIN_SERIES_AGGREGATION -> new WithinSeriesAggregate(unresolved.source(), child, metadata, extraParams);
            case VALUE_TRANSFORMATION -> new ValueTransformationFunction(unresolved.source(), child, metadata, extraParams);
            case VECTOR_CONVERSION -> new VectorConversionFunction(unresolved.source(), child, metadata, extraParams);
            case SCALAR_CONVERSION -> new ScalarConversionFunction(unresolved.source(), child, metadata, extraParams);
            case SCALAR, TIME_EXTRACTION -> child == null
                ? new ScalarFunction(unresolved.source(), metadata)
                : new ValueTransformationFunction(unresolved.source(), child, metadata, extraParams);
            default -> throw new VerificationException(
                List.of(Failure.fail(unresolved, "Unsupported function type [{}] for function [{}]", metadata.functionType(), name))
            );
        };
    }

    /**
     * Resolves a {@code label_replace}/{@code label_join} call into a {@link MetadataManipulationFunction}. Unlike the
     * generic resolution path, the trailing arguments are string ({@code KEYWORD}) literals rather than vectors, and the
     * child must be an instant vector (a range-vector argument is rejected, matching Prometheus). Also performs Prometheus's
     * analysis-time parity checks: the regex compiles via RE2/J and the destination (and, for {@code label_join}, source)
     * label names are valid.
     */
    private static LogicalPlan resolveMetadataManipulation(
        UnresolvedPromqlFunction unresolved,
        PromqlFunctionDefinition metadata,
        List<LogicalPlan> rawParams
    ) {
        String name = metadata.name();

        // label_replace/label_join are not aggregations, so a by(...)/without(...) grouping clause is invalid PromQL
        // and is rejected here rather than silently dropped (mirroring the guard the generic path applies to other
        // non-aggregation functions).
        AcrossSeriesAggregate.Grouping grouping = unresolved.grouping();
        if (grouping != null) {
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

        LogicalPlan child = rawParams.getFirst();
        PromqlDataType childType = PromqlPlan.getType(child);
        if (childType != PromqlDataType.INSTANT_VECTOR) {
            throw new VerificationException(
                List.of(
                    Failure.fail(
                        unresolved,
                        "expected type {} in call to function [{}], got {}",
                        PromqlDataType.INSTANT_VECTOR,
                        name,
                        childType
                    )
                )
            );
        }

        List<Expression> extraParams = new ArrayList<>(rawParams.size() - 1);
        for (int i = 1; i < rawParams.size(); i++) {
            LogicalPlan providedParam = rawParams.get(i);
            if (providedParam instanceof LiteralSelector literalSelector && literalSelector.literal().dataType() == DataType.KEYWORD) {
                extraParams.add(literalSelector.literal());
            } else {
                throw new VerificationException(
                    List.of(
                        Failure.fail(
                            unresolved,
                            "expected string literal parameter in call to function [{}], got {}",
                            name,
                            providedParam.nodeName()
                        )
                    )
                );
            }
        }

        validateLabelFunctionArguments(unresolved, metadata, extraParams);
        return new MetadataManipulationFunction(unresolved.source(), child, metadata, extraParams);
    }

    /**
     * Applies Prometheus's analysis-time validation for the label functions. {@code label_replace} compiles its regex via
     * RE2/J (anchored as {@code ^(?s:regex)$}, matching Prometheus) and validates the destination label name;
     * {@code label_join} validates every source label name and the destination label name.
     */
    private static void validateLabelFunctionArguments(
        UnresolvedPromqlFunction unresolved,
        PromqlFunctionDefinition metadata,
        List<Expression> extraParams
    ) {
        String name = metadata.name();
        if (metadata == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE) {
            // extraParams: [dst_label, replacement, src_label, regex]
            String regex = stringValue(extraParams.get(3));
            try {
                com.google.re2j.Pattern.compile("^(?s:" + regex + ")$");
            } catch (com.google.re2j.PatternSyntaxException e) {
                throw new VerificationException(
                    List.of(
                        Failure.fail(
                            unresolved,
                            "invalid regular expression [{}] in call to function [{}]: {}",
                            regex,
                            name,
                            e.getMessage()
                        )
                    )
                );
            }
            requireValidLabelName(unresolved, name, "destination", stringValue(extraParams.get(0)));
        } else if (metadata == PromqlBuiltinFunctionDefinitions.LABEL_JOIN) {
            // extraParams: [dst_label, separator, src_label_1, ... src_label_N]
            for (int i = 2; i < extraParams.size(); i++) {
                requireValidLabelName(unresolved, name, "source", stringValue(extraParams.get(i)));
            }
            requireValidLabelName(unresolved, name, "destination", stringValue(extraParams.get(0)));
        } else {
            throw new IllegalStateException("unexpected metadata-manipulation function [" + name + "]");
        }
    }

    private static void requireValidLabelName(UnresolvedPromqlFunction unresolved, String function, String role, String labelName) {
        if (PromqlLabels.isValidLabelName(labelName) == false) {
            throw new VerificationException(
                List.of(Failure.fail(unresolved, "invalid {} label name [{}] in call to function [{}]", role, labelName, function))
            );
        }
    }

    private static String stringValue(Expression literal) {
        return ((BytesRef) ((Literal) literal).value()).utf8ToString();
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
