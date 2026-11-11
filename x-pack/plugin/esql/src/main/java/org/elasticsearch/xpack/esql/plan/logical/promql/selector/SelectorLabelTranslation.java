/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.util.List;

/**
 * Translates the selectors over a label container: a selector over the source starts its series from the source's label
 * container, and its label matchers become a source predicate.
 */
public final class SelectorLabelTranslation {

    private SelectorLabelTranslation() {}

    /** The last sample of each series in the lookback window. */
    public static IntermediateResult translate(InstantSelector selector, TranslationContextV2 context) {
        return series(
            selector,
            context,
            new LastOverTime(selector.source(), selector.series(), AggregateFunction.NO_WINDOW, context.time())
        );
    }

    /** The samples of each series in the range. */
    public static IntermediateResult translate(RangeSelector selector, TranslationContextV2 context) {
        return series(selector, context, selector.series());
    }

    /** A literal per step: a vector without labels. */
    public static IntermediateResult translate(LiteralSelector selector, TranslationContextV2 context) {
        LogicalPlan input = context.cmd().child();
        LogicalPlan foldedPlan = PromqlLogicalPlanBuilder.tryFoldRelation(context.cmd(), input);
        Expression matcher = selector.labelMatchers().predicate(selector.source(), selector.labels(), context.configuration());

        if (foldedPlan != null) {
            // a compile-time relation carries its own step column
            Attribute foldedStep = PromqlLabels.getLabel(foldedPlan.output(), context.cmd().stepColumnName());
            return new IntermediateResult(foldedPlan, selector.literal(), null, foldedStep, matcher, Kind.CONSTANT);
        }
        return new IntermediateResult(input, selector.literal(), null, context.stepAttr(), matcher);
    }

    /**
     * A selector over the source reading {@code value} per series; source predicates and evaluation time are applied here.
     */
    private static IntermediateResult series(Selector selector, TranslationContextV2 context, Expression value) {
        LogicalPlan input = context.cmd().child();
        LogicalPlan foldedPlan = PromqlLogicalPlanBuilder.tryFoldRelation(context.cmd(), input);
        Expression matcher = selector.labelMatchers().predicate(selector.source(), selector.labels(), context.configuration());

        if (foldedPlan != null) {
            var empty = new LocalRelation(
                context.cmd().source(),
                List.of(context.cmd().valueAttribute(), context.cmd().stepAttribute()),
                EmptyLocalSupplier.EMPTY
            );
            return new IntermediateResult(empty, Literal.NULL, null, context.cmd().stepAttribute(), null, Kind.CONSTANT);
        }
        return new IntermediateResult(input, value, TranslationContextV2.sourceLabels(selector.source()), context.stepAttr(), matcher);
    }
}
