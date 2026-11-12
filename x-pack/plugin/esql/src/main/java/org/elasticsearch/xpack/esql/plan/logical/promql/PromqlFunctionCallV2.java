/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

/**
 * Translates the PromQL functions over a label container: a function over each series changes its value and keeps its
 * labels; a function returning a scalar drops them.
 */
final class PromqlFunctionCallV2 {

    private PromqlFunctionCallV2() {}

    /**
     * A generic PromQL function call (rate, ceil, abs, etc.): an expression over the child's value, over the same series.
     * Shared by the function families that need no special lowering.
     */
    static IntermediateResult translate(PromqlFunctionCall function, TranslationContextV2 context) {
        return apply(function, context, context.translate(function.child()));
    }

    /** Applies {@code function} to the value of its already translated child. */
    static IntermediateResult apply(PromqlFunctionCall function, TranslationContextV2 context, IntermediateResult child) {
        if (child.kind().constant) {
            return child;
        }
        Expression window = AggregateFunction.NO_WINDOW;
        if (function.child() instanceof RangeSelector rangeSelector) {
            window = context.cmd().resolveRangeWindow(rangeSelector.range());
        }
        var promqlCtx = new PromqlContext(context.time(), window, child.step(), context.configuration(), context.cmd().timestamp());
        return context.eval(
            child,
            TranslationContextV2.emptyRangeAsNull(function.buildEsqlFunction(child.value(), promqlCtx), context.time())
        );
    }

    /**
     * A scalar function (time(), etc.): an expression over the unchanged source. It reads the step of the frame it
     * translates under, which an independent pipeline keys afresh; typed as the command's step, since the frame's may not
     * be resolved yet.
     */
    static IntermediateResult translate(ScalarFunction function, TranslationContextV2 context) {
        Attribute step = context.cmd().stepAttribute().withId(context.stepAttr().id());
        var esqlFunction = function.buildEsqlFunction(new PromqlContext(context.cmd().timestamp(), null, step, context.configuration()));
        return new IntermediateResult(context.cmd().child(), esqlFunction, null, context.stepAttr(), null);
    }

    /** {@code scalar(v)}: the single series' value per step, or NaN otherwise; the result has no labels. */
    static IntermediateResult translate(ScalarConversionFunction function, TranslationContextV2 context) {
        IntermediateResult child = context.translate(function.child());
        if (child.value().foldable()) {
            Expression value = new ToDouble(function.source(), child.value());
            return new IntermediateResult(child.plan(), value, null, child.step(), child.pendingFilter());
        }
        var scalarExpr = new Scalar(function.source(), child.value());
        return child.kind().afterInitialAggregation
            ? context.emitAggregate(child, null, scalarExpr)
            : context.emitInitialAggregate(child, null, scalarExpr);
    }
}
