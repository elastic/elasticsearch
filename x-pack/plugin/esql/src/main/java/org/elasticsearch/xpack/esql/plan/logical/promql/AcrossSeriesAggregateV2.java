/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult;

import java.util.List;

import static java.util.function.Predicate.not;

/**
 * Translates the PromQL aggregations across series over a label container: the series are grouped by their labels after
 * the grouping, {@code by(...)} keeping the listed labels and {@code without(...)} leaving them out.
 */
final class AcrossSeriesAggregateV2 {

    private AcrossSeriesAggregateV2() {}

    /**
     * Translates the child once, then aggregates it keyed by the labels of each group. Within-series functions remain
     * expressions in the first aggregate.
     */
    static IntermediateResult translate(AcrossSeriesAggregate aggregate, TranslationContextV2 context) {
        IntermediateResult ir = context.translate(aggregate.child());
        if (ir.kind().constant) {
            return ir;
        }

        var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, ir.step(), context.configuration());
        Expression function = aggregate.buildEsqlFunction(ir.value(), promqlCtx);

        // A raw operand collapses once, with the operator's function fused into the per-series aggregate; a table regroups.
        Expression labels = groupLabels(aggregate, ir.labels());
        IntermediateResult result = ir.kind().afterInitialAggregation
            ? context.emitAggregate(ir, labels, function)
            : context.emitInitialAggregate(ir, labels, function);
        // A count is at least 1 for an element. A group none of whose series has a value - every series of an operator
        // left without a partner, say - is no element in Prometheus, not a count of 0.
        if (function instanceof Count) {
            result = context.eval(result, TranslationContextV2.zeroCountAsNull(result.valueColumn()));
        }
        return result;
    }

    /**
     * The labels of a group: {@code without(...)} leaves the listed labels out, {@code by(...)} keeps only the listed labels;
     * a group without labels has none.
     */
    private static @Nullable Expression groupLabels(AcrossSeriesAggregate aggregate, @Nullable Expression input) {
        if (input == null) {
            return null;
        }
        if (aggregate.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT) {
            List<String> dropped = PromqlLabels.labelNames(aggregate.groupings());
            return new TimeSeriesMetadataFilter(input.source(), input, PromqlLabels.labelLiterals(input.source(), dropped), true);
        }
        // absent labels and metric fields are no labels of a group
        List<String> kept = PromqlLabels.labelNames(
            aggregate.groupings()
                .stream()
                .filter(a -> a.resolved() && a.dataType() != DataType.NULL)
                .filter(not(a -> a instanceof FieldAttribute fa && fa.isMetric()))
                .toList()
        );
        return kept.isEmpty()
            ? null
            : new TimeSeriesMetadataFilter(input.source(), input, PromqlLabels.labelLiterals(input.source(), kept), false);
    }
}
