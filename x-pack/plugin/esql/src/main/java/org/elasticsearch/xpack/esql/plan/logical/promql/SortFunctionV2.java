/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult;

/**
 * Translates {@code sort}, {@code sort_desc}, {@code sort_by_label} and {@code sort_by_label_desc} over a label container. The
 * order itself is applied around the command; the series pass through, and the command reads the labels it orders by from the
 * label container.
 */
final class SortFunctionV2 {

    private SortFunctionV2() {}

    /** {@code sort} and {@code sort_desc} pass the series through; over native histograms they return nothing. */
    static IntermediateResult translate(SortFunction sort, TranslationContextV2 context) {
        IntermediateResult result = context.translate(sort.child());
        if (result.value().resolved() && result.value().dataType().isHistogram()) {
            var skipAllFilter = new Filter(sort.source(), result.plan(), Literal.FALSE);
            var nullGrouping = new Values(sort.source(), new Literal(sort.source(), null, DataType.DOUBLE));
            IntermediateResult skipped = result.with(skipAllFilter, result.value());
            return skipped.kind().afterInitialAggregation
                ? context.emitAggregate(skipped, skipped.labels(), nullGrouping)
                : context.emitInitialAggregate(skipped, skipped.labels(), nullGrouping);
        }
        return result;
    }

    /** {@code sort_by_label} passes the series through: its labels stay in the label container. */
    static IntermediateResult translate(SortByLabelFunction sort, TranslationContextV2 context) {
        return context.translate(sort.child());
    }
}
