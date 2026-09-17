/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFunctionComparisonRewriter;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

/**
 * After literals sit on the right, invert {@code DATE_TRUNC} / monotonic {@code DATE_EXTRACT}
 * comparisons into timestamp ranges so later combination and source pushdown can see
 * field-vs-literal predicates.
 */
public final class RewriteDateFunctionComparisons extends OptimizerRules.OptimizerExpressionRule<EsqlBinaryComparison> {

    public RewriteDateFunctionComparisons() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected Expression rule(EsqlBinaryComparison cmp, LogicalOptimizerContext ctx) {
        Expression rewritten = DateFunctionComparisonRewriter.tryRewriteComparison(cmp, ctx.foldCtx());
        return rewritten != null ? rewritten : cmp;
    }
}
