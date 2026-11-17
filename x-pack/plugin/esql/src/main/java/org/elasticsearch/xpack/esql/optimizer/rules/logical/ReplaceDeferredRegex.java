/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.DeferredRegexExpression;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

/**
 * Converts {@link DeferredRegexExpression} nodes into concrete {@code WildcardLike} or
 * {@code RLike} nodes after constant folding and eval propagation have run.
 * <p>
 * This rule runs after {@code PropagateEvalFoldables} and {@code ConstantFolding} so that a pattern
 * arriving via an {@code EVAL} alias is treated identically to an inline literal. For example,
 * {@code EVAL x = "demo*" | WHERE field LIKE x} and {@code WHERE field LIKE "demo*"} produce the
 * same expression. The folded pattern is handed to {@link ReplaceRegexMatch#replace} so the
 * constant-expression path benefits from the exact same {@code matchesAll}/{@code exactMatch}/
 * decomposition rewrites (and invalid-pattern error handling) as the inline-literal path, rather
 * than duplicating them here.
 * <p>
 * Nodes whose pattern is not foldable to a string (non-constant field reference, wrong type, or a
 * value that folds to null) are left as-is for
 * {@link DeferredRegexExpression#postOptimizationVerification} to report the appropriate error.
 */
public final class ReplaceDeferredRegex extends OptimizerRules.OptimizerExpressionRule<DeferredRegexExpression> {

    public ReplaceDeferredRegex() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected Expression rule(DeferredRegexExpression expr, LogicalOptimizerContext ctx) {
        Expression pattern = expr.patternExpression();
        if (pattern.foldable() == false || DataType.isString(pattern.dataType()) == false) {
            return expr;
        }
        Object val = pattern.fold(ctx.foldCtx());
        if (val == null) {
            return expr;
        }
        String patternStr = BytesRefs.toString(val);
        RegexMatch<?> regex = DeferredRegexExpression.buildRegexMatch(expr.source(), expr.field(), expr.variant(), patternStr);
        return ReplaceRegexMatch.replace(regex, ctx);
    }
}
