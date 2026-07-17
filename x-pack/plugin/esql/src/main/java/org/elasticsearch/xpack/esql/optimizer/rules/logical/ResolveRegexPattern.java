/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.UnresolvedRegexExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

/**
 * Converts {@link UnresolvedRegexExpression} nodes into concrete {@link WildcardLike} or
 * {@link RLike} nodes after constant folding and eval propagation have run.
 * <p>
 * This rule runs after {@code PropagateEvalFoldables} and {@code ConstantFolding} so that
 * a pattern arriving via an {@code EVAL} alias is treated identically to an inline literal.
 * For example, {@code EVAL x = "demo*" | WHERE field LIKE x} and
 * {@code WHERE field LIKE "demo*"} both produce the same {@link WildcardLike} node.
 * <p>
 * Nodes whose pattern is not foldable to a string (non-constant field reference, wrong type)
 * are left as-is for {@link UnresolvedRegexExpression#postOptimizationVerification} to report
 * the appropriate error.
 */
public final class ResolveRegexPattern extends OptimizerRules.OptimizerExpressionRule<UnresolvedRegexExpression> {

    public ResolveRegexPattern() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected Expression rule(UnresolvedRegexExpression expr, LogicalOptimizerContext ctx) {
        Expression pattern = expr.patternExpression();
        if (pattern.foldable() == false) {
            return expr;
        }
        if (DataType.isString(pattern.dataType()) == false) {
            return expr;
        }
        Object val = pattern.fold(FoldContext.small());
        if (val == null) {
            return expr;
        }
        String patternStr = BytesRefs.toString(val);
        return switch (expr.variant()) {
            case LIKE -> {
                WildcardPattern wp = new WildcardPattern(patternStr);
                if (wp.matchesAll()) {
                    yield new IsNotNull(expr.source(), expr.field());
                }
                String exact = wp.exactMatch();
                if (exact != null) {
                    yield new Equals(expr.source(), expr.field(), Literal.keyword(expr.source(), exact));
                }
                yield new WildcardLike(expr.source(), expr.field(), wp, expr.caseInsensitive());
            }
            case RLIKE -> {
                RLikePattern rp = new RLikePattern(patternStr);
                if (rp.matchesAll()) {
                    yield new IsNotNull(expr.source(), expr.field());
                }
                String exact = rp.exactMatch();
                if (exact != null) {
                    yield new Equals(expr.source(), expr.field(), Literal.keyword(expr.source(), exact));
                }
                yield new RLike(expr.source(), expr.field(), rp, expr.caseInsensitive());
            }
        };
    }
}
