/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.StringPattern;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ChangeCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

/**
 * Replaces binary case-sensitive operations - like some comparisons and regex matches - involving a case-changing function (a subclass of
 * {@link ChangeCase}) and a string literal (be it an immediate value or a pattern) with an equivalent case-insensitive operation.
 */
public class ReplaceStringCasingWithInsensitiveEquivalent extends OptimizerRules.OptimizerExpressionRule<ScalarFunction> {

    public ReplaceStringCasingWithInsensitiveEquivalent() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected Expression rule(ScalarFunction sf, LogicalOptimizerContext ctx) {
        Expression e = sf;
        if (sf instanceof BinaryComparison bc) {
            e = rewriteBinaryComparison(ctx, sf, bc, false);
        } else if (sf instanceof Not not && not.field() instanceof BinaryComparison bc) {
            e = rewriteBinaryComparison(ctx, sf, bc, true);
        } else if (sf instanceof RegexMatch<? extends StringPattern> regexMatch) {
            e = rewriteRegexMatch(regexMatch);
        }
        return e;
    }

    private static Expression rewriteBinaryComparison(
        LogicalOptimizerContext ctx,
        ScalarFunction sf,
        BinaryComparison bc,
        boolean negated
    ) {
        Expression e = sf;
        if (bc.left() instanceof ChangeCase changeCase && bc.right().foldable()) {
            if (bc instanceof Equals) {
                e = replaceChangeCase(ctx, bc, changeCase, negated);
            } else if (bc instanceof NotEquals) { // not actually used currently, `!=` is built as `NOT(==)` already
                e = replaceChangeCase(ctx, bc, changeCase, negated == false);
            }
        }
        return e;
    }

    private static Expression replaceChangeCase(LogicalOptimizerContext ctx, BinaryComparison bc, ChangeCase changeCase, boolean negated) {
        var foldedRight = BytesRefs.toString(bc.right().fold(ctx.foldCtx()));
        var field = unwrapCase(changeCase.field());
        var e = changeCase.caseType().matchesCase(foldedRight)
            ? new InsensitiveEquals(bc.source(), field, bc.right())
            : Literal.of(bc, Boolean.FALSE);
        if (negated) {
            e = e instanceof Literal ? new IsNotNull(e.source(), field) : new Not(e.source(), e);
        }
        return e;
    }

    private static Expression rewriteRegexMatch(RegexMatch<? extends StringPattern> regexMatch) {
        Expression e = regexMatch;
        if (regexMatch.field() instanceof ChangeCase changeCase) {
            var pattern = regexMatch.pattern().pattern();
            e = changeCase.caseType().matchesCase(pattern)
                ? insensitiveRegexMatch(regexMatch)
                : Literal.of(regexMatch, Boolean.FALSE);
        }
        return e;
    }

    private static Expression insensitiveRegexMatch(RegexMatch<? extends StringPattern> regexMatch) {
        return switch (regexMatch) {
            case RLike rlike -> new RLike(rlike.source(), unwrapCase(rlike.field()), rlike.pattern(), true);
            case WildcardLike wildcardLike -> new WildcardLike(
                wildcardLike.source(),
                unwrapCase(wildcardLike.field()),
                wildcardLike.pattern(),
                true
            );
            default -> regexMatch;
        };
    }

    private static Expression unwrapCase(Expression e) {
        for (; e instanceof ChangeCase cc; e = cc.field()) {
        }
        return e;
    }
}
