/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexSyntax;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

/**
 * Reads the patterns of {@code RLIKE}, one or a list, in RE2 syntax when the {@code regex_syntax} query setting asks for
 * it, as {@link Re2Like}. A pattern that is a constant expression rather than a literal is read in it when
 * {@link ReplaceDeferredRegex} folds it.
 */
public final class ApplyRegexSyntax extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (QuerySettings.REGEX_SYNTAX.get(context.configuration().resolvedSettings()) != RegexSyntax.RE2) {
            return plan;
        }
        return plan.transformExpressionsUp(Expression.class, ApplyRegexSyntax::re2);
    }

    private static Expression re2(Expression expression) {
        if (expression instanceof RLike rLike) {
            assert rLike.caseInsensitive() == false : "the analyzer sees RLIKE as written, case-sensitive";
            return new Re2Like(rLike.source(), rLike.field(), new Re2Pattern(rLike.pattern().pattern()));
        }
        if (expression instanceof RLikeList rLikeList) {
            return Predicates.combineOr(
                rLikeList.pattern()
                    .patternList()
                    .stream()
                    .map(pattern -> (Expression) new Re2Like(rLikeList.source(), rLikeList.field(), new Re2Pattern(pattern.pattern())))
                    .toList()
            );
        }
        return expression;
    }
}
