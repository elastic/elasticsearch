/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.UnresolvedRegexExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Converts {@link UnresolvedRegexExpression} nodes into concrete {@link WildcardLike} or {@link RLike}
 * nodes once both children (field and pattern expression) are resolved and the pattern expression is
 * foldable.
 * <p>
 * This rule runs in the "Resolution" batch of the analyzer, which iterates until fixpoint. After the
 * pattern expression is resolved (functions resolved, references bound), this rule folds the constant
 * expression to a string and builds the concrete regex node. If the pattern is not foldable, the node
 * remains unresolved and the verifier reports the type error from
 * {@link UnresolvedRegexExpression#resolveType()}.
 */
public class FoldConstantRegexPattern extends AnalyzerRules.AnalyzerRule<LogicalPlan> {

    @Override
    protected boolean skipResolved() {
        // Run even on "resolved" plans so we can fold UnresolvedRegexExpression nodes whose
        // children became resolved in an earlier rule within the same batch iteration.
        return false;
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return plan.transformExpressionsDown(UnresolvedRegexExpression.class, expr -> {
            if (expr.childrenResolved() == false || expr.typeResolved().unresolved()) {
                return expr;
            }
            if (expr.patternExpression().foldable() == false) {
                return expr;
            }
            Object val = expr.patternExpression().fold(FoldContext.small());
            if (val == null) {
                return expr;
            }
            String patternStr = BytesRefs.toString(val);
            return switch (expr.variant()) {
                case LIKE -> new WildcardLike(expr.source(), expr.field(), new WildcardPattern(patternStr), expr.caseInsensitive());
                case RLIKE -> new RLike(expr.source(), expr.field(), new RLikePattern(patternStr), expr.caseInsensitive());
            };
        });
    }
}
