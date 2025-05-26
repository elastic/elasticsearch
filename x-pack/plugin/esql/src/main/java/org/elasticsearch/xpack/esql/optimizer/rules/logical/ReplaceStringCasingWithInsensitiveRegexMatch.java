/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.StringPattern;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ChangeCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStringCasingWithInsensitiveEquals.unwrapCase;

public class ReplaceStringCasingWithInsensitiveRegexMatch extends OptimizerRules.OptimizerExpressionRule<
    RegexMatch<? extends StringPattern>> {

    public ReplaceStringCasingWithInsensitiveRegexMatch() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected Expression rule(RegexMatch<? extends StringPattern> regexMatch, LogicalOptimizerContext unused) {
        Expression e = regexMatch;
        if (regexMatch.field() instanceof ChangeCase changeCase) {
            var pattern = regexMatch.pattern().pattern();
            e = changeCase.caseType().matchesCase(pattern) ? insensitiveRegexMatch(regexMatch) : Literal.of(regexMatch, Boolean.FALSE);
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

}
