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
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ChangeCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.parser.ParsingException;

public final class ReplaceRegexMatch extends OptimizerRules.OptimizerExpressionRule<RegexMatch<?>> {

    public ReplaceRegexMatch() {
        // UP: when this rule rewrites WildcardLike → And(StartsWith, WildcardLike),
        // the inner WildcardLike child was already visited, preventing re-entry.
        // DOWN would descend into the new WildcardLike child and loop infinitely.
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(RegexMatch<?> regexMatch, LogicalOptimizerContext ctx) {
        Expression e = regexMatch;
        StringPattern pattern = regexMatch.pattern();
        boolean matchesAll;
        try {
            matchesAll = pattern.matchesAll();
        } catch (IllegalArgumentException ex) {
            throw new ParsingException(
                regexMatch.source(),
                "Invalid regex pattern for RLIKE [{}]: [{}]",
                regexMatch.pattern().pattern(),
                ex.getMessage()
            );
        }
        if (matchesAll) {
            e = new IsNotNull(e.source(), regexMatch.field());
        } else {
            String match = pattern.exactMatch();
            if (match != null) {
                Literal literal = Literal.keyword(regexMatch.source(), match);
                e = regexToEquals(regexMatch, literal);
            } else if (regexMatch instanceof WildcardLike wl
                && wl.caseInsensitive() == false
                && (wl.field() instanceof ChangeCase) == false) {
                    Expression decomposed = decomposeWildcardLike(wl);
                    if (decomposed != null) {
                        e = decomposed;
                    }
                }
        }
        return e;
    }

    protected Expression regexToEquals(RegexMatch<?> regexMatch, Literal literal) {
        return new Equals(regexMatch.source(), regexMatch.field(), literal);
    }

    private static Expression decomposeWildcardLike(WildcardLike wl) {
        WildcardPattern wp = wl.pattern();
        String prefix = wp.extractPrefix();
        String raw = wp.pattern();

        if (prefix != null && raw.equals(StringUtils.escapeWildcardLiteral(prefix) + "*")) {
            return new StartsWith(wl.source(), wl.field(), Literal.keyword(wl.source(), prefix));
        }
        String suffix = wp.extractSuffix();
        if (suffix != null && raw.equals("*" + StringUtils.escapeWildcardLiteral(suffix))) {
            return new EndsWith(wl.source(), wl.field(), Literal.keyword(wl.source(), suffix));
        }
        if (prefix != null) {
            return new And(wl.source(), new StartsWith(wl.source(), wl.field(), Literal.keyword(wl.source(), prefix)), wl);
        }
        return null;
    }
}
