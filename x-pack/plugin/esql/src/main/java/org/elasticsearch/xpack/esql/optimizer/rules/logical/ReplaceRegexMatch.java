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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.parser.ParsingException;

public final class ReplaceRegexMatch extends OptimizerRules.OptimizerExpressionRule<RegexMatch<?>> {

    public ReplaceRegexMatch() {
        super(OptimizerRules.TransformDirection.DOWN);
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
                Literal literal = new Literal(regexMatch.source(), match, DataType.KEYWORD);
                e = regexToEquals(regexMatch, literal);
            }
        }
        return e;
    }

    protected Expression regexToEquals(RegexMatch<?> regexMatch, Literal literal) {
        return new Equals(regexMatch.source(), regexMatch.field(), literal);
    }
}
