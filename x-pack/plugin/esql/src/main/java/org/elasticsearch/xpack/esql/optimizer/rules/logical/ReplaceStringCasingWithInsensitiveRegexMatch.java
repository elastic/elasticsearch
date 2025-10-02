/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.StringPattern;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ChangeCase;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStringCasingWithInsensitiveEquals.unwrapCase;

public class ReplaceStringCasingWithInsensitiveRegexMatch extends OptimizerRules.OptimizerExpressionRule<
    RegexMatch<? extends StringPattern>> {

    public ReplaceStringCasingWithInsensitiveRegexMatch() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected Expression rule(RegexMatch<? extends StringPattern> regexMatch, LogicalOptimizerContext unused) {
        if (regexMatch.field() instanceof ChangeCase changeCase) {
            Predicate<String> matchesCase = changeCase.caseType()::matchesCase;
            Expression unwrappedField = unwrapCase(regexMatch.field());
            return regexMatch.optimizeStringCasingWithInsensitiveRegexMatch(unwrappedField, matchesCase);
        }
        return regexMatch;
    }

}
