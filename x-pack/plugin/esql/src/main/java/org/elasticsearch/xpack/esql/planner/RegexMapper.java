/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.predicate.regex.AbstractStringPattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;

import java.util.function.Supplier;

public abstract class RegexMapper extends EvalMapper.ExpressionMapper<RegexMatch<?>> {
    static final EvalMapper.ExpressionMapper<?> REGEX_MATCH = new RegexMapper() {
        @Override
        protected Supplier<EvalOperator.ExpressionEvaluator> map(RegexMatch<?> expression, Layout layout) {
            return () -> new org.elasticsearch.xpack.esql.expression.predicate.operator.regex.RegexMatchEvaluator(
                EvalMapper.toEvaluator(expression.field(), layout).get(),
                new CharacterRunAutomaton(((AbstractStringPattern) expression.pattern()).createAutomaton())
            );
        }
    };
}
