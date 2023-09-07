/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.ql.expression.predicate.regex.AbstractStringPattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;

import java.util.function.Supplier;

public abstract class RegexMapper extends ExpressionMapper<RegexMatch<?>> {

    public static final ExpressionMapper<?> REGEX_MATCH = new RegexMapper() {
        @Override
        public Supplier<EvalOperator.ExpressionEvaluator> map(RegexMatch<?> expression, Layout layout) {
            return () -> new org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.RegexMatchEvaluator(
                EvalMapper.toEvaluator(expression.field(), layout).get(),
                new CharacterRunAutomaton(((AbstractStringPattern) expression.pattern()).createAutomaton())
            );
        }
    };
}
