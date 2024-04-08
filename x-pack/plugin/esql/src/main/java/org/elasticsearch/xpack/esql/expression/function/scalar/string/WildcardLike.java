/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.automaton.Automata;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class WildcardLike extends org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardLike implements EvaluatorMapper {
    public WildcardLike(Source source, Expression left, WildcardPattern pattern) {
        super(source, left, pattern, false);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardLike> info() {
        return NodeInfo.create(this, WildcardLike::new, field(), pattern());
    }

    @Override
    protected WildcardLike replaceChild(Expression newLeft) {
        return new WildcardLike(source(), newLeft, pattern());
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        return AutomataMatch.toEvaluator(
            source(),
            toEvaluator.apply(field()),
            // The empty pattern will accept the empty string
            pattern().pattern().length() == 0 ? Automata.makeEmptyString() : pattern().createAutomaton()
        );
    }
}
