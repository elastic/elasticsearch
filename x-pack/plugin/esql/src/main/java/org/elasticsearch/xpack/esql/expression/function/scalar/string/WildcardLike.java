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
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class WildcardLike extends org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardLike implements EvaluatorMapper {
    @FunctionInfo(returnType = "boolean", description = """
        Use `LIKE` to filter data based on string patterns using wildcards. `LIKE`
        usually acts on a field placed on the left-hand side of the operator, but it can
        also act on a constant (literal) expression. The right-hand side of the operator
        represents the pattern.

        The following wildcard characters are supported:

        * `*` matches zero or more characters.
        * `?` matches one character.""", examples = @Example(file = "docs", tag = "like"))
    public WildcardLike(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }) Expression left,
        @Param(name = "pattern", type = { "keyword", "text" }) WildcardPattern pattern
    ) {
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
