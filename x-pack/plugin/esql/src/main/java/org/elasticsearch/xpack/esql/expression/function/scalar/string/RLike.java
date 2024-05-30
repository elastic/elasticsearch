/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class RLike extends org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike implements EvaluatorMapper {

    @FunctionInfo(returnType = "boolean", description = """
        Use `RLIKE` to filter data based on string patterns using using
        <<regexp-syntax,regular expressions>>. `RLIKE` usually acts on a field placed on
        the left-hand side of the operator, but it can also act on a constant (literal)
        expression. The right-hand side of the operator represents the pattern.""", examples = @Example(file = "docs", tag = "rlike"))
    public RLike(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "A literal value.") Expression field,
        @Param(name = "pattern", type = { "keyword", "text" }, description = "A regular expression.") RLikePattern rLikePattern,
        @Param(
            name = "caseInsensitive",
            type = { "boolean" },
            description = "Indicate whether it is case insensitive."
        ) boolean caseInsensitive
    ) {
        super(source, field, rLikePattern, caseInsensitive);
    }

    public RLike(Source source, Expression value, RLikePattern pattern) {
        super(source, value, pattern);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike> info() {
        return NodeInfo.create(this, RLike::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected RLike replaceChild(Expression newChild) {
        return new RLike(source(), newChild, pattern(), caseInsensitive());
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        return AutomataMatch.toEvaluator(source(), toEvaluator.apply(field()), pattern().createAutomaton());
    }
}
