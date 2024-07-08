/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class RLike extends org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "RLike", RLike::new);

    public RLike(Source source, Expression value, RLikePattern pattern) {
        super(source, value, pattern);
    }

    public RLike(Source source, Expression field, RLikePattern rLikePattern, boolean caseInsensitive) {
        super(source, field, rLikePattern, caseInsensitive);
    }

    private RLike(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), new RLikePattern(in.readString()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeString(pattern().asJavaRegex());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
