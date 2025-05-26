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
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class RLike extends org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike
    implements
        EvaluatorMapper,
        TranslationAware.SingleValueTranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "RLike", RLike::new);

    @FunctionInfo(returnType = "boolean", description = """
        Use `RLIKE` to filter data based on string patterns using using
        <<regexp-syntax,regular expressions>>. `RLIKE` usually acts on a field placed on
        the left-hand side of the operator, but it can also act on a constant (literal)
        expression. The right-hand side of the operator represents the pattern.""", detailedDescription = """
        Matching special characters (eg. `.`, `*`, `(`...) will require escaping.
        The escape character is backslash `\\`. Since also backslash is a special character in string literals,
        it will require further escaping.

        <<load-esql-example, file=string tag=rlikeEscapingSingleQuotes>>

        To reduce the overhead of escaping, we suggest using triple quotes strings `\"\"\"`

        <<load-esql-example, file=string tag=rlikeEscapingTripleQuotes>>
        """, operator = "RLIKE", examples = @Example(file = "docs", tag = "rlike"))
    public RLike(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "A literal value.") Expression value,
        @Param(name = "pattern", type = { "keyword", "text" }, description = "A regular expression.") RLikePattern pattern
    ) {
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
    protected NodeInfo<RLike> info() {
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
    public Boolean fold(FoldContext ctx) {
        return (Boolean) EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return AutomataMatch.toEvaluator(source(), toEvaluator.apply(field()), pattern().createAutomaton());
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableFieldAttribute(field()) ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fa = LucenePushdownPredicates.checkIsFieldAttribute(field());
        // TODO: see whether escaping is needed
        return new RegexQuery(source(), handler.nameOf(fa.exactAttribute()), pattern().asJavaRegex(), caseInsensitive());
    }

    @Override
    public Expression singleValueField() {
        return field();
    }
}
