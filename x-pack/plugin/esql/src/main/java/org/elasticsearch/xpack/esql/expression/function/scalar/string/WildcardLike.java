/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.automaton.Automata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
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

public class WildcardLike extends org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardLike
    implements
        EvaluatorMapper,
        TranslationAware.SingleValueTranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WildcardLike",
        WildcardLike::new
    );

    @FunctionInfo(returnType = "boolean", description = """
        Use `LIKE` to filter data based on string patterns using wildcards. `LIKE`
        usually acts on a field placed on the left-hand side of the operator, but it can
        also act on a constant (literal) expression. The right-hand side of the operator
        represents the pattern.

        The following wildcard characters are supported:

        * `*` matches zero or more characters.
        * `?` matches one character.""", detailedDescription = """
        Matching the exact characters `*` and `.` will require escaping.
        The escape character is backslash `\\`. Since also backslash is a special character in string literals,
        it will require further escaping.

        <<load-esql-example, file=string tag=likeEscapingSingleQuotes>>

        To reduce the overhead of escaping, we suggest using triple quotes strings `\"\"\"`

        <<load-esql-example, file=string tag=likeEscapingTripleQuotes>>
        """, operator = "LIKE", examples = @Example(file = "docs", tag = "like"))
    public WildcardLike(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "A literal expression.") Expression left,
        @Param(name = "pattern", type = { "keyword", "text" }, description = "Pattern.") WildcardPattern pattern
    ) {
        super(source, left, pattern, false);
    }

    private WildcardLike(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), new WildcardPattern(in.readString()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeString(pattern().pattern());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardLike> info() {
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
    public Boolean fold(FoldContext ctx) {
        return (Boolean) EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return AutomataMatch.toEvaluator(
            source(),
            toEvaluator.apply(field()),
            // The empty pattern will accept the empty string
            pattern().pattern().length() == 0 ? Automata.makeEmptyString() : pattern().createAutomaton()
        );
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableAttribute(field()) ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var field = field();
        LucenePushdownPredicates.checkIsPushableAttribute(field);
        return translateField(handler.nameOf(field instanceof FieldAttribute fa ? fa.exactAttribute() : field));
    }

    // TODO: see whether escaping is needed
    private Query translateField(String targetFieldName) {
        return new WildcardQuery(source(), targetFieldName, pattern().asLuceneWildcard(), caseInsensitive());
    }

    @Override
    public Expression singleValueField() {
        return field();
    }
}
