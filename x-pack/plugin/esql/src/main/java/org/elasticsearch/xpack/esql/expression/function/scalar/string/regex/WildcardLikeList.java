/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string.regex;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPatternList;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

public class WildcardLikeList extends RegexMatch<WildcardPatternList> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WildcardLikeList",
        WildcardLikeList::new
    );
    public static final String NAME = "LIKELIST";

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
        """, operator = NAME, examples = @Example(file = "docs", tag = "like"))
    public WildcardLikeList(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "A literal expression.") Expression left,
        @Param(name = "pattern", type = { "keyword", "text" }, description = "Pattern.") WildcardPatternList patterns
    ) {
        this(source, left, patterns, false);
    }

    public WildcardLikeList(Source source, Expression left, WildcardPatternList patterns, boolean caseInsensitive) {
        super(source, left, patterns, caseInsensitive);
    }

    public WildcardLikeList(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            WildcardPatternList.readFrom(in),
            deserializeCaseInsensitivity(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        pattern().writeTo(out);
        serializeCaseInsensitivity(out);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<WildcardLikeList> info() {
        return NodeInfo.create(this, WildcardLikeList::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected WildcardLikeList replaceChild(Expression newLeft) {
        return new WildcardLikeList(source(), newLeft, pattern(), caseInsensitive());
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        if (pattern().patternList().size() != 1) {
            // we only support a single pattern in the list for pushdown for now
            return Translatable.NO;
        }
        return pushdownPredicates.isPushableAttribute(field()) ? Translatable.YES : Translatable.NO;

    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var field = field();
        LucenePushdownPredicates.checkIsPushableAttribute(field);
        return translateField(handler.nameOf(field instanceof FieldAttribute fa ? fa.exactAttribute() : field));
    }

    private Query translateField(String targetFieldName) {
        if (pattern().patternList().size() != 1) {
            throw new IllegalArgumentException("WildcardLikeList can only be translated when it has a single pattern");
        }
        return new WildcardQuery(source(), targetFieldName, pattern().patternList().getFirst().asLuceneWildcard(), caseInsensitive());
    }
}
