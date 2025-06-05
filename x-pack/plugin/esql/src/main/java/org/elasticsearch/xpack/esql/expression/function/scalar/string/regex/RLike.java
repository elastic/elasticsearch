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
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

public class RLike extends RegexMatch<RLikePattern> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "RLike", RLike::new);
    public static final String NAME = "RLIKE";

    @FunctionInfo(returnType = "boolean", description = """
        Use `RLIKE` to filter data based on string patterns using using
        <<regexp-syntax,regular expressions>>. `RLIKE` usually acts on a field placed on
        the left-hand side of the operator, but it can also act on a constant (literal)
        expression. The right-hand side of the operator represents the pattern.""", detailedDescription = """
        Matching special characters (eg. `.`, `*`, `(`...) will require escaping.
        The escape character is backslash `\\`. Since also backslash is a special character in string literals,
        it will require further escaping.

        [source.merge.styled,esql]
        ----
        include::{esql-specs}/string.csv-spec[tag=rlikeEscapingSingleQuotes]
        ----

        To reduce the overhead of escaping, we suggest using triple quotes strings `\"\"\"`

        [source.merge.styled,esql]
        ----
        include::{esql-specs}/string.csv-spec[tag=rlikeEscapingTripleQuotes]
        ----
        """, operator = NAME, examples = @Example(file = "docs", tag = "rlike"))
    public RLike(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "A literal value.") Expression value,
        @Param(name = "pattern", type = { "keyword", "text" }, description = "A regular expression.") RLikePattern pattern
    ) {
        this(source, value, pattern, false);
    }

    public RLike(Source source, Expression field, RLikePattern rLikePattern, boolean caseInsensitive) {
        super(source, field, rLikePattern, caseInsensitive);
    }

    private RLike(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            new RLikePattern(in.readString()),
            deserializeCaseInsensitivity(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeString(pattern().asJavaRegex());
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
    protected NodeInfo<RLike> info() {
        return NodeInfo.create(this, RLike::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected RLike replaceChild(Expression newChild) {
        return new RLike(source(), newChild, pattern(), caseInsensitive());
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fa = LucenePushdownPredicates.checkIsFieldAttribute(field());
        // TODO: see whether escaping is needed
        return new RegexQuery(source(), handler.nameOf(fa.exactAttribute()), pattern().asJavaRegex(), caseInsensitive());
    }
}
