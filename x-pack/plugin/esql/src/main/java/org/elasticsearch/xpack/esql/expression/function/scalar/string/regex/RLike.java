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
import org.elasticsearch.xpack.esql.core.expression.Literal;
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
import java.util.function.Predicate;

public class RLike extends RegexMatch<RLikePattern> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "RLike", RLike::new);
    public static final String NAME = "RLIKE";

    @FunctionInfo(
        returnType = "boolean",
        description = """
            Use `RLIKE` to filter data based on string patterns using
            <<regexp-syntax,regular expressions>>. `RLIKE` usually acts on a field placed on
            the left-hand side of the operator, but it can also act on a constant (literal)
            expression. The right-hand side of the operator represents the pattern.""",

        // we use an inline example here because ?pattern not supported in csv-spec test
        detailedDescription = """
            Matching special characters (eg. `.`, `*`, `(`...) will require escaping.
            The escape character is backslash `\\`. Since also backslash is a special character in string literals,
            it will require further escaping.

            <<load-esql-example, file=string tag=rlikeEscapingSingleQuotes>>

            To reduce the overhead of escaping, we suggest using triple quotes strings `\"\"\"`

            <<load-esql-example, file=string tag=rlikeEscapingTripleQuotes>>
            ```{applies_to}
            stack: ga 9.2
            serverless: ga
            ```

            Both a single pattern or a list of patterns are supported. If a list of patterns is provided,
            the expression will return true if any of the patterns match.

            <<load-esql-example, file=where-like tag=rlikeListDocExample>>

            Patterns may be specified with REST query placeholders as well

            ```esql
            FROM employees
            | WHERE first_name RLIKE ?pattern
            | KEEP first_name, last_name
            ```

            ```{applies_to}
            stack: ga 9.3
            ```
            """,
        operator = NAME,
        examples = @Example(file = "docs", tag = "rlike")
    )
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

    /**
     * Pushes down string casing optimization for a single pattern using the provided predicate.
     * Returns a new RLike with case insensitivity or a Literal.FALSE if not matched.
     */
    @Override
    public Expression optimizeStringCasingWithInsensitiveRegexMatch(Expression unwrappedField, Predicate<String> matchesCaseFn) {
        if (matchesCaseFn.test(pattern().pattern()) == false) {
            return Literal.of(this, Boolean.FALSE);
        }
        return new RLike(source(), unwrappedField, pattern(), true);
    }
}
