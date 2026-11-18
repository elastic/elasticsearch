/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string.regex;

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.ExpressionQuery;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Whether a string matches a regular expression in RE2 syntax as a whole, which is what a PromQL label matcher asks.
 * It is not part of the ES|QL language, whose {@code RLIKE} takes Lucene's regexp syntax instead.
 */
public class Re2Like extends RegexMatch<Re2Pattern> implements AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Re2Like", Re2Like::new);
    public static final String NAME = "RE2_LIKE";

    public Re2Like(Source source, Expression field, Re2Pattern pattern) {
        super(source, field, pattern, false);
    }

    private Re2Like(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), new Re2Pattern(in.readString()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeString(pattern().pattern());
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
    protected NodeInfo<Re2Like> info() {
        return NodeInfo.create(this, Re2Like::new, field(), pattern());
    }

    @Override
    protected Re2Like replaceChild(Expression newChild) {
        return new Re2Like(source(), newChild, pattern());
    }

    /**
     * A {@code regexp} query parses Lucene's syntax, so the expression goes to the shard and builds its automaton there.
     */
    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fa = LucenePushdownPredicates.checkIsFieldAttribute(field());
        return new ExpressionQuery(source(), handler.nameOf(fa.exactAttribute()), this);
    }

    @Override
    public org.apache.lucene.search.Query asLuceneQuery(
        MappedFieldType fieldType,
        MultiTermQuery.RewriteMethod constantScoreRewrite,
        SearchExecutionContext context
    ) {
        Automaton automaton = pattern().createAutomaton(caseInsensitive());
        return fieldType.automatonQuery(
            () -> automaton,
            () -> new CharacterRunAutomaton(automaton),
            constantScoreRewrite,
            context,
            NAME + "(\"" + pattern().pattern() + "\")"
        );
    }

    /**
     * The case check reads the pattern's text, which in RE2 can spell letters as escapes like {@code \x41} or change
     * their case with {@code (?i)}, so the match stays as it is.
     */
    @Override
    public Expression optimizeStringCasingWithInsensitiveRegexMatch(Expression unwrappedField, Predicate<String> matchesCaseFn) {
        return this;
    }
}
