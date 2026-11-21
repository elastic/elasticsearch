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
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingOperations;
import org.elasticsearch.xpack.esql.capabilities.TransportVersionAware;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
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

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Whether a string matches a regular expression in RE2 syntax as a whole, which is what a PromQL label matcher asks.
 * It is not part of the ES|QL language, whose {@code RLIKE} takes Lucene's regexp syntax instead.
 */
public class Re2Like extends RegexMatch<Re2Pattern> implements AnyNullIsNull, TransportVersionAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Re2Like", Re2Like::new);
    public static final String NAME = "RE2_LIKE";

    private static final TransportVersion ESQL_RE2_LIKE = TransportVersion.fromName("esql_re2_like");

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
        // the checks a regexp query makes on the shard, which the RE2 automaton bypasses
        int maxRegexLength = context.getIndexSettings().getMaxRegexLength();
        if (pattern().pattern().length() > maxRegexLength) {
            throw new IllegalArgumentException(
                "The length of regex ["
                    + pattern().pattern().length()
                    + "] used in a label matcher has exceeded the allowed maximum of ["
                    + maxRegexLength
                    + "]. This maximum can be set by changing the ["
                    + IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey()
                    + "] index level setting."
            );
        }
        if (fieldType.indexType().hasTerms() && context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[regexp] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        // accounted on the breaker of the shard, as a regexp query's automaton is
        CircuitBreaker circuitBreaker = context.getCircuitBreaker();
        Automaton automaton = circuitBreaker == null
            ? pattern().createAutomaton(caseInsensitive())
            : pattern().createAutomaton(caseInsensitive(), circuitBreaker, NAME + ":" + fieldType.name());
        return fieldType.automatonQuery(
            () -> automaton,
            () -> new CharacterRunAutomaton(automaton),
            constantScoreRewrite,
            context,
            NAME + "(\"" + pattern().pattern() + "\")"
        );
    }

    /**
     * A node that predates this expression can't read it, so while there is one the pattern is matched as {@link RLike}
     * matches it, in Lucene's regexp syntax, as PromQL label matchers were matched before. That is only done when both
     * syntaxes give the pattern the same strings, as they do for the usual {@code a|b} or {@code prefix.*}; any other
     * pattern is refused rather than matched in a way the query didn't ask for.
     */
    @Override
    public Expression forTransportVersion(TransportVersion minTransportVersion) {
        if (minTransportVersion.supports(ESQL_RE2_LIKE) == false) {
            RLikePattern lucene = new RLikePattern(pattern().pattern());
            if (sameLanguage(pattern(), lucene) == false) {
                throw new IllegalArgumentException(
                    "regex ["
                        + pattern().pattern()
                        + "] can't be matched until every node is upgraded: nodes on older versions read it as a Lucene regexp,"
                        + " which matches different strings"
                );
            }
            return new RLike(source(), field(), lucene);
        }
        return this;
    }

    /**
     * Whether both syntaxes give the pattern the same strings. The automata and the differences between them are built
     * within a fold budget, as a label matcher's automaton is, so that the check is bounded on the coordinator; a pattern
     * past it is taken to differ.
     */
    private boolean sameLanguage(Re2Pattern re2, RLikePattern lucene) {
        CircuitBreaker breaker = FoldContext.small().circuitBreakerView(source());
        try {
            Automaton a = re2.createAutomaton(false, breaker, NAME);
            Automaton b = lucene.createAutomaton(false);
            return Operations.isEmpty(CircuitBreakingOperations.minus(a, b, breaker, NAME))
                && Operations.isEmpty(CircuitBreakingOperations.minus(b, a, breaker, NAME));
        } catch (IllegalArgumentException | TooComplexToDeterminizeException | FoldContext.FoldTooMuchMemoryException e) {
            return false;
        }
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
