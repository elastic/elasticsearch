/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.lucene.search.CaseInsensitivePrefixQuery;
import org.elasticsearch.common.lucene.search.CaseInsensitiveWildcardQuery;
import org.elasticsearch.common.lucene.search.SharedAutomaton;
import org.elasticsearch.common.lucene.search.SharedAutomatonQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AutomatonKey;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.search.FuzzyQueries;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Base class for {@link MappedFieldType} implementations that use the same
 * representation for internal index terms as the external representation so
 * that partial matching queries such as prefix, wildcard and fuzzy queries
 * can be implemented.
 *
 * <p>Circuit breaker accounting for automaton-based queries happens in two phases:
 * <ul>
 *   <li><b>Pre-flight reservation:</b> {@link SharedAutomaton#compile} holds an estimate of the
 *   {@code CompiledAutomaton} construction peak on the breaker across the build. This guards the
 *   construction window itself, which is invisible to any post-hoc walk of the assembled tree.</li>
 *   <li><b>Retained-size charge (once per automaton):</b>
 *   {@link SearchExecutionContext#computeAutomatonIfAbsent} charges the automaton where it is built and
 *   reuses it for every later clause of the request resolving to the same pattern, so a pattern expanded
 *   over many fields is compiled and charged once. Those clauses are marked pre-charged, so the
 *   {@code MaxClauseCountQueryVisitor} walk in
 *   {@link org.elasticsearch.index.query.AbstractQueryBuilder#toQuery(SearchExecutionContext)} skips them
 *   and charges only the leaves nobody accounted for at construction time.</li>
 * </ul>
 */
public abstract class StringFieldType extends TermBasedFieldType {

    // DOTALL so an escape (\X) is recognised even when X is a line terminator, matching Lucene which escapes any code point.
    private static final Pattern WILDCARD_PATTERN = Pattern.compile("(\\\\.)|([?*]+)", Pattern.DOTALL);

    public StringFieldType(String name, IndexType indexType, boolean isStored, TextSearchInfo textSearchInfo, Map<String, String> meta) {
        super(name, indexType, isStored, textSearchInfo, meta);
    }

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        SearchExecutionContext context,
        @Nullable MultiTermQuery.RewriteMethod rewriteMethod
    ) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[fuzzy] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        failIfNotIndexed();
        return FuzzyQueries.create(
            new Term(name(), indexedValueForSearch(value)),
            fuzziness.asDistance(BytesRefs.toString(value)),
            prefixLength,
            maxExpansions,
            transpositions,
            rewriteMethod,
            context,
            name()
        );
    }

    @Override
    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, SearchExecutionContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[prefix] queries cannot be executed when '"
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "' is set to false. For optimised prefix queries on text "
                    + "fields please enable [index_prefixes]."
            );
        }
        failIfNotIndexed();
        Term prefix = new Term(name(), indexedValueForSearch(value));
        AutomatonQuery query;
        if (caseInsensitive) {
            query = method == null ? new CaseInsensitivePrefixQuery(prefix, false) : new CaseInsensitivePrefixQuery(prefix, false, method);
        } else {
            query = method == null ? new PrefixQuery(prefix) : new PrefixQuery(prefix, method);
        }
        return query;
    }

    public static final String normalizeWildcardPattern(String fieldname, String value, Analyzer normalizer) {
        if (normalizer == null) {
            return value;
        }
        // Normalize the literal parts of the pattern but keep the ? and * operators, e.g. F?o Ba* to f?o ba*. Escapes
        // (\X) are literal data, so we gather each contiguous literal run (across plain text and escapes) and normalize
        // it as a whole; context-sensitive normalizers need the full run. Operators the normalizer emits are re-escaped.
        Matcher wildcardMatcher = WILDCARD_PATTERN.matcher(value);
        BytesRefBuilder sb = new BytesRefBuilder();
        StringBuilder literal = new StringBuilder();
        int last = 0;

        while (wildcardMatcher.find()) {
            if (wildcardMatcher.start() > last) {
                literal.append(value, last, wildcardMatcher.start());
            }
            String escape = wildcardMatcher.group(1);
            if (escape != null) {
                // \X is an escape: the escaped character is literal data, so drop the backslash and keep X
                literal.append(escape, 1, escape.length());
            } else {
                // operators: flush the accumulated literal run, then keep them verbatim
                appendNormalizedLiteral(sb, normalizer, fieldname, literal.toString());
                literal.setLength(0);
                sb.append(new BytesRef(wildcardMatcher.group()));
            }
            last = wildcardMatcher.end();
        }
        if (last < value.length()) {
            literal.append(value, last, value.length());
        }
        appendNormalizedLiteral(sb, normalizer, fieldname, literal.toString());
        return sb.toBytesRef().utf8ToString();
    }

    /** Normalizes one literal run and appends it, re-escaping any {@code *}, {@code ?}, or backslash the normalizer produced. */
    private static void appendNormalizedLiteral(BytesRefBuilder sb, Analyzer normalizer, String fieldname, String chunk) {
        if (chunk.isEmpty()) {
            return;
        }
        BytesRef normalized = normalizer.normalize(fieldname, chunk);
        // The operators are ASCII and UTF-8 never uses bytes below 0x80 inside a multi-byte sequence, so scanning the
        // raw bytes is safe. In the common case the normalizer emits no operator and the bytes are appended as-is.
        int operators = 0;
        for (int i = 0; i < normalized.length; i++) {
            byte b = normalized.bytes[normalized.offset + i];
            if (b == '*' || b == '?' || b == '\\') {
                operators++;
            }
        }
        if (operators == 0) {
            sb.append(normalized);
            return;
        }
        sb.grow(sb.length() + normalized.length + operators);
        for (int i = 0; i < normalized.length; i++) {
            byte b = normalized.bytes[normalized.offset + i];
            if (b == '*' || b == '?' || b == '\\') {
                sb.append((byte) '\\');
            }
            sb.append(b);
        }
    }

    @Override
    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, SearchExecutionContext context) {
        return wildcardQuery(value, method, caseInsensitive, false, context);
    }

    @Override
    public Query normalizedWildcardQuery(String value, MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
        return wildcardQuery(value, method, false, true, context);
    }

    protected Query wildcardQuery(
        String value,
        MultiTermQuery.RewriteMethod method,
        boolean caseInsensitive,
        boolean shouldNormalize,
        SearchExecutionContext context
    ) {
        failIfNotIndexed();
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[wildcard] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        Term term;
        if (getTextSearchInfo().searchAnalyzer() != null && shouldNormalize) {
            value = normalizeWildcardPattern(name(), value, getTextSearchInfo().searchAnalyzer());
            term = new Term(name(), value);
        } else {
            term = new Term(name(), indexedValueForSearch(value));
        }

        CircuitBreaker circuitBreaker = context.getCircuitBreaker();
        if (circuitBreaker != null) {
            return sharedAutomatonQuery(
                term,
                new AutomatonKey.Wildcard(term.text(), caseInsensitive),
                () -> caseInsensitive
                    ? AutomatonQueries.toCaseInsensitiveWildcardAutomaton(term, circuitBreaker)
                    : AutomatonQueries.toWildcardAutomaton(term, circuitBreaker),
                // CaseInsensitiveWildcardQuery prints the requested field rather than its own, so keep the two apart.
                caseInsensitive
                    ? f -> "CaseInsensitiveWildcardQuery{" + f + ":" + term.text() + "}"
                    : SharedAutomatonQuery.fieldPrefixed(term, term.text()),
                method,
                context
            );
        }

        AutomatonQuery query;
        if (caseInsensitive) {
            query = method == null ? new CaseInsensitiveWildcardQuery(term) : new CaseInsensitiveWildcardQuery(term, false, method);
        } else {
            query = method == null ? new WildcardQuery(term) : new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, method);
        }
        return query;
    }

    /**
     * Builds a query over the automaton {@code key} identifies, reusing it if another clause of this request already
     * built the same one. The automaton is charged where it is built and this clause charges only what it adds on top,
     * so the query is marked pre-charged and the retained-size walk in
     * {@link org.elasticsearch.index.query.AbstractQueryBuilder#toQuery} does not count the automaton again.
     */
    protected static Query sharedAutomatonQuery(
        Term term,
        AutomatonKey key,
        Supplier<Automaton> dfa,
        Function<String, String> description,
        MultiTermQuery.RewriteMethod method,
        SearchExecutionContext context
    ) {
        SharedAutomaton shared = context.computeAutomatonIfAbsent(
            key,
            () -> SharedAutomaton.compile(dfa.get(), context.getCircuitBreaker(), key.category())
        );
        SharedAutomatonQuery query = new SharedAutomatonQuery(
            term,
            shared,
            description,
            method == null ? MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE : method
        );
        context.addCircuitBreakerMemory(query.unsharedRamBytesUsed(), key.category());
        context.markQueryMemoryPreCharged(query);
        return query;
    }

    /**
     * Wildcard query for a field answered from doc values rather than a terms dictionary. Same automaton as the
     * indexed form, so a pattern spanning both kinds of field still compiles and charges one.
     */
    protected static Query docValuesWildcardQuery(Term term, SearchExecutionContext context) {
        return sharedAutomatonQuery(
            term,
            new AutomatonKey.Wildcard(term.text(), false),
            () -> AutomatonQueries.toWildcardAutomaton(term, context.getCircuitBreaker()),
            SharedAutomatonQuery.fieldPrefixed(term, term.text()),
            MultiTermQuery.DOC_VALUES_REWRITE,
            context
        );
    }

    /** Regexp counterpart of {@link #docValuesWildcardQuery}. */
    protected static Query docValuesRegexpQuery(
        Term term,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        SearchExecutionContext context
    ) {
        return sharedAutomatonQuery(
            term,
            new AutomatonKey.Regexp(term.text(), syntaxFlags, matchFlags, maxDeterminizedStates),
            () -> AutomatonQueries.toRegexpAutomaton(term, syntaxFlags, matchFlags, maxDeterminizedStates, context.getCircuitBreaker()),
            SharedAutomatonQuery.fieldPrefixed(term, "/" + term.text() + "/"),
            MultiTermQuery.DOC_VALUES_REWRITE,
            context
        );
    }

    @Override
    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        MultiTermQuery.RewriteMethod method,
        SearchExecutionContext context
    ) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[regexp] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        failIfNotIndexed();

        value = AutomatonQueries.collapseConsecutiveQuantifiers(value);
        Term term = new Term(name(), indexedValueForSearch(value));
        CircuitBreaker circuitBreaker = context.getCircuitBreaker();
        if (circuitBreaker != null) {
            return sharedAutomatonQuery(
                term,
                new AutomatonKey.Regexp(term.text(), syntaxFlags, matchFlags, maxDeterminizedStates),
                () -> AutomatonQueries.toRegexpAutomaton(term, syntaxFlags, matchFlags, maxDeterminizedStates, circuitBreaker),
                SharedAutomatonQuery.fieldPrefixed(term, "/" + term.text() + "/"),
                method,
                context
            );
        }

        return method == null
            ? new RegexpQuery(new Term(name(), indexedValueForSearch(value)), syntaxFlags, matchFlags, maxDeterminizedStates)
            : new RegexpQuery(term, syntaxFlags, matchFlags, RegexpQuery.DEFAULT_PROVIDER, maxDeterminizedStates, method);
    }

    @Override
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        SearchExecutionContext context
    ) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[range] queries on [text] or [keyword] fields cannot be executed when '"
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "' is set to false."
            );
        }
        failIfNotIndexed();
        AutomatonQuery query = new TermRangeQuery(
            name(),
            lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
            upperTerm == null ? null : indexedValueForSearch(upperTerm),
            includeLower,
            includeUpper
        );
        return query;
    }
}
