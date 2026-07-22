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
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.lucene.search.CaseInsensitivePrefixQuery;
import org.elasticsearch.common.lucene.search.CaseInsensitiveWildcardQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AutomatonQueryWithDescription;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.lucene.search.cost.AutomatonQueryCostEstimator;

import java.util.Map;
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
 *   <li><b>Pre-flight reservation:</b> wildcard and regexp queries reserve an estimate of
 *   the {@code CompiledAutomaton} construction peak on the breaker before calling the
 *   {@link AutomatonQuery} constructor, and refund it once construction returns. This guards the
 *   construction window itself, which is invisible to any post-hoc walk of the assembled tree.
 *   A reservation left behind by a failed construction is refunded at request end.</li>
 *   <li><b>Retained-size charge (once per phase):</b>
 *   {@link org.elasticsearch.index.query.AbstractQueryBuilder#toQuery(SearchExecutionContext)}
 *   walks the produced tree with {@code MaxClauseCountQueryVisitor} and charges the sum of
 *   {@code ramBytesUsed()} for every {@code Accountable} leaf in a single breaker call, peeking
 *   mid-walk so pathological fan-outs trip before the full tree is materialised.</li>
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
        AutomatonQuery query;
        long reservation = 0;
        if (circuitBreaker != null) {
            Automaton dfa = caseInsensitive
                ? AutomatonQueries.toCaseInsensitiveWildcardAutomaton(term, circuitBreaker)
                : AutomatonQueries.toWildcardAutomaton(term, circuitBreaker);
            reservation = new AutomatonQueryCostEstimator(dfa.ramBytesUsed()).estimate();
            context.addCircuitBreakerMemory(reservation, ChildMemoryCircuitBreaker.CATEGORY_WILDCARD);

            if (caseInsensitive) {
                query = method == null
                    ? new CaseInsensitiveWildcardQuery(term, dfa)
                    : new CaseInsensitiveWildcardQuery(term, dfa, false, method);
            } else {
                query = method == null
                    ? new AutomatonQueryWithDescription(term, dfa, term.text())
                    : new AutomatonQuery(term, dfa, false, method);
            }
            context.addCircuitBreakerMemory(0L, reservation, ChildMemoryCircuitBreaker.CATEGORY_WILDCARD);
        } else {
            if (caseInsensitive) {
                query = method == null ? new CaseInsensitiveWildcardQuery(term) : new CaseInsensitiveWildcardQuery(term, false, method);
            } else {
                query = method == null
                    ? new WildcardQuery(term)
                    : new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, method);
            }
        }
        return query;
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
        AutomatonQuery query;
        Term term = new Term(name(), indexedValueForSearch(value));
        CircuitBreaker circuitBreaker = context.getCircuitBreaker();
        long reservation = 0;
        if (circuitBreaker != null) {
            Automaton dfa = AutomatonQueries.toRegexpAutomaton(term, syntaxFlags, matchFlags, maxDeterminizedStates, circuitBreaker);
            reservation = new AutomatonQueryCostEstimator(dfa.ramBytesUsed()).estimate();
            context.addCircuitBreakerMemory(reservation, ChildMemoryCircuitBreaker.CATEGORY_REGEXP);
            query = method == null
                ? new AutomatonQueryWithDescription(term, dfa, "/" + term.text() + "/")
                : new AutomatonQuery(term, dfa, false, method);
            // Construction succeeded; refund the pre-flight reservation. The retained
            // ramBytesUsed() of the produced query is charged once per phase by the
            // visitor walk in AbstractQueryBuilder#toQuery.
            context.addCircuitBreakerMemory(0L, reservation, ChildMemoryCircuitBreaker.CATEGORY_REGEXP);
        } else {
            query = method == null
                ? new RegexpQuery(new Term(name(), indexedValueForSearch(value)), syntaxFlags, matchFlags, maxDeterminizedStates)
                : new RegexpQuery(term, syntaxFlags, matchFlags, RegexpQuery.DEFAULT_PROVIDER, maxDeterminizedStates, method);
        }
        return query;
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
