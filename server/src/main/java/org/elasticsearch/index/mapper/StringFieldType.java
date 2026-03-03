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
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.CaseInsensitivePrefixQuery;
import org.elasticsearch.common.lucene.search.CaseInsensitiveWildcardQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;

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
 * <p>Circuit breaker accounting for automaton-based queries (prefix, wildcard, regexp, range)
 * is performed here, at the point each individual Lucene query is created, rather than solely
 * in {@link SearchExecutionContext#toQuery} after the full query tree is assembled. This is
 * intentional: compound queries such as a {@code bool} with many wildcard clauses build each
 * clause sequentially, so by the time the complete tree is available for a post-hoc walk every
 * automaton has already been allocated. Accounting per-clause lets the circuit breaker trip as
 * soon as cumulative memory crosses the threshold, preventing the remaining clauses from being
 * constructed and avoiding a potential OOM.
 */
public abstract class StringFieldType extends TermBasedFieldType {

    private static final Pattern WILDCARD_PATTERN = Pattern.compile("(\\\\.)|([?*]+)");

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
        return rewriteMethod == null
            ? new FuzzyQuery(
                new Term(name(), indexedValueForSearch(value)),
                fuzziness.asDistance(BytesRefs.toString(value)),
                prefixLength,
                maxExpansions,
                transpositions
            )
            : new FuzzyQuery(
                new Term(name(), indexedValueForSearch(value)),
                fuzziness.asDistance(BytesRefs.toString(value)),
                prefixLength,
                maxExpansions,
                transpositions,
                rewriteMethod
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
        context.addCircuitBreakerMemory(query.ramBytesUsed(), "prefix:" + name());
        return query;
    }

    public static final String normalizeWildcardPattern(String fieldname, String value, Analyzer normalizer) {
        if (normalizer == null) {
            return value;
        }
        // we want to normalize everything except wildcard characters, e.g. F?o Ba* to f?o ba*, even if e.g there
        // is a char_filter that would otherwise remove them
        Matcher wildcardMatcher = WILDCARD_PATTERN.matcher(value);
        BytesRefBuilder sb = new BytesRefBuilder();
        int last = 0;

        while (wildcardMatcher.find()) {
            if (wildcardMatcher.start() > 0) {
                String chunk = value.substring(last, wildcardMatcher.start());

                BytesRef normalized = normalizer.normalize(fieldname, chunk);
                sb.append(normalized);
            }
            // append the matched group - without normalizing
            sb.append(new BytesRef(wildcardMatcher.group()));

            last = wildcardMatcher.end();
        }
        if (last < value.length()) {
            String chunk = value.substring(last);
            BytesRef normalized = normalizer.normalize(fieldname, chunk);
            sb.append(normalized);
        }
        return sb.toBytesRef().utf8ToString();
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
        AutomatonQuery query;
        if (caseInsensitive) {
            query = method == null ? new CaseInsensitiveWildcardQuery(term) : new CaseInsensitiveWildcardQuery(term, false, method);
        } else {
            query = method == null ? new WildcardQuery(term) : new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, method);
        }
        context.addCircuitBreakerMemory(query.ramBytesUsed(), "wildcard:" + name());
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
        AutomatonQuery query = method == null
            ? new RegexpQuery(new Term(name(), indexedValueForSearch(value)), syntaxFlags, matchFlags, maxDeterminizedStates)
            : new RegexpQuery(
                new Term(name(), indexedValueForSearch(value)),
                syntaxFlags,
                matchFlags,
                RegexpQuery.DEFAULT_PROVIDER,
                maxDeterminizedStates,
                method
            );
        context.addCircuitBreakerMemory(query.ramBytesUsed(), "regexp:" + name());
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
        context.addCircuitBreakerMemory(query.ramBytesUsed(), "range:" + name());
        return query;
    }
}
