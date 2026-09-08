/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.columnar.ColumnarStringAutomatonQuery;
import org.elasticsearch.columnar.ColumnarStringMatchQuery;
import org.elasticsearch.columnar.ColumnarStringTermQuery;
import org.elasticsearch.columnar.ScanBudget;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Queries answered by the ColumNAR string column itself rather than by reading a blob for every document.
 *
 * <p>What each costs is the column's own business and varies by what it is: a term or a prefix over a column whose
 * values arrive in term order bisects to the run of matches, and a dictionary column runs a test over its terms and
 * then answers documents by ordinal. What is shared is that a test is paid once per distinct value rather than once
 * per document wherever the column can arrange it.
 *
 * <p>Automata are built here rather than in the column library, which has no dependency on the mapper's automaton
 * helpers and no business deciding what a pattern means. The library runs whatever it is handed.
 */
final class ColumnarBinaryDocValuesQueries implements BinaryDocValuesQueries {

    static final ColumnarBinaryDocValuesQueries INSTANCE = new ColumnarBinaryDocValuesQueries();

    /** The column library keeps no notion of a heap budget, so it is handed the search layer's. */
    private static final ScanBudget BUDGET = ContextIndexSearcher::checkBinaryDvDecodeBreaker;

    private ColumnarBinaryDocValuesQueries() {}

    @Override
    public Query term(String field, BytesRef term) {
        return ColumnarStringTermQuery.term(field, term, BUDGET);
    }

    @Override
    public Query terms(String field, Collection<BytesRef> terms) {
        final Set<BytesRef> set = new HashSet<>(terms);
        // What the query is compared by, so two of them cache as one however the caller ordered its terms. The terms
        // themselves rather than a rendering of them: there may be tens of thousands.
        return new ColumnarStringMatchQuery(field, set::contains, new TreeSet<>(terms), BUDGET);
    }

    @Override
    public Query range(String field, @Nullable BytesRef lower, @Nullable BytesRef upper, boolean includeLower, boolean includeUpper) {
        final BytesRef low = lower == null ? null : BytesRef.deepCopyOf(lower);
        final BytesRef high = upper == null ? null : BytesRef.deepCopyOf(upper);
        final String identity = "range=" + (includeLower ? "[" : "{") + low + "," + high + (includeUpper ? "]" : "}");
        return new ColumnarStringMatchQuery(field, value -> {
            if (low != null) {
                final int cmp = value.compareTo(low);
                if (cmp < 0 || (cmp == 0 && includeLower == false)) {
                    return false;
                }
            }
            if (high != null) {
                final int cmp = value.compareTo(high);
                return cmp < 0 || (cmp == 0 && includeUpper);
            }
            return true;
        }, identity, BUDGET);
    }

    @Override
    public Query prefix(String field, String value, boolean caseInsensitive) {
        if (caseInsensitive == false) {
            // The column bisects a prefix, so this never becomes an automaton.
            return ColumnarStringTermQuery.prefix(field, new BytesRef(value), BUDGET);
        }
        return new ColumnarStringAutomatonQuery(
            field,
            AutomatonQueries.caseInsensitivePrefix(value),
            "caseInsensitivePrefix=" + value,
            BUDGET
        );
    }

    @Override
    public Query fuzzy(String field, String term, int maxEdits, int prefixLength, boolean transpositions) {
        // The compiled automaton comes from the query that defines the edit distance, as the scanning path also does.
        final FuzzyQuery delegate = FuzzyQueries.create(
            new Term(field, term),
            maxEdits,
            prefixLength,
            1,
            transpositions,
            null,
            null,
            field
        );
        return new ColumnarStringAutomatonQuery(
            field,
            delegate.getAutomata().runAutomaton,
            "fuzzy,term=" + term + ",maxEdits=" + maxEdits + ",prefixLength=" + prefixLength + ",transpositions=" + transpositions,
            BUDGET
        );
    }

    @Override
    public Query caseInsensitiveTerm(String field, String value) {
        return new ColumnarStringAutomatonQuery(field, Automata.makeCaseInsensitiveString(value), "caseInsensitiveTerm=" + value, BUDGET);
    }

    @Override
    public Query wildcard(String field, String pattern, boolean caseInsensitive) {
        if (caseInsensitive == false) {
            // Rewrites a pattern naming a term, a prefix or a contained run into the query that answers it directly.
            return ColumnarStringAutomatonQuery.forWildcard(field, pattern, BUDGET);
        }
        return new ColumnarStringAutomatonQuery(
            field,
            AutomatonQueries.toCaseInsensitiveWildcardAutomaton(new Term(field, pattern)),
            "pattern=" + pattern + ",caseInsensitive=true",
            BUDGET
        );
    }

    @Override
    public Query automaton(String field, Automaton automaton, String description) {
        return new ColumnarStringAutomatonQuery(field, automaton, description, BUDGET);
    }

    @Override
    public Query regexp(
        String field,
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        @Nullable CircuitBreaker breaker
    ) {
        return new ColumnarStringAutomatonQuery(
            field,
            // The byte-run form, as the scanning path uses: it is the one that tolerates a caller with no breaker.
            AutomatonQueries.toRegexpByteRunAutomaton(field, pattern, syntaxFlags, matchFlags, maxDeterminizedStates, breaker),
            "regexp=" + pattern + ",flags=" + syntaxFlags + "," + matchFlags,
            BUDGET
        );
    }
}
