/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesRegexpQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesWildcardQuery;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.search.runtime.AbstractStringScriptFieldAutomatonQuery;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * {@link QueryCostEstimator} for the automata Lucene's {@code UnifiedHighlighter} rebuilds while
 * highlighting a query, without materializing the automaton itself. {@code FuzzyQuery} is charged
 * via {@link FuzzyQueries#estimateAutomataBytes}; leaves with an already-built cached automaton
 * (see {@link #hasCachedAutomaton}) are charged {@link #MATCHER_WRAPPER_BYTES}; everything else is
 * charged the {@link #REBUILT_AUTOMATON_FLOOR_BYTES} floor.
 * <p>
 * {@code fieldMatcher} must match the predicate for a single {@code UnifiedHighlighter} extraction
 * pass. With {@code matched_fields}, {@code UnifiedHighlighter} runs one pass per masked field plus
 * one for the original field; callers must invoke this estimator once per pass and sum the
 * results, rather than passing one matcher for the field union.
 */
public final class HighlightAutomatonCostEstimator implements QueryCostEstimator {

    /** Floor charged per rebuilt-automaton leaf that isn't a {@link FuzzyQuery}. */
    public static final long REBUILT_AUTOMATON_FLOOR_BYTES = AutomatonQueryCostEstimator.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES;

    /** Bytes charged per cached-automaton leaf for the retained wrapper and its label. */
    public static final long MATCHER_WRAPPER_BYTES = QueryCostEstimator.LEAF_FLOOR_BYTES;

    private final Query query;
    private final Predicate<String> fieldMatcher;
    private final boolean weightMatchesEffective;

    /**
     * @param query the highlight query, pre-rewrite (the same query passed to {@code CustomUnifiedHighlighter}).
     * @param fieldMatcher the field matcher for a single extraction pass; see the class javadoc.
     * @param weightMatchesEffective whether the highlighter runs with {@code HighlightFlag#WEIGHT_MATCHES}
     *                      enabled for {@code query}; see {@code CustomUnifiedHighlighter#isWeightMatchesEffective}.
     */
    public HighlightAutomatonCostEstimator(Query query, Predicate<String> fieldMatcher, boolean weightMatchesEffective) {
        this.query = Objects.requireNonNull(query, "query");
        this.fieldMatcher = Objects.requireNonNull(fieldMatcher, "fieldMatcher");
        this.weightMatchesEffective = weightMatchesEffective;
    }

    /**
     * Returns a ceiling on the bytes the highlighter's automata extraction will retain for the
     * fields accepted by {@code fieldMatcher}, with saturation on overflow.
     */
    @Override
    public long estimate() {
        if (weightMatchesEffective && hasUnrecognizedLeaf(query, fieldMatcher)) {
            return 0L;
        }
        long[] bytes = new long[1];
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                return fieldMatcher.test(field);
            }

            @Override
            public void consumeTermsMatching(Query leaf, String field, Supplier<ByteRunAutomaton> automaton) {
                if (bytes[0] == Long.MAX_VALUE) {
                    return;
                }
                long addition = leaf instanceof FuzzyQuery fuzzyQuery ? FuzzyQueries.estimateAutomataBytes(fuzzyQuery)
                    : hasCachedAutomaton(leaf) ? MATCHER_WRAPPER_BYTES
                    : REBUILT_AUTOMATON_FLOOR_BYTES;
                try {
                    bytes[0] = Math.addExact(bytes[0], addition);
                } catch (ArithmeticException e) {
                    bytes[0] = Long.MAX_VALUE;
                }
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                if (weightMatchesEffective == false && parent instanceof SpanQuery) {
                    return QueryVisitor.EMPTY_VISITOR;
                }
                return super.getSubVisitor(occur, parent);
            }
        });
        return bytes[0];
    }

    /** {@code true} if {@code leaf} supplies an automaton already built and cached in a field, rather than rebuilt per call. */
    private static boolean hasCachedAutomaton(Query leaf) {
        return leaf instanceof Accountable
            || leaf instanceof AbstractStringScriptFieldAutomatonQuery
            || leaf instanceof ScanningBinaryDocValuesWildcardQuery
            || leaf instanceof ScanningBinaryDocValuesRegexpQuery;
    }

    /**
     * Mirrors {@code UnifiedHighlighter#hasUnrecognizedQuery}: {@code true} if {@code query} has a
     * leaf, on a field {@code fieldMatcher} accepts, that {@code MultiTermHighlighting} can't build
     * an automaton from.
     */
    private static boolean hasUnrecognizedLeaf(Query query, Predicate<String> fieldMatcher) {
        boolean[] hasUnrecognized = new boolean[1];
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                return hasUnrecognized[0] == false && fieldMatcher.test(field);
            }

            @Override
            public void visitLeaf(Query leaf) {
                boolean recognized = leaf instanceof FuzzyQuery
                    || leaf instanceof AutomatonQuery
                    || leaf instanceof MatchAllDocsQuery
                    || leaf instanceof MatchNoDocsQuery;
                if (recognized == false) {
                    hasUnrecognized[0] = true;
                }
            }
        });
        return hasUnrecognized[0];
    }
}
