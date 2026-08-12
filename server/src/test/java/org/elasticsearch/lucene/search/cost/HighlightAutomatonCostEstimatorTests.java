/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesWildcardQuery;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.runtime.StringScriptFieldWildcardQuery;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class HighlightAutomatonCostEstimatorTests extends ESTestCase {

    private static final Predicate<String> ACCEPT_ALL_FIELDS = field -> true;

    public void testConstructorRejectsNullArguments() {
        Query query = new TermQuery(new Term("field", "value"));
        expectThrows(NullPointerException.class, () -> new HighlightAutomatonCostEstimator(null, ACCEPT_ALL_FIELDS, true));
        expectThrows(NullPointerException.class, () -> new HighlightAutomatonCostEstimator(query, null, true));
    }

    public void testTermQueryChargesNothing() {
        Query query = new TermQuery(new Term("field", "value"));
        assertEquals(0L, new HighlightAutomatonCostEstimator(query, ACCEPT_ALL_FIELDS, true).estimate());
    }

    public void testAutomatonQueryChargesOnlyMatcherWrapper() {
        WildcardQuery query = new WildcardQuery(new Term("field", "foo*bar"));
        long estimate = new HighlightAutomatonCostEstimator(query, ACCEPT_ALL_FIELDS, true).estimate();
        assertEquals(HighlightAutomatonCostEstimator.MATCHER_WRAPPER_BYTES, estimate);
        assertThat(
            "must not charge the cached automaton's retained RAM, only the retained matcher wrapper",
            estimate,
            lessThan(query.ramBytesUsed())
        );
    }

    public void testScriptFieldAutomatonQueryChargesOnlyMatcherWrapper() {
        StringScriptFieldWildcardQuery query = new StringScriptFieldWildcardQuery(
            new Script("dummy"),
            ctx -> null,
            "field",
            "foo*bar",
            randomBoolean()
        );
        assertEquals(
            "the automaton is built once in the constructor and stored in a field, so the "
                + "highlighter only retains a small wrapper on top of it, not a rebuilt automaton",
            HighlightAutomatonCostEstimator.MATCHER_WRAPPER_BYTES,
            new HighlightAutomatonCostEstimator(query, ACCEPT_ALL_FIELDS, false).estimate()
        );
    }

    public void testBinaryDocValuesAutomatonQueryChargesOnlyMatcherWrapper() {
        ScanningBinaryDocValuesWildcardQuery query = new ScanningBinaryDocValuesWildcardQuery(
            "field",
            "foo*bar",
            randomBoolean(),
            randomBoolean()
        );
        assertEquals(
            "the automaton is built once in the constructor and stored in a field, so the "
                + "highlighter only retains a small wrapper on top of it, not a rebuilt automaton",
            HighlightAutomatonCostEstimator.MATCHER_WRAPPER_BYTES,
            new HighlightAutomatonCostEstimator(query, ACCEPT_ALL_FIELDS, false).estimate()
        );
    }

    public void testFuzzyQueryDelegatesToFuzzyQueryCostEstimator() {
        FuzzyQuery query = new FuzzyQuery(new Term("field", "captain"), 2);
        assertEquals(
            FuzzyQueries.estimateAutomataBytes(query),
            new HighlightAutomatonCostEstimator(query, ACCEPT_ALL_FIELDS, true).estimate()
        );
    }

    public void testFieldMatcherExcludesNonMatchingFields() {
        FuzzyQuery onFieldA = new FuzzyQuery(new Term("field_a", "captain"), 2);
        FuzzyQuery onFieldB = new FuzzyQuery(new Term("field_b", "corsair"), 2);
        Query bq = new BooleanQuery.Builder().add(onFieldA, BooleanClause.Occur.SHOULD).add(onFieldB, BooleanClause.Occur.SHOULD).build();

        long scoped = new HighlightAutomatonCostEstimator(bq, "field_a"::equals, true).estimate();
        assertEquals(
            "must charge only the highlighted field's clause, matching what UnifiedHighlighter actually extracts",
            FuzzyQueries.estimateAutomataBytes(onFieldA),
            scoped
        );

        long unscoped = new HighlightAutomatonCostEstimator(bq, ACCEPT_ALL_FIELDS, true).estimate();
        assertThat("an unscoped matcher must charge both fields' clauses, more than the field-scoped estimate", scoped, lessThan(unscoped));
    }

    public void testFieldMatcherUnionForMatchedFields() {
        FuzzyQuery onHighlighted = new FuzzyQuery(new Term("title", "captain"), 2);
        FuzzyQuery onMatched = new FuzzyQuery(new Term("title.plain", "corsair"), 2);
        FuzzyQuery onUnrelated = new FuzzyQuery(new Term("other_field", "unrelated"), 2);
        Query bq = new BooleanQuery.Builder().add(onHighlighted, BooleanClause.Occur.SHOULD)
            .add(onMatched, BooleanClause.Occur.SHOULD)
            .add(onUnrelated, BooleanClause.Occur.SHOULD)
            .build();

        Set<String> extractionFields = Set.of("title", "title.plain");
        long combined = new HighlightAutomatonCostEstimator(bq, extractionFields::contains, true).estimate();
        long expected = FuzzyQueries.estimateAutomataBytes(onHighlighted) + FuzzyQueries.estimateAutomataBytes(onMatched);
        assertEquals(expected, combined);
    }

    public void testBooleanQueryChargesEachRebuiltLeafOnce() {
        FuzzyQuery f1 = new FuzzyQuery(new Term("field", "aaa"), 1);
        FuzzyQuery f2 = new FuzzyQuery(new Term("field", "bbb"), 2);
        Query bq = new BooleanQuery.Builder().add(f1, BooleanClause.Occur.SHOULD).add(f2, BooleanClause.Occur.SHOULD).build();
        long combined = new HighlightAutomatonCostEstimator(bq, ACCEPT_ALL_FIELDS, true).estimate();
        long expected = FuzzyQueries.estimateAutomataBytes(f1) + FuzzyQueries.estimateAutomataBytes(f2);
        assertEquals(expected, combined);
    }

    public void testMustNotClauseChargesNothing() {
        FuzzyQuery fuzzy = new FuzzyQuery(new Term("field", "captain"), 2);
        Query bq = new BooleanQuery.Builder().add(new TermQuery(new Term("field", "other")), BooleanClause.Occur.MUST)
            .add(fuzzy, BooleanClause.Occur.MUST_NOT)
            .build();
        assertEquals(
            "a fuzzy clause under must_not is never visited by UnifiedHighlighter, so it must not be charged",
            0L,
            new HighlightAutomatonCostEstimator(bq, ACCEPT_ALL_FIELDS, true).estimate()
        );
    }

    public void testSpanQueryChargesNothingWhenLookInSpanIsFalse() {
        SpanQuery span = new SpanMultiTermQueryWrapper<>(new FuzzyQuery(new Term("field", "captain"), 2));
        Query near = new SpanNearQuery(new SpanQuery[] { span, new SpanTermQuery(new Term("field", "corsair")) }, 2, true);
        assertEquals(
            "when weight-matches is not effective, UnifiedHighlighter defers span contents to PhraseHelper "
                + "instead of extracting automata, so the estimate must be 0",
            0L,
            new HighlightAutomatonCostEstimator(near, ACCEPT_ALL_FIELDS, false).estimate()
        );
    }

    public void testSpanQueryChargesWhenLookInSpanIsTrue() {
        SpanQuery span = new SpanMultiTermQueryWrapper<>(new FuzzyQuery(new Term("field", "captain"), 2));
        Query near = new SpanNearQuery(new SpanQuery[] { span, new SpanTermQuery(new Term("field", "corsair")) }, 2, true);
        assertThat(new HighlightAutomatonCostEstimator(near, ACCEPT_ALL_FIELDS, true).estimate(), greaterThanOrEqualTo(1L));
    }

    public void testUnrecognizedLeafOnHighlightedFieldSkipsExtractionWhenWeightMatchesEffective() {
        FuzzyQuery fuzzy = new FuzzyQuery(new Term("field", "captain"), 2);
        Query bq = new BooleanQuery.Builder().add(fuzzy, BooleanClause.Occur.SHOULD)
            .add(new FieldExistsQuery("field"), BooleanClause.Occur.FILTER)
            .build();
        assertEquals(
            "an exists clause on the highlighted field makes UnifiedHighlighter skip automata "
                + "extraction entirely once weight-matches is effective",
            0L,
            new HighlightAutomatonCostEstimator(bq, ACCEPT_ALL_FIELDS, true).estimate()
        );
    }

    public void testUnrecognizedLeafStillChargedWhenWeightMatchesNotEffective() {
        FuzzyQuery fuzzy = new FuzzyQuery(new Term("field", "captain"), 2);
        Query bq = new BooleanQuery.Builder().add(fuzzy, BooleanClause.Occur.SHOULD)
            .add(new FieldExistsQuery("field"), BooleanClause.Occur.FILTER)
            .build();
        assertEquals(
            "without weight-matches, UnifiedHighlighter always extracts automata regardless of unrecognized leaves",
            FuzzyQueries.estimateAutomataBytes(fuzzy),
            new HighlightAutomatonCostEstimator(bq, ACCEPT_ALL_FIELDS, false).estimate()
        );
    }

    public void testUnrecognizedLeafOnUnrelatedFieldDoesNotSuppressExtraction() {
        FuzzyQuery fuzzy = new FuzzyQuery(new Term("field", "captain"), 2);
        Query bq = new BooleanQuery.Builder().add(fuzzy, BooleanClause.Occur.SHOULD)
            .add(new FieldExistsQuery("other_field"), BooleanClause.Occur.FILTER)
            .build();
        assertEquals(
            "an exists clause on a field the fieldMatcher doesn't accept must not suppress extraction",
            FuzzyQueries.estimateAutomataBytes(fuzzy),
            new HighlightAutomatonCostEstimator(bq, "field"::equals, true).estimate()
        );
    }

    /** Checks the estimate never falls below the RAM the automaton {@code FuzzyQuery} actually builds. */
    public void testEstimateIsCeilingOnMeasuredAutomataRam() {
        int[] termLengths = { 5, 20, 50, 200 };
        int[] maxEditsValues = { 1, 2 };
        int[] prefixLengths = { 0, 3 };
        Alphabet[] alphabets = Alphabet.values();

        long worstRatioMicros = 0;

        for (int termLength : termLengths) {
            for (int maxEdits : maxEditsValues) {
                for (int prefix : prefixLengths) {
                    for (Alphabet alphabet : alphabets) {
                        Random rnd = new Random(0xC0FFEEL ^ termLength ^ alphabet.ordinal());
                        String term = alphabet.generate(termLength, rnd);
                        FuzzyQuery query = new FuzzyQuery(new Term("field", term), maxEdits, prefix);

                        long estimated = new HighlightAutomatonCostEstimator(query, ACCEPT_ALL_FIELDS, true).estimate();
                        long measured = measureConsumeTermsMatchingBytes(query);

                        double ratio = measured == 0L ? Double.POSITIVE_INFINITY : (double) estimated / (double) measured;
                        long ratioMicros = measured == 0L ? Long.MAX_VALUE : Math.round(ratio * 1_000_000.0);
                        worstRatioMicros = Math.max(worstRatioMicros, ratioMicros);

                        assertThat(
                            String.format(
                                Locale.ROOT,
                                "estimate must be a ceiling on the measured highlighter automaton RAM "
                                    + "[termLen=%d, maxEdits=%d, prefix=%d, alphabet=%s, estimated=%d, measured=%d, ratio=%.3f]",
                                termLength,
                                maxEdits,
                                prefix,
                                alphabet,
                                estimated,
                                measured,
                                ratio
                            ),
                            estimated,
                            greaterThanOrEqualTo(measured)
                        );
                    }
                }
            }
        }
        logger.info("HighlightAutomatonCostEstimator worst reservation/actual ratio: {}", worstRatioMicros / 1_000_000.0);
    }

    private static long measureConsumeTermsMatchingBytes(Query query) {
        long[] bytes = new long[1];
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTermsMatching(Query leaf, String field, Supplier<ByteRunAutomaton> automaton) {
                bytes[0] += automaton.get().ramBytesUsed();
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;
            }
        });
        return bytes[0];
    }

    private enum Alphabet {
        SINGLE_CHAR {
            @Override
            String generate(int n, Random r) {
                return "a".repeat(n);
            }
        },
        ASCII_LETTERS {
            @Override
            String generate(int n, Random r) {
                StringBuilder sb = new StringBuilder(n);
                for (int i = 0; i < n; i++) {
                    sb.append((char) ('a' + r.nextInt(26)));
                }
                return sb.toString();
            }
        },
        UNICODE_BMP {
            @Override
            String generate(int n, Random r) {
                StringBuilder sb = new StringBuilder(n);
                for (int i = 0; i < n; i++) {
                    int cp;
                    do {
                        cp = r.nextInt(0xD800);
                    } while (Character.isISOControl(cp) || Character.isWhitespace(cp));
                    sb.appendCodePoint(cp);
                }
                return sb.toString();
            }
        };

        abstract String generate(int n, Random r);
    }
}
