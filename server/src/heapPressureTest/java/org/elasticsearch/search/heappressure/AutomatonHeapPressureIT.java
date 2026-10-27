/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.function.IntFunction;

/**
 * Wildcard and regexp queries compile automata while the query is built on the data node, and each clause keeps its automaton until the
 * request ends. One pattern can need more states than the heap holds, so the breaker must be charged as they are built and for what each
 * clause keeps. The documents are small: the memory is all in the query.
 */
public class AutomatonHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "automata";
    private static final int FIELDS = 50;
    /**
     * After a leading {@code *x}, these twelve wildcards compile to an automaton of about 9 MB (11 MB case-insensitive), under the default
     * determinization limit.
     */
    private static final String TWELVE_ANY = "????????????";

    private static final String WIDE_INDEX = "wide";
    private static final int WIDE_FIELDS = 400;
    /** Fourteen {@code *}-separated runs of a word. It compiles to about 0.8 MB, so one automaton per field would need about 650 MB. */
    private static final String WIDE_PATTERN = ("asdf".repeat(6) + "*").repeat(14);

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexKeywords() throws IOException {
        StringBuilder properties = new StringBuilder("""
            {"tag": {"type": "keyword"}, "dv_tag": {"type": "keyword", "index": false}""");
        for (int f = 0; f < FIELDS; f++) {
            properties.append(Strings.format(", \"f%02d\": {\"type\": \"keyword\"}", f));
        }
        properties.append('}');
        indexDocuments(INDEX, 1, properties.toString(), 1000, i -> {
            StringBuilder doc = new StringBuilder(Strings.format("{\"tag\":\"tag-%04d\",\"dv_tag\":\"tag-%04d\"", i, i));
            for (int f = 0; f < FIELDS; f++) {
                doc.append(Strings.format(",\"f%02d\":\"value-%04d\"", f, i));
            }
            return doc.append('}').toString();
        });
    }

    /** One document that dynamically maps {@link #WIDE_FIELDS} fields, each a text field with a keyword sub-field. */
    @Before
    public void indexWideDocument() throws IOException {
        indexDocuments(WIDE_INDEX, 1, "{}", 1, i -> "{" + clauses(WIDE_FIELDS, f -> Strings.format("\"a%d\":\"a\"", f)) + "}");
    }

    /**
     * {@code [ac]*a[ac]{20}} determinizes to 2^21 states, several times the heap. The determinization limit is lifted, so only the breaker
     * stops it.
     */
    public void testRegexpDeterminizationTripsBreaker() throws IOException {
        assertTripsOnlyWhenLarge(n -> search(INDEX, Strings.format("""
            {"size": 0, "query": {"regexp": {"tag": {"value": "[ac]*a[ac]{%d}", "max_determinized_states": 10000000}}}}
            """, n)), 2, 20, "regexp:tag");
    }

    /** Each pattern is under the determinization limit, but every clause keeps its automaton until the request ends, and 26 do not fit. */
    public void testWildcardClausesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(n -> search(INDEX, should(n, i -> wildcard("tag", (char) ('a' + i), false))), 1, 26, "wildcard");
    }

    /** Case-insensitive matching compiles a bigger automaton through a separately guarded path. */
    public void testCaseInsensitiveWildcardClausesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(n -> search(INDEX, should(n, i -> wildcard("tag", (char) ('a' + i), true))), 1, 26, "wildcard");
    }

    /** A field with doc values but no terms index is matched by running the automaton over the doc values. */
    public void testDocValuesWildcardClausesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(n -> search(INDEX, should(n, i -> wildcard("dv_tag", (char) ('a' + i), false))), 1, 26, "wildcard");
    }

    /**
     * {@code query_string} expands a pattern into a clause per field, and the clauses share one automaton. Built per field, the 50 would
     * need about 450 MB and be rejected.
     */
    public void testQueryStringSharesAutomatonAcrossFields() throws IOException {
        assertSucceeds(search(INDEX, Strings.format("""
            {"size": 0, "query": {"query_string": {"query": "*a%s", "fields": ["f*"]}}}
            """, TWELVE_ANY)));
    }

    /**
     * A {@code query_string} wildcard over all 800 text and keyword fields of a wide index. They resolve to one pattern and share one
     * automaton. This query is lenient, which turns a breaker trip on a field into a clause that matches nothing, so the breaker
     * must also not trip.
     */
    public void testQueryStringWildcardOnWideIndexSharesAutomaton() throws IOException {
        long trippedBefore = trippedCount();
        assertSucceeds(search(WIDE_INDEX, Strings.format("""
            {"size": 0, "query": {"query_string": {"query": "*:%s"}}}
            """, WIDE_PATTERN)));
        assertEquals("a clause tripped the breaker instead of sharing the automaton", trippedBefore, trippedCount());
    }

    /** Distinct patterns cannot share an automaton, so the breaker must reject them. Not lenient, so the rejection reaches the client. */
    public void testQueryStringDistinctWildcardsTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(n -> search(INDEX, Strings.format("""
            {"size": 0, "query": {"query_string": {"query": "%s", "lenient": false}}}
            """, distinctWildcards(n))), 1, 32, "wildcard");
    }

    private static String wildcard(String field, char letter, boolean caseInsensitive) {
        return Strings.format("""
            {"wildcard": {"%s": {"value": "*%c%s", "case_insensitive": %b}}}""", field, letter, TWELVE_ANY, caseInsensitive);
    }

    /** A {@code query_string} with a wildcard per letter (lower case, then upper case), each on its own field. */
    private static String distinctWildcards(int count) {
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < count; i++) {
            char letter = (char) (i < 26 ? 'a' + i : 'A' + i - 26);
            query.append(i > 0 ? " " : "").append(Strings.format("f%02d:*%c%s", i, letter, TWELVE_ANY));
        }
        return query.toString();
    }

    private static String should(int count, IntFunction<String> clause) {
        return Strings.format("""
            {"size": 0, "query": {"bool": {"should": [%s]}}}
            """, clauses(count, clause));
    }

    private static String clauses(int count, IntFunction<String> clause) {
        StringBuilder clauses = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                clauses.append(',');
            }
            clauses.append(clause.apply(i));
        }
        return clauses.toString();
    }
}
