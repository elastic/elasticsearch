/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A {@code bool} query with many clauses must not be able to take a node down.
 *
 * <p>{@code max_clause_count} bounds the number of clauses, but it assumes that a clause needs 16kb, and clauses can need far more. Each
 * clause can allocate memory twice: when the Lucene query is built, where it can be estimated from the clause, and when the scorers of
 * all the clauses are created, where it depends on how many documents a clause matches. The cases cover both, and the nesting of the
 * clauses:
 * <ul>
 * <li>wildcard clauses, which keep their automata for as long as the request runs and are charged while the query is built, spread over
 * nested {@code bool}s;
 * <li>point range clauses that match nearly every document, whose bit sets are all held at once while the scorers are created and are
 * charged then, flat and spread over nested {@code bool}s;
 * <li>the clause limit itself, which has to hold for nested {@code bool}s as well.
 * </ul>
 * Each of the first two cases is a single request that is more than the heap of the node can hold, so the request circuit breaker has to
 * reject it, and without the breaker the node runs out of memory. The query cache is off so that every clause is built and charged,
 * whatever an earlier case left in it.
 */
public class BoolQueryHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "bool-query-heap-pressure";
    private static final int DOCS = 500_000;
    // Many documents share a term, so the terms dictionary stays small whatever the number of documents.
    private static final int TERMS = 10_000;

    /**
     * The clause limit of the node: 256mb of heap times 64 over {@code thread_pool.search.size}, which is pinned below because the number
     * of search threads depends on the processors of the machine. {@link #testClauseLimitHoldsForNestedBools} checks this value.
     */
    private static final int MAX_CLAUSE_COUNT = 4096;
    private static final int CLAUSES_PER_NESTED_BOOL = 100;
    // Under the limit, and enough range clauses to need more memory than the breaker allows, and more than the heap, if not charged.
    private static final int MANY_CLAUSES = MAX_CLAUSE_COUNT - 96;
    /**
     * After a leading {@code *x}, these twelve wildcards compile to an automaton of about 9 MB, under the default determinization limit, so
     * 26 clauses with a different {@code x} are more than the heap of the node can hold.
     */
    private static final String TWELVE_ANY = "????????????";
    private static final int WILDCARD_CLAUSES = 26;
    private static final int WILDCARDS_PER_NESTED_BOOL = 5;

    private static final String WILDCARD_LABEL = "wildcard";
    private static final String POINT_RANGE_LABEL = "pointrange-execution";

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").setting("thread_pool.search.size", "4")
        .setting("indices.queries.cache.size", "0%")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexDocuments() throws IOException {
        // The range field has points but no doc values, so that a range clause can only be answered by building a bit set from the points,
        // rather than by running over doc values.
        indexDocuments(INDEX, 1, """
            {"k": {"type": "keyword"}, "n": {"type": "long", "doc_values": false}}""", DOCS, i -> Strings.format("""
            {"k":"term%06d","n":%d}""", i % TERMS, i + 1));
    }

    /**
     * Each wildcard clause builds an automaton that the query keeps until the request ends, megabytes for the pattern used, far more than
     * the 16kb a clause is assumed to need. They are in nested {@code bool}s of {@link #WILDCARDS_PER_NESTED_BOOL}, so that no
     * {@code bool} has many clauses but the total does, and the breaker has to account for the clauses of all of them.
     */
    public void testNestedBoolsOfWildcardClausesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(
            n -> search(INDEX, searchBody(nestedShoulds(n, WILDCARDS_PER_NESTED_BOOL, BoolQueryHeapPressureIT::wildcard))),
            1,
            WILDCARD_CLAUSES,
            WILDCARD_LABEL
        );
    }

    /**
     * Point range clauses that match nearly every document are answered with a bit set per clause, and the scorers of all the clauses of
     * a {@code bool} are created together, so a request holds the bit sets of all of them: about 62kb each for {@link #DOCS} documents.
     */
    public void testRangeClausesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(
            n -> search(INDEX, searchBody(should(n, BoolQueryHeapPressureIT::range))),
            200,
            MANY_CLAUSES,
            POINT_RANGE_LABEL
        );
    }

    /**
     * The same range clauses spread over nested {@code bool}s of {@link #CLAUSES_PER_NESTED_BOOL}: the scorers of the nested {@code bool}s
     * are created together as well, and the breaker has to see the bit sets of all of them.
     */
    public void testNestedBoolsOfRangeClausesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(
            n -> search(INDEX, searchBody(nestedShoulds(n, CLAUSES_PER_NESTED_BOOL, BoolQueryHeapPressureIT::range))),
            200,
            MANY_CLAUSES,
            POINT_RANGE_LABEL
        );
    }

    /**
     * {@code max_clause_count} counts the clauses of nested {@code bool}s together, so a request is rejected for having more clauses than
     * the limit however they are nested, and accepted at the limit. The rejection reports the limit, so this also checks the one the other
     * cases assume. It needs no circuit breaker, so it holds when the breakers are turned off.
     */
    public void testClauseLimitHoldsForNestedBools() throws IOException {
        IntFunction<String> term = BoolQueryHeapPressureIT::term;
        // A flat bool at the limit is accepted, and one clause more is not.
        assertSucceeds(search(INDEX, searchBody(should(MAX_CLAUSE_COUNT, term))));
        assertTooManyClauses(search(INDEX, searchBody(should(MAX_CLAUSE_COUNT + 1, term))));
        // The same count in nested bools, which are each far below the limit, and twice the limit.
        assertSucceeds(search(INDEX, searchBody(nestedShoulds(MAX_CLAUSE_COUNT, CLAUSES_PER_NESTED_BOOL, term))));
        assertTooManyClauses(search(INDEX, searchBody(nestedShoulds(MAX_CLAUSE_COUNT + 1, CLAUSES_PER_NESTED_BOOL, term))));
        assertTooManyClauses(search(INDEX, searchBody(nestedShoulds(2 * MAX_CLAUSE_COUNT, CLAUSES_PER_NESTED_BOOL, term))));
        assertNodeAlive();
    }

    private void assertTooManyClauses(Request request) throws IOException {
        Map<String, Object> body = errorBodyOrNull(request);
        assertNotNull("expected the request to be rejected for having too many clauses, but it succeeded", body);
        assertThat(body.get("status"), equalTo(400));
        assertThat(body.toString(), containsString("too_many_nested_clauses"));
        assertThat(body.toString(), containsString("maxClauseCount is set to " + MAX_CLAUSE_COUNT));
    }

    /** Every clause has a different leading letter, so that the automata are not shared. */
    private static String wildcard(int i) {
        return Strings.format("""
            {"wildcard": {"k": {"value": "*%c%s"}}}""", (char) ('a' + i), TWELVE_ANY);
    }

    /** Matches nearly every document, and differs from every other so that the clauses are not merged into one. */
    private static String range(int i) {
        return Strings.format("""
            {"range": {"n": {"gte": %d, "lte": %d}}}""", i + 2, DOCS);
    }

    private static String term(int i) {
        return Strings.format("""
            {"term": {"k": "term%06d"}}""", i);
    }

    private static String searchBody(String boolQuery) {
        return Strings.format("""
            {"size": 0, "track_total_hits": true, "query": %s}
            """, boolQuery);
    }

    private static String should(int count, IntFunction<String> clause) {
        return Strings.format("""
            {"bool": {"should": [%s]}}""", clauses(count, clause));
    }

    /** {@code count} clauses in {@code bool}s of {@code perBool}, in one {@code bool}. */
    private static String nestedShoulds(int count, int perBool, IntFunction<String> clause) {
        StringBuilder nested = new StringBuilder();
        for (int first = 0; first < count; first += perBool) {
            int from = first;
            int size = Math.min(perBool, count - first);
            if (first > 0) {
                nested.append(',');
            }
            nested.append(should(size, i -> clause.apply(from + i)));
        }
        return Strings.format("""
            {"bool": {"should": [%s]}}""", nested);
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
