/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that run ESQL queries with {@code WHERE field IN (subquery)} predicates that use a ton of memory. We want to make sure they don't
 * consume the entire heap and crash Elasticsearch.
 * <p>
 * Four shapes are exercised:
 * <ul>
 *     <li>The size of the intermediate results LocalRelation exceeds esql.intermediate_local_relation_max_size.</li>
 *     <li>The size of the intermediate results trips the BlockHash used by duplication removal in AbstractSubqueryJoin.</li>
 *     <li>The IN subquery carries a large amount of data (many big fields, or a giant text field) through to the output.</li>
 *     <li>The IN subquery carries a sort over many big fields or a grouping aggregation with a large grouping set.</li>
 * </ul>
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackInSubqueryIT extends HeapAttackTestCase {

    // Reuse HeapAttackIT methods to prepare the indices.
    private static final HeapAttackIT heapAttackIT = new HeapAttackIT();

    // Stay above esql.in_subquery_hash_join_threshold (100) so the IN subquery runs as a hash join
    private static final int MAX_DOC = 350;

    private static final int STRING_FIELD_1000 = 1000;

    @Before
    public void checkCapability() {
        assumeTrue("Run these tests in snapshot build", Build.current().isSnapshot());
    }

    /**
     * Trips the {@code esql.intermediate_local_relation_max_size} guard rather than the circuit breaker: the IN subquery returns more data
     * (~32MB) than the cap (default {@code min(2% heap, 100mb)} ≈ 10mb on the heap-attack cluster), so {@code EsqlSession.resultToPlan}
     * rejects the subquery result with a {@code 400 illegal_argument_exception}.
     */
    public void testIntermediateLocalRelationMaxSizeWithInSubquery() throws IOException {
        heapAttackIT.initGiantTextField(20, true, 1, true);
        try {
            Map<String, Object> response = giantTextInSubquery();
            fail("expected the intermediate local relation size guard to reject the subquery result but got " + response);
        } catch (ResponseException e) {
            assertMap(
                responseAsMap(e.getResponse()),
                matchesMap().extraOk()
                    .entry("status", 400)
                    .entry(
                        "error",
                        matchesMap().extraOk()
                            .entry("type", "illegal_argument_exception")
                            .entry("reason", containsString("sub-plan execution results too large"))
                    )
            );
        }
    }

    /**
     * Trips the circuit breaker inside the IN-subquery <em>duplication removal</em> step, where {@code AbstractSubqueryJoin} runs the
     * subquery's key column through a {@link org.elasticsearch.compute.aggregation.blockhash.BlockHash} on the coordinator before deciding
     * the join shape. The values are distinct giant texts so the dedup {@code BlockHash} retains one entry per document, and the
     * per-request {@code esql.intermediate_local_relation_max_size} cap is raised so the large subquery result reaches dedup instead
     * of being rejected by {@code checkPagesBelowSize} first.
     */
    public void testDedupBlockHashWithInSubquery() throws IOException {
        // ~64MB of distinct 1MB values, sized to hit the narrow window where the request circuit breaker (~60% of the 512MB node
        // heap, ~307MB) trips inside dedup rather than earlier. SessionUtils.fromPages concatenates the subquery result while still
        // holding the input pages (~2x the data), keeping the request breaker just below its ~307MB limit so it survives; then the
        // dedup BlockHash allocates a third copy of the distinct keys, tipping the breaker just over ~307MB and tripping it (observed:
        // request usage would be ~307.2MB vs the ~307.1MB limit, via BlockHash reused arrays). A larger total (e.g. 128 docs) instead
        // trips earlier, in the fromPages concatenation, before reaching the BlockHash. The cap below is only raised so
        // checkPagesBelowSize lets the result through to dedup.
        heapAttackIT.initGiantTextField(64, true, 1, true);
        setIntermediateLocalRelationMaxSize("128mb");
        try {
            assertCircuitBreaksVia(attempt -> giantTextInSubquery(), "AbstractSubqueryJoin", "BlockHash");
        } finally {
            setIntermediateLocalRelationMaxSize(null);
        }
    }

    public void testRandomKeywordOrTextFieldsWithInSubquery() throws IOException {
        String dataType = keywordOrText();
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, dataType, true, STRING_FIELD_1000);
        assertCircuitBreaks(attempt -> inSubqueryKeepManyFields());
    }

    public void testSortWithInSubquery() throws IOException {
        String dataType = keywordOrText();
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, dataType, true, STRING_FIELD_1000);
        assertCircuitBreaks(attempt -> inSubqueryWithSort(sortOrGroupingColumns(100)));
    }

    public void testAggWithInSubquery() throws IOException {
        String dataType = keywordOrText();
        heapAttackIT.initManyBigFieldsIndex(MAX_DOC, dataType, true, STRING_FIELD_1000);
        assertCircuitBreaks(attempt -> inSubqueryWithAgg("c = COUNT_DISTINCT(f499)", sortOrGroupingColumns(100)));
    }

    private Map<String, Object> inSubqueryKeepManyFields() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | WHERE f000 IN (FROM manybigfields | KEEP f000)");
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> giantTextInSubquery() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM bigtext | WHERE f IN (FROM bigtext | KEEP f)");
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> inSubqueryWithSort(String sortKeys) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | WHERE f000 IN (FROM manybigfields | SORT ")
            .append(sortKeys)
            .append(" | KEEP f000 | LIMIT ")
            .append(MAX_DOC)
            .append(")");
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> inSubqueryWithAgg(String agg, String grouping) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | WHERE f000 IN (FROM manybigfields | STATS ").append(agg);
        if (grouping != null && grouping.isEmpty() == false) {
            query.append(" BY ").append(grouping);
        }
        query.append(" | KEEP f000)");
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private static String sortOrGroupingColumns(int columnCount) {
        StringBuilder keys = new StringBuilder(fieldName(0));
        for (int f = 1; f < columnCount; f++) {
            keys.append(", ").append(fieldName(f));
        }
        return keys.toString();
    }

    private static String fieldName(int f) {
        return "f" + String.format(Locale.ROOT, "%03d", f);
    }

    private static String keywordOrText() {
        return randomBoolean() ? "keyword" : "text";
    }

    /**
     * Change (or resets, when {@code size} is {@code null}) the maximum size of a subplan(subquery)'s intermediate {@code LocalRelation}.
     * The default is {@code min(2% heap, 100mb)}, which on the small heap-attack cluster rejects large subquery results before they reach
     * the IN-subquery dedup {@code BlockHash}. Tests that want to exercise the dedup breaker should raise it first.
     */
    protected void setIntermediateLocalRelationMaxSize(String size) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{\"persistent\": {\"esql.intermediate_local_relation_max_size\": " + (size == null ? "null" : "\"" + size + "\"") + "}}"
        );
        adminClient().performRequest(request);
    }

    private static void verifyCircuitBreakingException(ResponseException re) throws IOException {
        Map<?, ?> map = responseAsMap(re.getResponse());
        assertMap(
            map,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }
}
