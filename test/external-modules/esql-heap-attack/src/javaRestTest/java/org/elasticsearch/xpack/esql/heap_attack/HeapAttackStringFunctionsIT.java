/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.hasSize;

/**
 * Heap-attack tests for string functions and long EVAL chains (CONCAT, REPEAT, DISSECT,
 * MV_CONCAT, TO_BASE64, JSON strings).
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackStringFunctionsIT extends HeapAttackTestCase {

    public void testSmallConcat() throws IOException {
        initSingleDocIndex();
        Response resp = concat(2);
        Map<String, Object> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "str").entry("type", "keyword"));
        ListMatcher values = matchesList().item(List.of(1, "1".repeat(100)));
        assertResultMap(map, columns, values);
    }

    public void testHugeConcat() throws IOException {
        initSingleDocIndex();
        ResponseException e = expectThrows(ResponseException.class, () -> concat(10));
        Map<?, ?> map = responseAsMap(e.getResponse());
        logger.info("expected request rejected {}", map);
        MapMatcher mapMatcher = matchesMap();
        assertMap(
            map,
            mapMatcher.entry("status", 400)
                .entry("error", matchesMap().extraOk().entry("reason", "concatenating more than [1048576] bytes is not supported"))
        );
    }

    public void testSmallJsonString() throws IOException {
        jsonString(5);
    }

    public void testHugeJsonString() throws IOException {
        assertFoldCircuitBreaks(attempt -> jsonString(attempt * 50));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyConcat() throws IOException {
        int strings = 300;
        initManyLongs(10);
        assertManyStrings(manyConcat("FROM manylongs", strings), strings);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyConcat() throws IOException {
        initManyLongs(10);
        // 2000 is plenty to break on most nodes
        assertCircuitBreaks(attempt -> manyConcat("FROM manylongs", attempt * 2000));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyConcatFromRow() throws IOException {
        int strings = 2000;
        assertManyStrings(manyConcat("ROW a=9999, b=9999, c=9999, d=9999, e=9999", strings), strings);
    }

    /**
     * A wide ROW followed by many DISSECTs. Planning used to rebuild every node's output on each call,
     * making it quadratic in the number of DISSECTs.
     */
    public void testManyDissectFromRow() throws IOException {
        int columns = 2500;
        int dissects = 450;
        StringBuilder query = startQuery();
        query.append("ROW s=\\\"a\\\"");
        for (int c = 0; c < columns; c++) {
            query.append(", c").append(c).append("=1");
        }
        for (int d = 0; d < dissects; d++) {
            query.append("\n| DISSECT s \\\"%{k").append(d).append("}\\\"");
        }
        query.append("\n| LIMIT 1\"}");
        Map<String, Object> response = responseAsMap(query(query.toString(), "columns"));
        assertThat((List<?>) response.get("columns"), hasSize(1 + columns + dissects));
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyConcatFromRow() throws IOException {
        // 5000 is plenty to break on most nodes
        assertFoldCircuitBreaks(
            attempt -> manyConcat(
                "ROW a=9999999999999, b=99999999999999999, c=99999999999999999, d=99999999999999999, e=99999999999999999",
                attempt * 5000
            )
        );
    }

    /**
     * Fails to parse a huge huge query.
     */
    public void testHugeHugeManyConcatFromRow() throws IOException {
        assertParseFailure(() -> manyConcat("ROW a=9999, b=9999, c=9999, d=9999, e=9999", 6000));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyRepeat() throws IOException {
        int strings = 12;
        initManyLongs(10);
        assertManyStrings(manyRepeat("FROM manylongs", strings), strings);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyRepeat() throws IOException {
        initManyLongs(10);
        // 75 is plenty to break on most nodes
        assertCircuitBreaks(attempt -> manyRepeat("FROM manylongs", attempt * 75));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyRepeatFromRow() throws IOException {
        int strings = 300;
        assertManyStrings(manyRepeat("ROW a = 99", strings), strings);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyRepeatFromRow() throws IOException {
        // 400 is enough to break on most nodes
        assertFoldCircuitBreaks(attempt -> manyRepeat("ROW a = 99", attempt * 400));
    }

    /**
     * Fails to parse a huge, huge query.
     */
    public void testHugeHugeManyRepeatFromRow() throws IOException {
        assertParseFailure(() -> manyRepeat("ROW a = 99", 100000));
    }

    /**
     * Joins a large multivalued field into a single enormous string.
     */
    public void testHugeMvConcat() throws IOException {
        // One doc whose f00 holds 300k values; joined with a ~1kb delimiter that is one huge string.
        initMvLongsIndex(1, 1, 300000, false);
        // Lower the request breaker so the join trips it well before the node runs out of memory.
        try {
            setRequestBreakerLimit("20%");
            assertCircuitBreaks(attempt -> mvConcat(attempt * 999));
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    /**
     * Chains many TO_BASE64 calls so the encoded value grows ~1.33x per level into a single huge
     * string.
     */
    public void testHugeToBase64() throws IOException {
        initGiantTextField(1, false, 5);
        // Lower the request breaker so the chain trips it well before the node runs out of memory.
        try {
            setRequestBreakerLimit("20%");
            assertCircuitBreaks(attempt -> toBase64Chain(10 + attempt * 4));
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    public void testManyEval() throws IOException {
        initManyLongs(10);
        Map<String, Object> response = manyEval(1);
        ListMatcher columns = matchesList();
        columns = columns.item(matchesMap().entry("name", "a").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "b").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "c").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "d").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "e").entry("type", "long"));
        for (int i = 0; i < 20; i++) {
            columns = columns.item(matchesMap().entry("name", "i0" + i).entry("type", "long"));
        }
        assertResultMap(response, columns, hasSize(10_000));
    }

    public void testTooManyEval() throws IOException {
        initManyLongs(10);
        // 490 is plenty to fail on most nodes
        assertCircuitBreaks(attempt -> manyEval(attempt * 490));
    }

    public void testTooManyEval_withViewDefined() throws IOException {
        initManyLongs(10);

        // When a view is defined, ViewResolver can do signifcant work with a large stack.
        Request r = new Request("PUT", "/_query/view/my_view");
        r.setJsonEntity("""
            { "query": "FROM manylongs" }""");
        client().performRequest(r);

        // 490 is plenty to fail on most nodes
        assertCircuitBreaks(attempt -> manyEval(attempt * 490));
    }
}
