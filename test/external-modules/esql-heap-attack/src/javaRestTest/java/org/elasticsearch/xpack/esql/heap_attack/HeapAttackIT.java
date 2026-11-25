/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.client.config.RequestConfig;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.IntOrLongMatcher;
import org.elasticsearch.test.ListMatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.matchesRegex;

/**
 * Heap-attack tests for sorting, TopN and grouping on many fields. We want to make
 * sure they don't consume the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackIT extends HeapAttackTestCase {

    /**
     * This used to fail, but we've since compacted top n so it actually succeeds now.
     */
    public void testSortByManyLongsSuccess() throws IOException {
        initManyLongs(10);
        // | SORT a, b, i0, i1, ...i500 | KEEP a, b | LIMIT 10000
        Map<String, Object> response = sortByManyLongs(500);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "b").entry("type", "long"));
        ListMatcher values = matchesList();
        for (int b = 0; b < 10; b++) {
            for (int i = 0; i < 1000; i++) {
                values = values.item(List.of(0, b));
            }
        }
        assertResultMap(response, columns, values);
    }

    /**
     * This used to crash the node with an out of memory, but now it just trips a circuit breaker.
     */
    public void testSortByManyLongsTooMuchMemory() throws IOException {
        initManyLongs(10);
        // 5000 is plenty to break on most nodes
        assertCircuitBreaks(attempt -> sortByManyLongs(attempt * 5000));
    }

    /**
     * This should record an async response with a {@link CircuitBreakingException}.
     */
    public void testSortByManyLongsTooMuchMemoryAsync() throws IOException {
        initManyLongs(10);
        Request request = new Request("POST", "/_query/async");
        request.addParameter("error_trace", "");
        request.setJsonEntity(makeSortByManyLongs(5000).toString().replace("\n", "\\n"));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(6).millis())).build())
                .setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        logger.info("--> test {} started async", getTestName());
        Response response = runQuery(() -> {
            Response r = client().performRequest(request);
            Map<?, ?> map = responseAsMap(r);
            assertMap(map, matchesMap().extraOk().entry("is_running", true).entry("id", any(String.class)));
            String id = map.get("id").toString();
            Request fetch = new Request("GET", "/_query/async/" + id);
            long endTime = System.nanoTime() + TimeValue.timeValueMinutes(5).nanos();
            while (System.nanoTime() < endTime) {
                Response resp;
                try {
                    resp = client().performRequest(fetch);
                } catch (ResponseException e) {
                    if (e.getResponse().getStatusLine().getStatusCode() == 403) {
                        /*
                         * There's a bug when loading from the translog with security
                         * enabled. If we retry a few times we'll load from the index
                         * itself and should succeed.
                         */
                        logger.error("polled for results got 403");
                        continue;
                    }
                    if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                        logger.error("polled for results got 404");
                        continue;
                    }
                    if (e.getResponse().getStatusLine().getStatusCode() == 503) {
                        logger.error("polled for results got 503");
                        continue;
                    }
                    if (e.getResponse().getStatusLine().getStatusCode() == 429) {
                        // This is what we were going to for - a CircuitBreakerException
                        return e.getResponse();
                    }
                    throw e;
                }
                Map<?, ?> m = responseAsMap(resp);
                logger.error("polled for results {}", m);
                boolean isRunning = (boolean) m.get("is_running");
                if (isRunning == false) {
                    return resp;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            throw new IOException("timed out");
        });
        assertThat(response.getStatusLine().getStatusCode(), equalTo(429));
        Map<?, ?> map = responseAsMap(response);
        assertMap(
            map,
            matchesMap().entry("status", 429)
                .entry(
                    "error",
                    matchesMap().extraOk()
                        .entry("bytes_wanted", greaterThan(1000))
                        .entry("reason", matchesRegex("\\[request] Data too large, data for \\[.+] would be .+"))
                        .entry("durability", "TRANSIENT")
                        .entry("type", "circuit_breaking_exception")
                        .entry("bytes_limit", greaterThan(1000))
                        .entry("root_cause", matchesList().item(any(Map.class)))
                )
        );
    }

    public void testSortByManyLongsGiantTopN() throws IOException {
        initManyLongs(10);
        assertMap(
            sortBySomeLongsLimit(100000),
            matchesMap().entry("took", greaterThan(0))
                .entry("is_partial", false)
                .entry("columns", List.of(Map.of("name", "MAX(a)", "type", "long")))
                .entry("values", List.of(List.of(9)))
                .entry("documents_found", greaterThan(0))
                .entry("values_loaded", greaterThan(0))
                .entry("rows_emitted", IntOrLongMatcher.isIntOrLong())
                .entry("bytes_read", IntOrLongMatcher.isIntOrLong())
                .entry("read_nanos", IntOrLongMatcher.isIntOrLong())
                .entry("read_cpu_nanos", IntOrLongMatcher.isIntOrLong())
                .entry("cpu_nanos", IntOrLongMatcher.isIntOrLong())
                .entry("driver_nanos", IntOrLongMatcher.isIntOrLong())
                .entry("completion_time_in_millis", greaterThan(0L))
                .entry("expiration_time_in_millis", greaterThan(0L))
                .entry("start_time_in_millis", greaterThan(0L))
        );
    }

    public void testSortByManyLongsGiantTopNTooMuchMemory() throws IOException {
        initManyLongs(20);
        assertCircuitBreaks(attempt -> sortBySomeLongsLimit(attempt * 500000));
    }

    public void testStupidTopN() throws IOException {
        initManyLongs(1); // Doesn't actually matter how much data there is.
        assertCircuitBreaks(attempt -> sortBySomeLongsLimit(2147483630));
    }

    /**
     * This groups on about 200 columns which is a lot but has never caused us trouble.
     */
    public void testGroupOnSomeLongs() throws IOException {
        initManyLongs(10);
        Response resp = groupOnManyLongs(200);
        Map<String, Object> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertResultMap(map, columns, values);
    }

    /**
     * This groups on 5000 columns which used to throw a {@link StackOverflowError}.
     */
    public void testGroupOnManyLongs() throws IOException {
        initManyLongs(10);
        Response resp = groupOnManyLongs(5000);
        Map<String, Object> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertResultMap(map, columns, values);
    }
}
