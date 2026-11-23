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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ListMatcher;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.matchesRegex;

/**
 * Heap-attack tests for sorting, TopN, grouping on many fields and LOOKUP JOIN/ENRICH explosions. We want to make
 * sure they don't consume the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackIT extends HeapAttackTestCase {

    /**
     * This used to fail, but we've since compacted top n so it actually succeeds now.
     */
    public void testSortByManyLongsSuccess() throws IOException {
        initManyLongs(10);
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

    public void testLookupExplosion() throws IOException {
        int sensorDataCount = 400;
        int lookupEntries = 10000;
        Map<?, ?> map = lookupExplosion(sensorDataCount, lookupEntries);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionManyMatches() throws IOException {
        // 1500, 10000 is enough locally, but some CI machines need more.
        assertCircuitBreaks(attempt -> lookupExplosion(attempt * 1500, 10000));
    }

    public void testLookupExplosionNoFetch() throws IOException {
        int sensorDataCount = 6000;
        int lookupEntries = 10000;
        Map<?, ?> map = lookupExplosionNoFetch(sensorDataCount, lookupEntries);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionNoFetchManyMatches() throws IOException {
        // 8500 is plenty on most nodes
        assertCircuitBreaks(attempt -> lookupExplosionNoFetch(attempt * 8500, 10000));
    }

    public void testLookupExplosionBigString() throws IOException {
        int sensorDataCount = 150;
        int lookupEntries = 1;
        Map<?, ?> map = lookupExplosionBigString(sensorDataCount, lookupEntries);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount * lookupEntries))));
    }

    public void testLookupExplosionBigStringManyMatches() throws IOException {
        // 500, 1 is enough to make it fail locally but some CI needs more
        assertCircuitBreaks(attempt -> lookupExplosionBigString(attempt * 500, 1));
    }

    private Map<String, Object> lookupExplosion(int sensorDataCount, int lookupEntries) throws IOException {
        try {
            lookupExplosionData(sensorDataCount, lookupEntries);
            StringBuilder query = startQuery();
            query.append("FROM sensor_data | LOOKUP JOIN sensor_lookup ON id | STATS COUNT(location)\"}");
            return responseAsMap(query(query.toString(), null));
        } finally {
            deleteIndex("sensor_data");
            deleteIndex("sensor_lookup");
        }
    }

    private Map<String, Object> lookupExplosionNoFetch(int sensorDataCount, int lookupEntries) throws IOException {
        try {
            lookupExplosionData(sensorDataCount, lookupEntries);
            StringBuilder query = startQuery();
            query.append("FROM sensor_data | LOOKUP JOIN sensor_lookup ON id | STATS COUNT(*)\"}");
            return responseAsMap(query(query.toString(), null));
        } finally {
            deleteIndex("sensor_data");
            deleteIndex("sensor_lookup");
        }
    }

    private void lookupExplosionData(int sensorDataCount, int lookupEntries) throws IOException {
        initSensorData(sensorDataCount, 1);
        initSensorLookup(lookupEntries, 1, i -> "73.9857 40.7484");
    }

    private Map<String, Object> lookupExplosionBigString(int sensorDataCount, int lookupEntries) throws IOException {
        try {
            initSensorData(sensorDataCount, 1);
            initSensorLookupString(lookupEntries, 1, i -> {
                int target = Math.toIntExact(ByteSizeValue.ofMb(1).getBytes());
                StringBuilder str = new StringBuilder(Math.toIntExact(ByteSizeValue.ofMb(2).getBytes()));
                while (str.length() < target) {
                    str.append("Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
                }
                logger.info("big string is {} characters", str.length());
                return str.toString();
            });
            StringBuilder query = startQuery();
            query.append("FROM sensor_data | LOOKUP JOIN sensor_lookup ON id | STATS COUNT(string)\"}");
            return responseAsMap(query(query.toString(), null));
        } finally {
            deleteIndex("sensor_data");
            deleteIndex("sensor_lookup");
        }
    }

    public void testEnrichExplosion() throws IOException {
        int sensorDataCount = 1000;
        int lookupEntries = 100;
        Map<?, ?> map = enrichExplosion(sensorDataCount, lookupEntries);
        assertMap(map, matchesMap().extraOk().entry("values", List.of(List.of(sensorDataCount))));
    }

    public void testEnrichExplosionManyMatches() throws IOException {
        // 1000, 10000 is enough on most nodes
        assertCircuitBreaks(attempt -> enrichExplosion(1000, attempt * 5000));
    }

    private Map<String, Object> enrichExplosion(int sensorDataCount, int lookupEntries) throws IOException {
        try {
            initSensorData(sensorDataCount, 1);
            initSensorEnrich(lookupEntries, 1, i -> "73.9857 40.7484");
            try {
                StringBuilder query = startQuery();
                query.append("FROM sensor_data | ENRICH sensor ON id | STATS COUNT(*)\"}");
                return responseAsMap(query(query.toString(), null));
            } finally {
                Request delete = new Request("DELETE", "/_enrich/policy/sensor");
                assertMap(responseAsMap(client().performRequest(delete)), matchesMap().entry("acknowledged", true));
            }
        } finally {
            deleteIndex("sensor_data");
            deleteIndex("sensor_lookup");
        }
    }

    private void initSensorLookupString(int lookupEntries, int sensorCount, IntFunction<String> string) throws IOException {
        logger.info("loading sensor lookup with huge strings");
        createIndex("sensor_lookup", Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build(), """
            {
                "properties": {
                    "id": { "type": "long" },
                    "string": { "type": "text" }
                }
            }""");
        int docsPerBulk = 10;
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < lookupEntries; i++) {
            int sensor = i % sensorCount;
            data.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"id": %d, "string": "%s"}
                """, sensor, string.apply(sensor)));
            if (i % docsPerBulk == docsPerBulk - 1) {
                bulk("sensor_lookup", data.toString());
                data.setLength(0);
            }
        }
        initIndex("sensor_lookup", data.toString());
    }

    private void initSensorEnrich(int lookupEntries, int sensorCount, IntFunction<String> location) throws IOException {
        initSensorLookup(lookupEntries, sensorCount, location);
        logger.info("loading sensor enrich");

        Request create = new Request("PUT", "/_enrich/policy/sensor");
        create.setJsonEntity("""
            {
              "match": {
                "indices": "sensor_lookup",
                "match_field": "id",
                "enrich_fields": ["location"]
              }
            }
            """);
        assertMap(responseAsMap(client().performRequest(create)), matchesMap().entry("acknowledged", true));
        Request execute = new Request("POST", "/_enrich/policy/sensor/_execute");
        assertMap(responseAsMap(client().performRequest(execute)), matchesMap().entry("status", Map.of("phase", "COMPLETE")));
    }
}
