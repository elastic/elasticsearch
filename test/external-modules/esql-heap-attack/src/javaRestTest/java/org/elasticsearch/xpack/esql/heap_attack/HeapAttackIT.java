/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;

/**
 * Tests that run ESQL queries that use a ton of memory. We want to make
 * sure they don't consume the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.buildCluster();

    static volatile boolean SUITE_ABORTED = false;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void skipOnAborted() {
        assumeFalse("skip on aborted", SUITE_ABORTED);
    }

    /**
     * This used to fail, but we've since compacted top n so it actually succeeds now.
     */
    public void testSortByManyLongsSuccess() throws IOException {
        initManyLongs();
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
        initManyLongs();
        // 5000 is plenty to break on most nodes
        assertCircuitBreaks(attempt -> sortByManyLongs(attempt * 5000));
    }

    /**
     * This should record an async response with a {@link CircuitBreakingException}.
     */
    public void testSortByManyLongsTooMuchMemoryAsync() throws IOException {
        initManyLongs();
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

    private static final int MAX_ATTEMPTS = 5;

    interface TryCircuitBreaking {
        Map<String, Object> attempt(int attempt) throws IOException;
    }

    private void assertCircuitBreaks(TryCircuitBreaking tryBreaking) throws IOException {
        assertCircuitBreaks(
            tryBreaking,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }

    private void assertFoldCircuitBreaks(TryCircuitBreaking tryBreaking) throws IOException {
        assertCircuitBreaks(
            tryBreaking,
            matchesMap().entry("status", 400).entry("error", matchesMap().extraOk().entry("type", "fold_too_much_memory_exception"))
        );
    }

    private void assertCircuitBreaks(TryCircuitBreaking tryBreaking, MapMatcher responseMatcher) throws IOException {
        int attempt = 1;
        while (attempt <= MAX_ATTEMPTS) {
            try {
                Map<String, Object> response = tryBreaking.attempt(attempt);
                logger.warn("{}: should circuit broken but got {}", attempt, response);
                attempt++;
            } catch (ResponseException e) {
                Map<?, ?> map = responseAsMap(e.getResponse());
                assertMap(map, responseMatcher);
                return;
            }
        }
        fail("giving up circuit breaking after " + attempt + " attempts");
    }

    private void assertParseFailure(ThrowingRunnable r) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, r);
        Map<?, ?> map = responseAsMap(e.getResponse());
        logger.info("expected parse failure {}", map);
        assertMap(map, matchesMap().entry("status", 400).entry("error", matchesMap().extraOk().entry("type", "parsing_exception")));
    }

    private Map<String, Object> sortByManyLongs(int count) throws IOException {
        logger.info("sorting by {} longs", count);
        return responseAsMap(query(makeSortByManyLongs(count).toString(), null));
    }

    private StringBuilder makeSortByManyLongs(int count) {
        StringBuilder query = makeManyLongs(count);
        query.append("| SORT a, b, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| KEEP a, b | LIMIT 10000\"}");
        return query;
    }

    /**
     * This groups on about 200 columns which is a lot but has never caused us trouble.
     */
    public void testGroupOnSomeLongs() throws IOException {
        initManyLongs();
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
        initManyLongs();
        Response resp = groupOnManyLongs(5000);
        Map<String, Object> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertResultMap(map, columns, values);
    }

    private Response groupOnManyLongs(int count) throws IOException {
        logger.info("grouping on {} longs", count);
        StringBuilder query = makeManyLongs(count);
        query.append("| STATS MIN(a) BY a, b, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| STATS MAX(a)\"}");
        return query(query.toString(), null);
    }

    private StringBuilder makeManyLongs(int count) {
        StringBuilder query = startQuery();
        query.append("FROM manylongs\\n| EVAL i0 = a + b, i1 = b + i0");
        for (int i = 2; i < count; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + ").append(i - 1);
        }
        return query.append("\\n");
    }

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

    private Response concat(int evals) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM single | EVAL str = TO_STRING(a)");
        for (int e = 0; e < evals; e++) {
            query.append("\n| EVAL str=CONCAT(")
                .append(IntStream.range(0, 10).mapToObj(i -> "str").collect(Collectors.joining(", ")))
                .append(")");
        }
        query.append("\"}");
        return query(query.toString(), null);
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyConcat() throws IOException {
        int strings = 300;
        initManyLongs();
        assertManyStrings(manyConcat("FROM manylongs", strings), strings);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyConcat() throws IOException {
        initManyLongs();
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
     * Tests that generate many moderately long strings.
     */
    private Map<String, Object> manyConcat(String init, int strings) throws IOException {
        StringBuilder query = startQuery();
        query.append(init).append(" | EVAL str = CONCAT(");
        query.append(
            Arrays.stream(new String[] { "a", "b", "c", "d", "e" })
                .map(f -> "TO_STRING(" + f + ")")
                .collect(Collectors.joining(", \\\"  \\\", "))
        );
        query.append(")\n| EVAL ");
        for (int s = 0; s < strings; s++) {
            if (s != 0) {
                query.append(",");
            }
            query.append("\nstr")
                .append(s)
                .append("=CONCAT(")
                .append(IntStream.range(0, 30).mapToObj(i -> "str").collect(Collectors.joining(", ")))
                .append(")");
        }
        query.append("\n|KEEP ");
        for (int s = 0; s < strings; s++) {
            if (s != 0) {
                query.append(", ");
            }
            query.append("str").append(s);
        }
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyRepeat() throws IOException {
        int strings = 30;
        initManyLongs();
        assertManyStrings(manyRepeat("FROM manylongs", strings), 30);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyRepeat() throws IOException {
        initManyLongs();
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
     * Tests that generate many moderately long strings.
     */
    private Map<String, Object> manyRepeat(String init, int strings) throws IOException {
        StringBuilder query = startQuery();
        query.append(init).append(" | EVAL str = TO_STRING(a)");
        for (int s = 0; s < strings; s++) {
            query.append(",\nstr").append(s).append("=REPEAT(str, 10000)");
        }
        query.append("\n|KEEP ");
        for (int s = 0; s < strings; s++) {
            if (s != 0) {
                query.append(", ");
            }
            query.append("str").append(s);
        }
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private void assertManyStrings(Map<String, Object> resp, int strings) throws IOException {
        ListMatcher columns = matchesList();
        for (int s = 0; s < strings; s++) {
            columns = columns.item(matchesMap().entry("name", "str" + s).entry("type", "keyword"));
        }
        MapMatcher mapMatcher = matchesMap();
        assertMap(resp, mapMatcher.entry("columns", columns));
    }

    public void testManyEval() throws IOException {
        initManyLongs();
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
        initManyLongs();
        // 490 is plenty to fail on most nodes
        assertCircuitBreaks(attempt -> manyEval(attempt * 490));
    }

    private Map<String, Object> manyEval(int evalLines) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manylongs");
        for (int e = 0; e < evalLines; e++) {
            query.append("\n| EVAL ");
            for (int i = 0; i < 20; i++) {
                if (i != 0) {
                    query.append(", ");
                }
                query.append("i").append(e).append(i).append(" = ").append(e * 10 + i).append(" + a + b");
            }
        }
        query.append("\n| LIMIT 10000\"}");
        return responseAsMap(query(query.toString(), null));
    }

    private Response query(String query, String filterPath) throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("error_trace", "");
        if (filterPath != null) {
            request.addParameter("filter_path", filterPath);
        }
        request.setJsonEntity(query.replace("\n", "\\n"));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(6).millis())).build())
                .setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        return runQuery(() -> client().performRequest(request));
    }

    private Response runQuery(CheckedSupplier<Response, IOException> run) throws IOException {
        logger.info("--> test {} started querying", getTestName());
        final ThreadPool testThreadPool = new TestThreadPool(getTestName());
        final long startedTimeInNanos = System.nanoTime();
        Scheduler.Cancellable schedule = null;
        try {
            schedule = testThreadPool.schedule(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() throws Exception {
                    SUITE_ABORTED = true;
                    TimeValue elapsed = TimeValue.timeValueNanos(System.nanoTime() - startedTimeInNanos);
                    logger.info("--> test {} triggering OOM after {}", getTestName(), elapsed);
                    Request triggerOOM = new Request("POST", "/_trigger_out_of_memory");
                    client().performRequest(triggerOOM);
                }
            }, TimeValue.timeValueMinutes(5), testThreadPool.executor(ThreadPool.Names.GENERIC));
            Response resp = run.get();
            logger.info("--> test {} completed querying", getTestName());
            return resp;
        } finally {
            if (schedule != null) {
                schedule.cancel();
            }
            terminate(testThreadPool);
        }
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        settings = Settings.builder().put(settings).put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "6m").build();
        return super.buildClient(settings, hosts);
    }

    public void testFetchManyBigFields() throws IOException {
        initManyBigFieldsIndex(100);
        Map<?, ?> response = fetchManyBigFields(100);
        ListMatcher columns = matchesList();
        for (int f = 0; f < 1000; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testFetchTooManyBigFields() throws IOException {
        initManyBigFieldsIndex(500);
        // 500 docs is plenty to circuit break on most nodes
        assertCircuitBreaks(attempt -> fetchManyBigFields(attempt * 500));
    }

    /**
     * Fetches documents containing 1000 fields which are {@code 1kb} each.
     */
    private Map<String, Object> fetchManyBigFields(int docs) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | SORT f000 | LIMIT " + docs + "\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    public void testAggMvLongs() throws IOException {
        int fieldValues = 100;
        initMvLongsIndex(1, 3, fieldValues);
        Map<?, ?> response = aggMvLongs(3);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(f00)").entry("type", "long"))
            .item(matchesMap().entry("name", "f00").entry("type", "long"))
            .item(matchesMap().entry("name", "f01").entry("type", "long"))
            .item(matchesMap().entry("name", "f02").entry("type", "long"));
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testAggTooManyMvLongs() throws IOException {
        initMvLongsIndex(1, 3, 1000);
        // 3 fields is plenty on most nodes
        assertCircuitBreaks(attempt -> aggMvLongs(attempt * 3));
    }

    private Map<String, Object> aggMvLongs(int fields) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs | STATS MAX(f00) BY f00");
        for (int f = 1; f < fields; f++) {
            query.append(", f").append(String.format(Locale.ROOT, "%02d", f));
        }
        return responseAsMap(query(query.append("\"}").toString(), "columns"));
    }

    public void testFetchMvLongs() throws IOException {
        int fields = 100;
        initMvLongsIndex(100, fields, 1000);
        Map<?, ?> response = fetchMvLongs();
        ListMatcher columns = matchesList();
        for (int f = 0; f < fields; f++) {
            columns = columns.item(matchesMap().entry("name", String.format(Locale.ROOT, "f%02d", f)).entry("type", "long"));
        }
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testFetchTooManyMvLongs() throws IOException {
        initMvLongsIndex(500, 100, 1000);
        assertCircuitBreaks(attempt -> fetchMvLongs());
    }

    private Map<String, Object> fetchMvLongs() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs\"}");
        return responseAsMap(query(query.toString(), "columns"));
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
        int sensorDataCount = 7000;
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

    private void initManyLongs() throws IOException {
        logger.info("loading many documents with longs");
        StringBuilder bulk = new StringBuilder();
        for (int a = 0; a < 10; a++) {
            for (int b = 0; b < 10; b++) {
                for (int c = 0; c < 10; c++) {
                    for (int d = 0; d < 10; d++) {
                        for (int e = 0; e < 10; e++) {
                            bulk.append(String.format(Locale.ROOT, """
                                {"create":{}}
                                {"a":%d,"b":%d,"c":%d,"d":%d,"e":%d}
                                """, a, b, c, d, e));
                        }
                    }
                }
            }
            bulk("manylongs", bulk.toString());
            bulk.setLength(0);
        }
        initIndex("manylongs", bulk.toString());
    }

    private void initSingleDocIndex() throws IOException {
        logger.info("loading a single document");
        initIndex("single", """
            {"create":{}}
            {"a":1}
            """);
    }

    private void initManyBigFieldsIndex(int docs) throws IOException {
        logger.info("loading many documents with many big fields");
        int docsPerBulk = 5;
        int fields = 1000;
        int fieldSize = Math.toIntExact(ByteSizeValue.ofKb(1).getBytes());

        Request request = new Request("PUT", "/manybigfields");
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        config.startObject("settings").field("index.mapping.total_fields.limit", 10000).endObject();
        config.startObject("mappings").startObject("properties");
        for (int f = 0; f < fields; f++) {
            config.startObject("f" + String.format(Locale.ROOT, "%03d", f)).field("type", "keyword").endObject();
        }
        config.endObject().endObject();
        request.setJsonEntity(Strings.toString(config.endObject()));
        Response response = client().performRequest(request);
        assertThat(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            equalTo("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"manybigfields\"}")
        );

        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            for (int f = 0; f < fields; f++) {
                if (f == 0) {
                    bulk.append('{');
                } else {
                    bulk.append(", ");
                }
                bulk.append('"').append("f").append(String.format(Locale.ROOT, "%03d", f)).append("\": \"");
                bulk.append(Integer.toString(f % 10).repeat(fieldSize));
                bulk.append('"');
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk("manybigfields", bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex("manybigfields", bulk.toString());
    }

    private void initMvLongsIndex(int docs, int fields, int fieldValues) throws IOException {
        logger.info("loading documents with many multivalued longs");
        int docsPerBulk = 100;

        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            for (int f = 0; f < fields; f++) {
                if (f == 0) {
                    bulk.append('{');
                } else {
                    bulk.append(", ");
                }
                bulk.append('"').append("f").append(String.format(Locale.ROOT, "%02d", f)).append("\": ");
                for (int fv = 0; fv < fieldValues; fv++) {
                    if (fv == 0) {
                        bulk.append('[');
                    } else {
                        bulk.append(", ");
                    }
                    bulk.append(f + fv);
                }
                bulk.append(']');
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk("mv_longs", bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex("mv_longs", bulk.toString());
    }

    private void initSensorData(int docCount, int sensorCount) throws IOException {
        logger.info("loading sensor data");
        createIndex("sensor_data", Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build(), """
            {
                "properties": {
                    "@timestamp": { "type": "date" },
                    "id": { "type": "long" },
                    "value": { "type": "double" }
                }
            }""");
        int docsPerBulk = 1000;
        long firstDate = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-01-01T00:00:00Z");

        StringBuilder data = new StringBuilder();
        for (int i = 0; i < docCount; i++) {
            data.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"timestamp":"%s", "id": %d, "value": %f}
                """, DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(i * 10L + firstDate), i % sensorCount, i * 1.1));
            if (i % docsPerBulk == docsPerBulk - 1) {
                bulk("sensor_data", data.toString());
                data.setLength(0);
            }
        }
        initIndex("sensor_data", data.toString());
    }

    private void initSensorLookup(int lookupEntries, int sensorCount, IntFunction<String> location) throws IOException {
        logger.info("loading sensor lookup");
        createIndex("sensor_lookup", Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build(), """
            {
                "properties": {
                    "id": { "type": "long" },
                    "location": { "type": "geo_point" }
                }
            }""");
        int docsPerBulk = 1000;
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < lookupEntries; i++) {
            int sensor = i % sensorCount;
            data.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"id": %d, "location": "POINT(%s)"}
                """, sensor, location.apply(sensor)));
            if (i % docsPerBulk == docsPerBulk - 1) {
                bulk("sensor_lookup", data.toString());
                data.setLength(0);
            }
        }
        initIndex("sensor_lookup", data.toString());
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

    private void bulk(String name, String bulk) throws IOException {
        Request request = new Request("POST", "/" + name + "/_bulk");
        request.setJsonEntity(bulk);
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(5).millis())).build())
        );
        Response response = client().performRequest(request);
        assertThat(entityAsMap(response), matchesMap().entry("errors", false).extraOk());
    }

    private void initIndex(String name, String bulk) throws IOException {
        if (indexExists(name) == false) {
            // not strictly required, but this can help isolate failure from bulk indexing.
            createIndex(name);
            var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(name).get(name)).get("settings");
            if (settings.containsKey(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey()) == false) {
                updateIndexSettings(name, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
            }
        }
        if (hasText(bulk)) {
            bulk(name, bulk);
        }
        Request request = new Request("POST", "/" + name + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        RequestOptions.Builder requestOptions = RequestOptions.DEFAULT.toBuilder()
            .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(5).millis())).build());
        request.setOptions(requestOptions);
        Response response = client().performRequest(request);
        assertWriteResponse(response);

        request = new Request("POST", "/" + name + "/_refresh");
        response = client().performRequest(request);
        request.setOptions(requestOptions);
        assertWriteResponse(response);
    }

    @SuppressWarnings("unchecked")
    private static void assertWriteResponse(Response response) throws IOException {
        Map<String, Object> shards = (Map<String, Object>) entityAsMap(response).get("_shards");
        assertThat((int) shards.get("successful"), greaterThanOrEqualTo(1));
        assertThat(shards.get("failed"), equalTo(0));
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        if (SUITE_ABORTED) {
            return;
        }
        assertBusy(() -> {
            Response response = adminClient().performRequest(new Request("GET", "/_nodes/stats"));
            Map<?, ?> stats = responseAsMap(response);
            Map<?, ?> nodes = (Map<?, ?>) stats.get("nodes");
            for (Object n : nodes.values()) {
                Map<?, ?> node = (Map<?, ?>) n;
                Map<?, ?> breakers = (Map<?, ?>) node.get("breakers");
                Map<?, ?> request = (Map<?, ?>) breakers.get("request");
                assertMap(request, matchesMap().extraOk().entry("estimated_size_in_bytes", 0).entry("estimated_size", "0b"));
            }
        });
    }

    private static StringBuilder startQuery() {
        StringBuilder query = new StringBuilder();
        query.append("{\"query\":\"");
        return query;
    }
}
