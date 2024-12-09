/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
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
 * Tests that run ESQL queries that have, in the past, used so much memory they
 * crash Elasticsearch.
 */
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
        Response response = sortByManyLongs(500);
        Map<?, ?> map = responseAsMap(response);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "b").entry("type", "long"));
        ListMatcher values = matchesList();
        for (int b = 0; b < 10; b++) {
            for (int i = 0; i < 1000; i++) {
                values = values.item(List.of(0, b));
            }
        }
        MapMatcher mapMatcher = matchesMap();
        assertMap(map, mapMatcher.entry("columns", columns).entry("values", values).entry("took", greaterThanOrEqualTo(0)));
    }

    /**
     * This used to crash the node with an out of memory, but now it just trips a circuit breaker.
     */
    public void testSortByManyLongsTooMuchMemory() throws IOException {
        initManyLongs();
        assertCircuitBreaks(() -> sortByManyLongs(5000));
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

    private void assertCircuitBreaks(ThrowingRunnable r) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, r);
        Map<?, ?> map = responseAsMap(e.getResponse());
        logger.info("expected circuit breaker {}", map);
        assertMap(
            map,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }

    private void assertParseFailure(ThrowingRunnable r) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, r);
        Map<?, ?> map = responseAsMap(e.getResponse());
        logger.info("expected parse failure {}", map);
        assertMap(map, matchesMap().entry("status", 400).entry("error", matchesMap().extraOk().entry("type", "parsing_exception")));
    }

    private Response sortByManyLongs(int count) throws IOException {
        logger.info("sorting by {} longs", count);
        return query(makeSortByManyLongs(count).toString(), null);
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
        Map<?, ?> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        MapMatcher mapMatcher = matchesMap();
        assertMap(map, mapMatcher.entry("columns", columns).entry("values", values).entry("took", greaterThanOrEqualTo(0)));
    }

    /**
     * This groups on 5000 columns which used to throw a {@link StackOverflowError}.
     */
    public void testGroupOnManyLongs() throws IOException {
        initManyLongs();
        Response resp = groupOnManyLongs(5000);
        Map<?, ?> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        MapMatcher mapMatcher = matchesMap();
        assertMap(map, mapMatcher.entry("columns", columns).entry("values", values).entry("took", greaterThanOrEqualTo(0)));
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
        Map<?, ?> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "str").entry("type", "keyword"));
        ListMatcher values = matchesList().item(List.of(1, "1".repeat(100)));
        MapMatcher mapMatcher = matchesMap();
        assertMap(map, mapMatcher.entry("columns", columns).entry("values", values).entry("took", greaterThanOrEqualTo(0)));
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
        Response resp = manyConcat("FROM manylongs", strings);
        assertManyStrings(resp, strings);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyConcat() throws IOException {
        initManyLongs();
        assertCircuitBreaks(() -> manyConcat("FROM manylongs", 2000));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyConcatFromRow() throws IOException {
        int strings = 2000;
        Response resp = manyConcat("ROW a=9999, b=9999, c=9999, d=9999, e=9999", strings);
        assertManyStrings(resp, strings);
    }

    /**
     * Fails to parse a huge huge query.
     */
    public void testHugeHugeManyConcatFromRow() throws IOException {
        assertParseFailure(() -> manyConcat("ROW a=9999, b=9999, c=9999, d=9999, e=9999", 50000));
    }

    /**
     * Tests that generate many moderately long strings.
     */
    private Response manyConcat(String init, int strings) throws IOException {
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
        return query(query.toString(), "columns");
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyRepeat() throws IOException {
        int strings = 30;
        initManyLongs();
        Response resp = manyRepeat("FROM manylongs", strings);
        assertManyStrings(resp, 30);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyRepeat() throws IOException {
        initManyLongs();
        assertCircuitBreaks(() -> manyRepeat("FROM manylongs", 75));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyRepeatFromRow() throws IOException {
        int strings = 10000;
        Response resp = manyRepeat("ROW a = 99", strings);
        assertManyStrings(resp, strings);
    }

    /**
     * Fails to parse a huge huge query.
     */
    public void testHugeHugeManyRepeatFromRow() throws IOException {
        assertParseFailure(() -> manyRepeat("ROW a = 99", 100000));
    }

    /**
     * Tests that generate many moderately long strings.
     */
    private Response manyRepeat(String init, int strings) throws IOException {
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
        return query(query.toString(), "columns");
    }

    private void assertManyStrings(Response resp, int strings) throws IOException {
        Map<?, ?> map = responseAsMap(resp);
        ListMatcher columns = matchesList();
        for (int s = 0; s < strings; s++) {
            columns = columns.item(matchesMap().entry("name", "str" + s).entry("type", "keyword"));
        }
        MapMatcher mapMatcher = matchesMap();
        assertMap(map, mapMatcher.entry("columns", columns));
    }

    public void testManyEval() throws IOException {
        initManyLongs();
        Response resp = manyEval(1);
        Map<?, ?> map = responseAsMap(resp);
        ListMatcher columns = matchesList();
        columns = columns.item(matchesMap().entry("name", "a").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "b").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "c").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "d").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "e").entry("type", "long"));
        for (int i = 0; i < 20; i++) {
            columns = columns.item(matchesMap().entry("name", "i0" + i).entry("type", "long"));
        }
        MapMatcher mapMatcher = matchesMap();
        assertMap(map, mapMatcher.entry("columns", columns).entry("values", hasSize(10_000)).entry("took", greaterThanOrEqualTo(0)));
    }

    public void testTooManyEval() throws IOException {
        initManyLongs();
        assertCircuitBreaks(() -> manyEval(490));
    }

    private Response manyEval(int evalLines) throws IOException {
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
        return query(query.toString(), null);
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
        fetchManyBigFields(100);
    }

    public void testFetchTooManyBigFields() throws IOException {
        initManyBigFieldsIndex(500);
        assertCircuitBreaks(() -> fetchManyBigFields(500));
    }

    /**
     * Fetches documents containing 1000 fields which are {@code 1kb} each.
     */
    private void fetchManyBigFields(int docs) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | SORT f000 | LIMIT " + docs + "\"}");
        Response response = query(query.toString(), "columns");
        Map<?, ?> map = responseAsMap(response);
        ListMatcher columns = matchesList();
        for (int f = 0; f < 1000; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        assertMap(map, matchesMap().entry("columns", columns));
    }

    public void testAggMvLongs() throws IOException {
        int fieldValues = 100;
        initMvLongsIndex(1, 3, fieldValues);
        Response response = aggMvLongs(3);
        Map<?, ?> map = responseAsMap(response);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(f00)").entry("type", "long"))
            .item(matchesMap().entry("name", "f00").entry("type", "long"))
            .item(matchesMap().entry("name", "f01").entry("type", "long"))
            .item(matchesMap().entry("name", "f02").entry("type", "long"));
        assertMap(map, matchesMap().entry("columns", columns));
    }

    public void testAggTooManyMvLongs() throws IOException {
        initMvLongsIndex(1, 3, 1000);
        assertCircuitBreaks(() -> aggMvLongs(3));
    }

    private Response aggMvLongs(int fields) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs | STATS MAX(f00) BY f00");
        for (int f = 1; f < fields; f++) {
            query.append(", f").append(String.format(Locale.ROOT, "%02d", f));
        }
        return query(query.append("\"}").toString(), "columns");
    }

    public void testFetchMvLongs() throws IOException {
        int fields = 100;
        initMvLongsIndex(100, fields, 1000);
        Response response = fetchMvLongs();
        Map<?, ?> map = responseAsMap(response);
        ListMatcher columns = matchesList();
        for (int f = 0; f < fields; f++) {
            columns = columns.item(matchesMap().entry("name", String.format(Locale.ROOT, "f%02d", f)).entry("type", "long"));
        }
        assertMap(map, matchesMap().entry("columns", columns));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/106683")
    public void testFetchTooManyMvLongs() throws IOException {
        initMvLongsIndex(500, 100, 1000);
        assertCircuitBreaks(() -> fetchMvLongs());
    }

    private Response fetchMvLongs() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs\"}");
        return query(query.toString(), "columns");
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
        logger.info("loading many documents with a single document");
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
