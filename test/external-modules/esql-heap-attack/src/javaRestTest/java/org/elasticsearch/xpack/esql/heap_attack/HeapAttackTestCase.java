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
import org.junit.BeforeClass;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Base class for heap attack tests. Holds the test cluster, the REST and circuit-breaker plumbing and the index
 * population helpers shared by the heap attack test classes.
 */
public abstract class HeapAttackTestCase extends ESRestTestCase {

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

    static final int MAX_ATTEMPTS = 5;

    interface TryCircuitBreaking {
        Map<String, Object> attempt(int attempt) throws IOException;
    }

    void assertCircuitBreaks(TryCircuitBreaking tryBreaking) throws IOException {
        assertCircuitBreaks(
            tryBreaking,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }

    void assertFoldCircuitBreaks(TryCircuitBreaking tryBreaking) throws IOException {
        assertCircuitBreaks(
            tryBreaking,
            matchesMap().entry("status", 400).entry("error", matchesMap().extraOk().entry("type", "fold_too_much_memory_exception"))
        );
    }

    void assertCircuitBreaks(TryCircuitBreaking tryBreaking, MapMatcher responseMatcher) throws IOException {
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

    void assertParseFailure(ThrowingRunnable r) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, r);
        Map<?, ?> map = responseAsMap(e.getResponse());
        logger.info("expected parse failure {}", map);
        assertMap(map, matchesMap().entry("status", 400).entry("error", matchesMap().extraOk().entry("type", "parsing_exception")));
    }

    Map<String, Object> sortByManyLongs(int count) throws IOException {
        logger.info("sorting by {} longs", count);
        return responseAsMap(query(makeSortByManyLongs(count).toString(), null));
    }

    StringBuilder makeSortByManyLongs(int count) {
        StringBuilder query = makeManyLongs(count);
        query.append("| SORT a, b, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| KEEP a, b | LIMIT 10000\"}");
        return query;
    }

    Map<String, Object> sortBySomeLongsLimit(int count) throws IOException {
        logger.info("sorting by 5 longs, keeping {}", count);
        return responseAsMap(query(makeSortBySomeLongsLimit(count), null));
    }

    String makeSortBySomeLongsLimit(int count) {
        StringBuilder query = new StringBuilder("{\"query\": \"FROM manylongs\n");
        query.append("| SORT a, b, c, d, e\n");
        query.append("| LIMIT ").append(count).append("\n");
        query.append("| STATS MAX(a)\n");
        query.append("\"}");
        return query.toString();
    }

    Response groupOnManyLongs(int count) throws IOException {
        logger.info("grouping on {} longs", count);
        StringBuilder query = makeManyLongs(count);
        query.append("| STATS MIN(a) BY a, b, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| STATS MAX(a)\"}");
        return query(query.toString(), null);
    }

    StringBuilder makeManyLongs(int count) {
        StringBuilder query = startQuery();
        query.append("FROM manylongs\\n| EVAL i0 = a + b, i1 = b + i0");
        for (int i = 2; i < count; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + ").append(i - 1);
        }
        return query.append("\\n");
    }

    Response concat(int evals) throws IOException {
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
     * Tests that generate many moderately long strings.
     */
    Map<String, Object> manyConcat(String init, int strings) throws IOException {
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
     * Tests that generate many moderately long strings.
     */
    Map<String, Object> manyRepeat(String init, int strings) throws IOException {
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

    void assertManyStrings(Map<String, Object> resp, int strings) throws IOException {
        ListMatcher columns = matchesList();
        for (int s = 0; s < strings; s++) {
            columns = columns.item(matchesMap().entry("name", "str" + s).entry("type", "keyword"));
        }
        MapMatcher mapMatcher = matchesMap();
        assertMap(resp, mapMatcher.entry("columns", columns));
    }

    Map<String, Object> mvConcat(int delimiterLength) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs | EVAL str = MV_CONCAT(TO_STRING(f00), REPEAT(\\\"x\\\", ")
            .append(delimiterLength)
            .append(")) | EVAL len = LENGTH(str) | KEEP len\"}");
        return responseAsMap(query(query.toString(), null));
    }

    void setRequestBreakerLimit(String limit) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{\"persistent\": {\"indices.breaker.request.limit\": " + (limit == null ? "null" : "\"" + limit + "\"") + "}}"
        );
        adminClient().performRequest(request);
    }

    Map<String, Object> toBase64Chain(int levels) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM bigtext | EVAL b = ");
        query.append("TO_BASE64(".repeat(levels));
        query.append("f");
        query.append(")".repeat(levels));
        query.append(" | EVAL len = LENGTH(b) | KEEP len\"}");
        return responseAsMap(query(query.toString(), null));
    }

    Map<String, Object> manyEval(int evalLines) throws IOException {
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

    Response query(String query, String filterPath) throws IOException {
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

    Response runQuery(CheckedSupplier<Response, IOException> run) throws IOException {
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

    /**
     * Fetches documents containing 1000 fields which are {@code 1kb} each.
     */
    Map<String, Object> fetchManyBigFields(int docs) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | SORT f000 | LIMIT " + docs + "\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    /**
     * Aggregates documents containing many fields which are {@code 1kb} each.
     */
    Map<String, Object> aggManyBigFields(int fields) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | STATS sum = SUM(");
        query.append("LENGTH(f").append(String.format(Locale.ROOT, "%03d", 0)).append(")");
        for (int f = 1; f < fields; f++) {
            query.append(" + LENGTH(f").append(String.format(Locale.ROOT, "%03d", f)).append(")");
        }
        query.append(")\"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    /**
     * Aggregates documents containing a text field that is {@code 1mb} each.
     */
    Map<String, Object> aggGiantTextField() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM bigtext | STATS sum = SUM(LENGTH(f))\"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    Map<String, Object> aggMvLongs(int fields) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs | STATS MAX(f00) BY f00");
        for (int f = 1; f < fields; f++) {
            query.append(", f").append(String.format(Locale.ROOT, "%02d", f));
        }
        return responseAsMap(query(query.append("\"}").toString(), "columns"));
    }

    Map<String, Object> fetchMvLongs() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    void initManyLongs(int countPerLong) throws IOException {
        logger.info("loading many documents with longs");
        StringBuilder bulk = new StringBuilder();
        int flush = 0;
        for (int a = 0; a < countPerLong; a++) {
            for (int b = 0; b < countPerLong; b++) {
                for (int c = 0; c < countPerLong; c++) {
                    for (int d = 0; d < countPerLong; d++) {
                        for (int e = 0; e < countPerLong; e++) {
                            bulk.append(String.format(Locale.ROOT, """
                                {"create":{}}
                                {"a":%d,"b":%d,"c":%d,"d":%d,"e":%d}
                                """, a, b, c, d, e));
                            flush++;
                            if (flush % 10_000 == 0) {
                                bulk("manylongs", bulk.toString());
                                bulk.setLength(0);
                                logger.info(
                                    "flushing {}/{} to manylongs",
                                    flush,
                                    countPerLong * countPerLong * countPerLong * countPerLong * countPerLong
                                );

                            }
                        }
                    }
                }
            }
        }
        initIndex("manylongs", bulk.toString());
    }

    void initSingleDocIndex() throws IOException {
        logger.info("loading a single document");
        initIndex("single", """
            {"create":{}}
            {"a":1}
            """);
    }

    void initManyBigFieldsIndex(int docs, String type) throws IOException {
        logger.info("loading many documents with many big fields");
        int docsPerBulk = 5;
        int fields = 1000;
        int fieldSize = Math.toIntExact(ByteSizeValue.ofKb(1).getBytes());

        Request request = new Request("PUT", "/manybigfields");
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        config.startObject("settings").field("index.mapping.total_fields.limit", 10000).endObject();
        config.startObject("mappings").startObject("properties");
        for (int f = 0; f < fields; f++) {
            config.startObject("f" + String.format(Locale.ROOT, "%03d", f)).field("type", type).endObject();
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

    void initGiantTextField(int docs, boolean includeId, long fieldSizeInMb) throws IOException {
        int docsPerBulk = 10;
        for (Map<?, ?> nodeInfo : getNodesInfo(adminClient()).values()) {
            for (Object module : (List<?>) nodeInfo.get("modules")) {
                Map<?, ?> moduleInfo = (Map<?, ?>) module;
                final String moduleName = moduleInfo.get("name").toString();
                if (moduleName.startsWith("serverless-")) {
                    docsPerBulk = 3;
                }
            }
        }
        logger.info("loading many documents with one big text field - docs per bulk {}", docsPerBulk);
        int fieldSize = Math.toIntExact(ByteSizeValue.ofMb(fieldSizeInMb).getBytes());

        Request request = new Request("PUT", "/bigtext");
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        config.startObject("mappings").startObject("properties");
        if (includeId) {
            config.startObject("id").field("type", "long").endObject();
        }
        config.startObject("f").field("type", "text").endObject();
        config.endObject().endObject();
        request.setJsonEntity(Strings.toString(config.endObject()));
        Response response = client().performRequest(request);
        assertThat(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            equalTo("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"bigtext\"}")
        );

        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            if (includeId) {
                bulk.append(String.format(Locale.ROOT, "{\"id\":\"%s\", \"f\":\"", d));
            } else {
                bulk.append("{\"f\":\"");
            }
            bulk.append(Integer.toString(d % 10).repeat(fieldSize));
            bulk.append("\"}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk("bigtext", bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex("bigtext", bulk.toString());
    }

    void initMvLongsIndex(int docs, int fields, int fieldValues, boolean includeId) throws IOException {
        logger.info("loading documents with many multivalued longs");
        int docsPerBulk = 100;

        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            for (int f = 0; f < fields; f++) {
                if (f == 0) {
                    bulk.append('{');
                    if (includeId) {
                        String idField = String.format(Locale.ROOT, "\"id\": %s, ", d);
                        bulk.append(idField);
                    }
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

    void initSensorData(int docCount, int sensorCount) throws IOException {
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

    void initSensorLookup(int lookupEntries, int sensorCount, IntFunction<String> location) throws IOException {
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

    void bulk(String name, String bulk) throws IOException {
        Request request = new Request("POST", "/" + name + "/_bulk");
        request.setJsonEntity(bulk);
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(5).millis())).build())
        );
        Response response = client().performRequest(request);
        assertThat(entityAsMap(response), matchesMap().entry("errors", false).extraOk());

        /*
         * Flush after each bulk to clear the test-time seenSequenceNumbers Map in
         * TranslogWriter. Without this the server will OOM from time to time keeping
         * stuff around to run assertions on.
         */
        request = new Request("POST", "/" + name + "/_flush");
        response = client().performRequest(request);
        assertThat(entityAsMap(response), matchesMap().entry("_shards", matchesMap().extraOk().entry("failed", 0)).extraOk());
    }

    void initIndex(String name, String bulk) throws IOException {
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
    static void assertWriteResponse(Response response) throws IOException {
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

    static StringBuilder startQuery() {
        StringBuilder query = new StringBuilder();
        query.append("{\"query\":\"");
        return query;
    }

    /**
     * {@link #SUITE_ABORTED} is static, so it is shared by every heap attack test class run in the same JVM. Each class
     * starts its own cluster, so a class only skips its own tests after an abort, not those of the classes after it.
     */
    @BeforeClass
    public static void resetSuiteAborted() {
        SUITE_ABORTED = false;
    }
}
