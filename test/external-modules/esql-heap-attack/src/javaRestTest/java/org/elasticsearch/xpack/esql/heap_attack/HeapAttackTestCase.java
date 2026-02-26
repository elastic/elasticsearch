/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;

import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Base class for heap attack tests. Contains common infrastructure and helper methods.
 */
public abstract class HeapAttackTestCase extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.buildCluster();

    protected static final int MAX_ATTEMPTS = 5;

    @Override
    protected boolean shouldFailureSkipRemainingTests() {
        /*
         * Failures will frequently poison the cluster being tested, causing the next
         * test to fail because the cluster has OOMed or exploded in some fun way. So
         * we skip them.
         */
        return true;
    }

    protected interface TryCircuitBreaking {
        Map<String, Object> attempt(int attempt) throws IOException;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected void assertCircuitBreaks(TryCircuitBreaking tryBreaking) throws IOException {
        assertCircuitBreaks(
            tryBreaking,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }

    protected void assertCircuitBreaks(TryCircuitBreaking tryBreaking, MapMatcher responseMatcher) throws IOException {
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

    protected Response query(String query, String filterPath) throws IOException {
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
        logger.info("Running query:" + query);
        return runQuery(() -> client().performRequest(request));
    }

    protected Response runQuery(CheckedSupplier<Response, IOException> run) throws IOException {
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

    protected void initSensorData(int docCount, int sensorCount, int joinFieldCount, boolean expressionBasedJoin) throws IOException {
        logger.info("loading sensor data");
        // We cannot go over 1000 fields, due to failed on parsing mappings on index creation
        // [sensor_data] java.lang.IllegalArgumentException: Limit of total fields [1000] has been exceeded
        assertTrue("Too many columns, it will throw an exception later", joinFieldCount <= 990);
        StringBuilder createIndexBuilder = new StringBuilder();
        createIndexBuilder.append("""
             {
                 "properties": {
                     "@timestamp": { "type": "date" },
            """);
        String suffix = expressionBasedJoin ? "_left" : "";
        for (int i = 0; i < joinFieldCount; i++) {
            createIndexBuilder.append("\"id").append(suffix).append(i).append("\": { \"type\": \"long\" },");
        }
        createIndexBuilder.append("""
                    "value": { "type": "double" }
                }
            }""");
        CreateIndexResponse response = createIndex(
            "sensor_data",
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build(),
            createIndexBuilder.toString()
        );
        assertTrue(response.isAcknowledged());
        int docsPerBulk = 1000;
        long firstDate = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-01-01T00:00:00Z");

        StringBuilder data = new StringBuilder();
        for (int i = 0; i < docCount; i++) {
            data.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"timestamp":"%s",""", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(i * 10L + firstDate)));
            for (int j = 0; j < joinFieldCount; j++) {
                data.append(String.format(Locale.ROOT, "\"id%s%d\":%d, ", suffix, j, i % sensorCount));
            }
            data.append(String.format(Locale.ROOT, "\"value\": %f}\n", i * 1.1));
            if (i % docsPerBulk == docsPerBulk - 1) {
                bulk("sensor_data", data.toString());
                data.setLength(0);
            }
        }
        initIndex("sensor_data", data.toString());
    }

    protected void initSensorLookup(
        int lookupEntries,
        int sensorCount,
        IntFunction<String> location,
        int joinFieldsCount,
        boolean expressionBasedJoin
    ) throws IOException {
        logger.info("loading sensor lookup");
        // cannot go over 1000 fields, due to failed on parsing mappings on index creation
        // [sensor_data] java.lang.IllegalArgumentException: Limit of total fields [1000] has been exceeded
        assertTrue("Too many join on fields, it will throw an exception later", joinFieldsCount <= 990);
        StringBuilder createIndexBuilder = new StringBuilder();
        createIndexBuilder.append("""
            {
                "properties": {
            """);
        String suffix = expressionBasedJoin ? "_right" : "";
        for (int i = 0; i < joinFieldsCount; i++) {
            createIndexBuilder.append("\"id").append(suffix).append(i).append("\": { \"type\": \"long\" },");
        }
        createIndexBuilder.append("""
                    "location": { "type": "geo_point" },
                    "filter_key": { "type": "integer" }
                }
            }""");
        CreateIndexResponse response = createIndex(
            "sensor_lookup",
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build(),
            createIndexBuilder.toString()
        );
        assertTrue(response.isAcknowledged());
        int docsPerBulk = 1000;
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < lookupEntries; i++) {
            int sensor = i % sensorCount;
            data.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"""));
            for (int j = 0; j < joinFieldsCount; j++) {
                data.append(String.format(Locale.ROOT, "\"id%s%d\":%d, ", suffix, j, sensor));
            }
            data.append(String.format(Locale.ROOT, """
                "location": "POINT(%s)", "filter_key": %d}\n""", location.apply(sensor), i));
            if (i % docsPerBulk == docsPerBulk - 1) {
                bulk("sensor_lookup", data.toString());
                data.setLength(0);
            }
        }
        initIndex("sensor_lookup", data.toString());
    }

    protected void bulk(String name, String bulk) throws IOException {
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

    protected void initIndex(String name, String bulk) throws IOException {
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
    protected static void assertWriteResponse(Response response) throws IOException {
        Map<String, Object> shards = (Map<String, Object>) entityAsMap(response).get("_shards");
        assertThat((int) shards.get("successful"), greaterThanOrEqualTo(1));
        assertThat(shards.get("failed"), equalTo(0));
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        if (previousFailureSkipsRemaining()) {
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

    protected static StringBuilder startQuery() {
        StringBuilder query = new StringBuilder();
        query.append("{\"query\":\"");
        return query;
    }

    protected static boolean isServerless() throws IOException {
        for (Map<?, ?> nodeInfo : getNodesInfo(adminClient()).values()) {
            for (Object module : (List<?>) nodeInfo.get("modules")) {
                Map<?, ?> moduleInfo = (Map<?, ?>) module;
                final String moduleName = moduleInfo.get("name").toString();
                if (moduleName.startsWith("serverless-")) {
                    return true;
                }
            }
        }
        return false;
    }
}
