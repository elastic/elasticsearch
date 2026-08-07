/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;

import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Base class for heap attack tests against Lucene-backed indices populated via the bulk API.
 * Index-population helpers and the standard heap-attack cluster live here; lower-level REST
 * plumbing (query, runQuery, circuit-breaker assertions, breaker-empty probe) is inherited from
 * {@link HeapAttackRestHelpers} so it can be shared with the EXTERNAL heap-attack suite which
 * needs a different cluster topology.
 */
public abstract class HeapAttackTestCase extends HeapAttackRestHelpers {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.buildCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
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

    /**
     * Loads {@code countPerLong^5} documents into the {@code manylongs} index with fields
     * {@code a, b, c, d, e ∈ [0, countPerLong-1]}, one document per unique combination.
     */
    protected void initManyLongs(int countPerLong) throws IOException {
        logger.info("loading many documents with longs");
        StringBuilder bulk = new StringBuilder();
        int flush = 0;
        long numLongs = (long) countPerLong * countPerLong * countPerLong * countPerLong * countPerLong;
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
                                logger.info("flushing {}/{} to manylongs", flush, numLongs);
                            }
                        }
                    }
                }
            }
        }
        initIndex("manylongs", bulk.toString());
    }

    /**
     * Like {@link #initManyLongs} but also adds a keyword field {@code f} containing a string
     * of the given length. This produces wide group keys without needing many EVAL columns.
     */
    protected void initManyLongsAndString(int countPerLong, int stringLength) throws IOException {
        logger.info("loading many documents with longs and a {}-char string", stringLength);
        String f = "x".repeat(stringLength);
        StringBuilder bulk = new StringBuilder();
        int flush = 0;
        long numDocs = (long) countPerLong * countPerLong * countPerLong * countPerLong * countPerLong;
        for (int a = 0; a < countPerLong; a++) {
            for (int b = 0; b < countPerLong; b++) {
                for (int c = 0; c < countPerLong; c++) {
                    for (int d = 0; d < countPerLong; d++) {
                        for (int e = 0; e < countPerLong; e++) {
                            bulk.append(String.format(Locale.ROOT, """
                                {"create":{}}
                                {"a":%d,"b":%d,"c":%d,"d":%d,"e":%d,"f":"%s"}
                                """, a, b, c, d, e, f));
                            flush++;
                            if (flush % 10_000 == 0) {
                                bulk("manylongsandstring", bulk.toString());
                                bulk.setLength(0);
                                logger.info("flushing {}/{} to manylongsandstring", flush, numDocs);
                            }
                        }
                    }
                }
            }
        }
        initIndex("manylongsandstring", bulk.toString());
    }

    /**
     * Builds a query preamble that EVALs {@code count} computed long columns
     * ({@code i0, i1, ..., i(count-1)}) as running sums over {@code a} and {@code b}.
     */
    protected static StringBuilder makeManyLongs(int count) {
        StringBuilder query = startQuery();
        query.append("FROM manylongs\\n| EVAL i0 = a + b, i1 = b + i0");
        for (int i = 2; i < count; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + ").append(i - 1);
        }
        return query.append("\\n");
    }

    protected void initSingleDocIndex() throws IOException {
        logger.info("loading a single document");
        initIndex("single", """
            {"create":{}}
            {"a":1}
            """);
    }
}
