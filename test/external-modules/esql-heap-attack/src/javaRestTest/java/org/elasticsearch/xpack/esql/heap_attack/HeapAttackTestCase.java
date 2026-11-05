/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
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
import static org.hamcrest.Matchers.isA;

/**
 * Base class for heap attack tests against Lucene-backed indices populated via the bulk API.
 * Index-population helpers and the standard heap-attack cluster live here; lower-level REST
 * plumbing (query, runQuery, circuit-breaker assertions, breaker-empty probe) is inherited from
 * {@link HeapAttackRestHelpers} so it can be shared with the EXTERNAL heap-attack suite which
 * needs a different cluster topology.
 */
public abstract class HeapAttackTestCase extends HeapAttackRestHelpers {
    private static final long ATTACKS_SIZED_FOR_HEAP_BYTES = ByteSizeValue.ofMb(512).getBytes();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.buildCluster();

    /**
     * Scales {@code size}, chosen to trip the request breaker on a 512mb heap, up to the smallest heap in the cluster under test.
     * Serverless runs the suite on larger nodes, where an unscaled attack fits under the breaker and the query succeeds. Never
     * returns less than {@code size}.
     */
    protected final int scaleToHeap(int size) throws IOException {
        long heap = smallestNodeHeapBytes();
        int scaled = Math.toIntExact(size * heap / ATTACKS_SIZED_FOR_HEAP_BYTES);
        logger.info("scaled attack size {} to {} for a {} byte heap", size, scaled, heap);
        return Math.max(size, scaled);
    }

    /**
     * The smallest max heap of any node in the cluster under test. Read from the cluster rather than
     * {@link Clusters#HEAP_SIZE_IN_MB}: that constant is inlined when the tests are compiled, so the {@link Clusters} that
     * serverless substitutes would not change it.
     */
    protected final long smallestNodeHeapBytes() throws IOException {
        Response response = adminClient().performRequest(new Request("GET", "/_nodes/stats?filter_path=nodes.*.jvm.mem.heap_max_in_bytes"));
        Map<?, ?> nodes = (Map<?, ?>) responseAsMap(response).get("nodes");
        long minHeapMax = Long.MAX_VALUE;
        for (Object node : nodes.values()) {
            Map<?, ?> mem = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) node).get("jvm")).get("mem");
            minHeapMax = Math.min(minHeapMax, ((Number) mem.get("heap_max_in_bytes")).longValue());
        }
        return minHeapMax;
    }

    @Override
    protected String getTestRestCluster() {
        return Clusters.testRestCluster(cluster);
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

    void assertFoldCircuitBreaks(TryCircuitBreaking tryBreaking) throws IOException {
        assertCircuitBreaks(
            tryBreaking,
            matchesMap().entry("status", 400).entry("error", matchesMap().extraOk().entry("type", "fold_too_much_memory_exception"))
        );
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

    Map<String, Object> jsonString(int evals) throws IOException {
        StringBuilder query = startQuery();
        query.append("ROW field = TO_STRING(42)");
        query.repeat(" | EVAL field = JSON_STRING(field, field)", evals);
        query.append("\"}");
        return responseAsMap(query(query.toString(), null));
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

    Map<String, Object> fetchMvLongs(int docs) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs | LIMIT ").append(docs).append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    void queryAndVerifyDuplicatedHistograms(String index, String column, int numDuplications) throws IOException {
        Map<String, Object> responseMap = queryDuplicatedHistograms(index, column, numDuplications);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "dummy").entry("type", "double"));
        ListMatcher values = matchesList(List.of(matchesList(List.of(isA(Double.class)))));
        assertResultMap(responseMap, columns, values);
    }

    /**
     * Creates a query which loads each histogram n-times into memory.
     * We do this by querying n percentiles on each histogram, each with a different filter condition which however never is false.
     * PERCENTILES() is implemented using a surrogate histogram merge, which means at the end of the STATS we
     * have n copies of each histogram in memory.
     */
    Map<String, Object> queryDuplicatedHistograms(String index, String column, int numDuplications) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM " + index);
        query.append("| STATS ");
        query.append(
            IntStream.range(0, numDuplications)
                .mapToObj(i -> "val_" + i + " = PERCENTILE(" + column + ", 50) WHERE histo_id != -" + i)
                .collect(Collectors.joining(", "))
        );
        query.append("BY histo_id");
        // in the end aggregate it all to a single sum to not blow up the result set
        query.append("| EVAL vals_sum = ");
        query.append(IntStream.range(0, numDuplications).mapToObj(i -> "val_" + i).collect(Collectors.joining(" + ")));
        query.append("| STATS dummy = SUM(vals_sum)\"}");
        String queryStr = query.toString().replace("\n", "\\n");
        return responseAsMap(query(queryStr, null));
    }

    void initManyBigFieldsIndex(int docs, String type, boolean random, int fields) throws IOException {
        logger.info("loading many documents with many big fields");
        int docsPerBulk = 5;
        int fieldSize = Math.toIntExact(ByteSizeValue.ofKb(1).getBytes());
        boolean numeric = type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("long") || type.equalsIgnoreCase("double");

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
                bulk.append('"').append("f").append(String.format(Locale.ROOT, "%03d", f)).append("\": ");
                if (numeric) {
                    bulk.append(randomNumericValue(type));
                } else {
                    bulk.append('"');
                    bulk.append(random ? randomAlphaOfLength(1024) : Integer.toString(f % 10).repeat(fieldSize));
                    bulk.append('"');
                }
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk("manybigfields", bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex("manybigfields", bulk.toString());
    }

    static String randomNumericValue(String type) {
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "integer" -> Integer.toString(randomInt());
            case "long" -> Long.toString(randomLong());
            case "double" -> Double.toString(randomDouble());
            default -> throw new IllegalArgumentException("unsupported numeric type: " + type);
        };
    }

    void initGiantTextField(int docs, boolean includeId, long fieldSizeInMb) throws IOException {
        initGiantTextField(docs, includeId, fieldSizeInMb, false);
    }

    void initGiantTextField(int docs, boolean includeId, long fieldSizeInMb, boolean distinct) throws IOException {
        int docsPerBulk = isServerless() ? 3 : 10;
        logger.info(
            "loading {} documents with one {} {}MB text field - docs per bulk {}",
            docs,
            distinct ? "distinct" : "big",
            fieldSizeInMb,
            docsPerBulk
        );

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
                String s = String.format(Locale.ROOT, "{\"id\":\"%s\", \"f\":\"", d);
                bulk.append(s);
            } else {
                bulk.append("{\"f\":\"");
            }
            if (distinct) {
                String prefix = String.format(Locale.ROOT, "%010d", d);
                bulk.append(prefix).append("0".repeat(fieldSize - prefix.length()));
            } else {
                bulk.append(Integer.toString(d % 10).repeat(fieldSize));
            }
            bulk.append("\"}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk("bigtext", bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex("bigtext", bulk.toString());
        logger.info("loaded");
    }

    Map<String, Object> valuesFromMany(boolean keyword, int countPer, int echoFactor) throws IOException {
        String query = """
            FROM manylongs
            | EVAL v = a + b * $b + c * $c + d * $d + e * $e
            """;
        query = query.replace("$b", Integer.toString(countPer));
        query = query.replace("$c", Integer.toString(countPer * countPer));
        query = query.replace("$d", Integer.toString(countPer * countPer * countPer));
        query = query.replace("$e", Integer.toString(countPer * countPer * countPer * countPer));
        StringBuilder q = new StringBuilder(query);
        if (keyword) {
            q.append("| EVAL v = v::KEYWORD\n");
        }
        q.append("| EVAL vv = v\n");
        if (keyword) {
            String echo = "a";
            for (int i = 0; i < echoFactor; i++) {
                q.append("| EVAL vv = MV_UNION(MV_UNION(vv, CONCAT(\\\"").append(echo).append("\\\", v)");
                echo += "a";
                q.append("), MV_UNION(CONCAT(\\\"").append(echo).append("\\\", v)");
                echo += "a";
                q.append(", CONCAT(\\\"").append(echo).append("\\\", v)))\n");
                echo += "a";
            }
        } else {
            long echoSize = ((long) countPer) * countPer * countPer * countPer * countPer;
            long echo = echoSize;
            for (int i = 0; i < echoFactor; i++) {
                q.append("| EVAL vv = MV_UNION(MV_UNION(vv, v + ").append(echo);
                echo += echoSize;
                q.append("), MV_UNION(v + ").append(echo);
                echo += echoSize;
                q.append(", v + ").append(echo).append("))\n");
                echo += echoSize;
            }
        }
        return responseAsMap(query("{\"query\": \"" + q + "| STATS VALUES(vv)\"}", "columns, values"));
    }

    Map<String, Object> aggregateByIdOnLargeText(String aggregation) throws IOException {
        StringBuilder query = new StringBuilder("{\"query\": \"FROM bigtext\n");
        // Grouping on the unique id field generates a large number of buckets where each bucket's value contains the
        // entire set of multivalues.
        String aggClause = "| STATS x = %s(f, id) BY id\n";
        query.append(String.format(Locale.ROOT, aggClause, aggregation));
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    Map<String, Object> aggregateByIdOnManyLongs(String aggregation) throws IOException {
        StringBuilder query = new StringBuilder("{\"query\": \"FROM mv_longs\n");
        // Grouping on the unique id field generates a large number of buckets where each bucket's value contains the
        // entire set of multivalues.
        query.append(String.format(Locale.ROOT, "| STATS x = %s(f00, f01) BY id\n", aggregation));
        query.append("\"}");

        return responseAsMap(query(query.toString(), "columns"));
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
                        String idField = String.format("\"id\": %s, ", d);
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

    void initManyExponentialHistograms(int numHistograms, int numBucketsPerHistogram) throws IOException {
        logger.info("loading many documents with exponential histograms");

        createIndex("many_exponential_histograms", Settings.EMPTY, """
            {
                "properties": {
                  "histo": {
                    "type": "exponential_histogram"
                  },
                  "histo_id": {
                    "type": "long"
                  }
                }
            }
            """);

        StringBuilder bulk = new StringBuilder();
        int flush = 0;
        for (int i = 0; i < numHistograms; i++) {

            // The scale doesn't actually matter here
            ExponentialHistogramBuilder builder = ExponentialHistogram.builder(10, ExponentialHistogramCircuitBreaker.noop());
            for (int j = 0; j < numBucketsPerHistogram; j++) {
                builder.setPositiveBucket(i + j, 1 + i + j * 2);
            }
            String histoJson;
            try (XContentBuilder xContentBuilder = JsonXContent.contentBuilder()) {
                ExponentialHistogramXContent.serialize(xContentBuilder, builder.build());
                histoJson = Strings.toString(xContentBuilder);
            }

            bulk.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"histo_id":%d,"histo":%s}
                """, i + 1, histoJson));
            flush++;
            if (flush % 10_000 == 0) {
                bulk("many_exponential_histograms", bulk.toString());
                bulk.setLength(0);
                logger.info("flushing {}/{} to many_exponential_histograms", flush, numHistograms);
            }
        }
        // Load the remaining data and also do a force merge
        initIndex("many_exponential_histograms", bulk.toString());
    }

    enum TDigestFieldType {
        TDIGEST,
        HISTOGRAM
    }

    Map<String, Object> fetchBigText(String esqlQuery) throws IOException {
        StringBuilder query = startQuery();
        query.append(esqlQuery).append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    void initManyTDigests(int numHistograms, int numCentroidsPerHistogram, TDigestFieldType fieldType) throws IOException {
        logger.info("loading many documents with tdigests");

        createIndex("many_tdigests", Settings.EMPTY, """
            {
                "properties": {
                  "histo": {
                    "type": "%s"
                  },
                  "histo_id": {
                    "type": "long"
                  }
                }
            }
            """.formatted(fieldType == TDigestFieldType.TDIGEST ? "tdigest" : "histogram"));

        StringBuilder bulk = new StringBuilder();
        int flush = 0;
        String centroidsFieldName = fieldType == TDigestFieldType.TDIGEST ? "centroids" : "values";
        for (int i = 0; i < numHistograms; i++) {
            StringBuilder histoJson = new StringBuilder("{");
            histoJson.append("\"").append(centroidsFieldName).append("\":");
            histoJson.append(
                IntStream.range(i, i + numCentroidsPerHistogram).mapToObj(Integer::toString).collect(Collectors.joining(",", "[", "]"))
            );
            histoJson.append(",\"counts\":");
            int finalI = i;
            histoJson.append(
                IntStream.range(0, numCentroidsPerHistogram)
                    .map(j -> 1 + finalI + (j * 2))
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining(",", "[", "]"))
            );
            histoJson.append("}");

            bulk.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"histo_id":%d,"histo":%s}
                """, i + 1, histoJson));
            flush++;
            if (flush % 10_000 == 0) {
                bulk("many_tdigests", bulk.toString());
                bulk.setLength(0);
                logger.info("flushing {}/{} to many_tdigests", flush, numHistograms);
            }
        }
        // Load the remaining data and also do a force merge
        initIndex("many_tdigests", bulk.toString());
    }
}
