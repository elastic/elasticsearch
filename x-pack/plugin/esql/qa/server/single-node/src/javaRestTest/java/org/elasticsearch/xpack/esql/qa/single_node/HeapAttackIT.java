/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests that run ESQL queries that have, in the past, used so much memory they
 * crash Elasticsearch.
 */
public class HeapAttackIT extends ESRestTestCase {
    /**
     * This used to fail, but we've since compacted top n so it actually succeeds now.
     */
    public void testSortByManyLongsSuccess() throws IOException {
        initManyLongs();
        Response response = sortByManyLongs(2000);
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "b").entry("type", "long"));
        ListMatcher values = matchesList();
        for (int b = 0; b < 10; b++) {
            for (int i = 0; i < 1000; i++) {
                values = values.item(List.of(0, b));
            }
        }
        assertMap(map, matchesMap().entry("columns", columns).entry("values", values));
    }

    /**
     * This used to crash the node with an out of memory, but now it just trips a circuit breaker.
     */
    public void testSortByManyLongsTooMuchMemory() throws IOException {
        initManyLongs();
        assertCircuitBreaks(() -> sortByManyLongs(5000));
    }

    private void assertCircuitBreaks(ThrowingRunnable r) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, r);
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(e.getResponse().getEntity()), false);
        logger.info("expected circuit breaker {}", map);
        assertMap(
            map,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }

    private Response sortByManyLongs(int count) throws IOException {
        logger.info("sorting by {} longs", count);
        StringBuilder query = makeManyLongs(count);
        query.append("| SORT a, b, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| KEEP a, b | LIMIT 10000\"}");
        return query(query.toString(), null);
    }

    /**
     * This groups on about 200 columns which is a lot but has never caused us trouble.
     */
    public void testGroupOnSomeLongs() throws IOException {
        initManyLongs();
        Map<?, ?> map = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            EntityUtils.toString(groupOnManyLongs(200).getEntity()),
            false
        );
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertMap(map, matchesMap().entry("columns", columns).entry("values", values));
    }

    /**
     * This groups on 5000 columns which used to throw a {@link StackOverflowError}.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100640")
    public void testGroupOnManyLongs() throws IOException {
        initManyLongs();
        Map<?, ?> map = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            EntityUtils.toString(groupOnManyLongs(5000).getEntity()),
            false
        );
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertMap(map, matchesMap().entry("columns", columns).entry("values", values));
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
        StringBuilder query = new StringBuilder();
        query.append("{\"query\":\"FROM manylongs\\n| EVAL i0 = a + b, i1 = b + i0");
        for (int i = 2; i < count; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + ").append(i - 1);
        }
        return query.append("\\n");
    }

    public void testSmallConcat() throws IOException {
        initSingleDocIndex();
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(concat(2).getEntity()), false);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "str").entry("type", "keyword"));
        ListMatcher values = matchesList().item(List.of(1, "1".repeat(100)));
        assertMap(map, matchesMap().entry("columns", columns).entry("values", values));
    }

    public void testHugeConcat() throws IOException {
        initSingleDocIndex();
        ResponseException e = expectThrows(ResponseException.class, () -> concat(10));
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(e.getResponse().getEntity()), false);
        logger.info("expected request rejected {}", map);
        assertMap(
            map,
            matchesMap().entry("status", 400)
                .entry("error", matchesMap().extraOk().entry("reason", "concatenating more than [1048576] bytes is not supported"))
        );
    }

    private Response concat(int evals) throws IOException {
        StringBuilder query = new StringBuilder();
        query.append("{\"query\":\"FROM single | EVAL str = TO_STRING(a)");
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
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100678")
    public void testManyConcat() throws IOException {
        initManyLongs();
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(manyConcat(300).getEntity()), false);
        ListMatcher columns = matchesList();
        for (int s = 0; s < 300; s++) {
            columns = columns.item(matchesMap().entry("name", "str" + s).entry("type", "keyword"));
        }
        assertMap(map, matchesMap().entry("columns", columns).entry("values", any(List.class)));
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyConcat() throws IOException {
        initManyLongs();
        assertCircuitBreaks(() -> manyConcat(2000));
    }

    /**
     * Tests that generate many moderately long strings.
     */
    private Response manyConcat(int strings) throws IOException {
        StringBuilder query = new StringBuilder();
        query.append("{\"query\":\"FROM manylongs | EVAL str = CONCAT(");
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
        return query(query.toString(), null);
    }

    public void testManyEval() throws IOException {
        initManyLongs();
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(manyEval(1).getEntity()), false);
        ListMatcher columns = matchesList();
        columns = columns.item(matchesMap().entry("name", "a").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "b").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "c").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "d").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "e").entry("type", "long"));
        for (int i = 0; i < 10; i++) {
            columns = columns.item(matchesMap().entry("name", "i0" + i).entry("type", "long"));
        }
        assertMap(map, matchesMap().entry("columns", columns).entry("values", hasSize(10_000)));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100528")
    public void testTooManyEval() throws IOException {
        initManyLongs();
        assertCircuitBreaks(() -> manyEval(1000));
    }

    private Response manyEval(int evalLines) throws IOException {
        StringBuilder query = new StringBuilder();
        query.append("{\"query\":\"FROM manylongs");
        for (int e = 0; e < evalLines; e++) {
            query.append("\n| EVAL ");
            for (int i = 0; i < 10; i++) {
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
        request.setJsonEntity(query.toString().replace("\n", "\\n"));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(5).millis())).build())
                .setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        return client().performRequest(request);
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
        Response response = query("{\"query\": \"FROM manybigfields | SORT f000 | LIMIT " + docs + "\"}", "columns");
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
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
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
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
        StringBuilder builder = new StringBuilder("{\"query\": \"FROM mv_longs | STATS MAX(f00) BY f00");
        for (int f = 1; f < fields; f++) {
            builder.append(", f").append(String.format(Locale.ROOT, "%02d", f));
        }
        return query(builder.append("\"}").toString(), "columns");
    }

    public void testFetchMvLongs() throws IOException {
        int fields = 100;
        initMvLongsIndex(100, fields, 1000);
        Response response = fetchMvLongs();
        Map<?, ?> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        ListMatcher columns = matchesList();
        for (int f = 0; f < fields; f++) {
            columns = columns.item(matchesMap().entry("name", String.format(Locale.ROOT, "f%02d", f)).entry("type", "long"));
        }
        assertMap(map, matchesMap().entry("columns", columns));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100528")
    public void testFetchTooManyMvLongs() throws IOException {
        initMvLongsIndex(500, 100, 1000);
        assertCircuitBreaks(() -> fetchMvLongs());
    }

    private Response fetchMvLongs() throws IOException {
        return query("{\"query\": \"FROM mv_longs\"}", "columns");
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
        request.addParameter("filter_path", "errors");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertThat(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), equalTo("{\"errors\":false}"));
    }

    private void initIndex(String name, String bulk) throws IOException {
        bulk(name, bulk);

        Request request = new Request("POST", "/" + name + "/_refresh");
        Response response = client().performRequest(request);
        assertThat(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            equalTo("{\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0}}")
        );

        request = new Request("POST", "/" + name + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        response = client().performRequest(request);
        assertThat(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            equalTo("{\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0}}")
        );

        request = new Request("POST", "/" + name + "/_refresh");
        response = client().performRequest(request);
        assertThat(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            equalTo("{\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0}}")
        );
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }
}
