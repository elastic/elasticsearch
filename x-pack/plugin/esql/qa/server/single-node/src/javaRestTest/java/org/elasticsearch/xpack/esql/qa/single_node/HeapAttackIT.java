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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
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
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(query.toString());
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(5).millis())).build())
        );
        return client().performRequest(request);
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
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(query.toString());
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(5).millis())).build())
        );
        return client().performRequest(request);
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
        assertCircuitBreaks(() -> concat(10));
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
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(query.toString().replace("\n", "\\n"));
        return client().performRequest(request);
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/99826")
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
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(query.toString().replace("\n", "\\n"));
        return client().performRequest(request);
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

    private void initIndex(String name, String bulk) throws IOException {
        Request request = new Request("POST", "/" + name + "/_bulk");
        request.addParameter("refresh", "true");
        request.addParameter("filter_path", "errors");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertThat(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), equalTo("{\"errors\":false}"));

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
