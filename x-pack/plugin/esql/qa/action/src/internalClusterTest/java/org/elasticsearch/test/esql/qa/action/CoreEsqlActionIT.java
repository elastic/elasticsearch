/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.esql.qa.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;

// A subset of ES|QL test scenarios exercised through the xpack core
// transport request API (rather than through the ES|QL request API).
// Tests here have no static dependencies on types from the ES|QL plugin.
public class CoreEsqlActionIT extends ESIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex("test");
    }

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        try {
            @SuppressWarnings("unchecked")
            var c = (Class<? extends Plugin>) Class.forName("org.elasticsearch.xpack.esql.plugin.EsqlPlugin");
            return List.of(c);
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e); // the ES|QL plugin must be present
        }
    }

    public void testRowTypesAndValues() {
        var query = "row a = 1, b = \"x\", c = 1000000000000, d = 1.1";
        var request = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query);
        try (var resp = run(request)) {
            logger.info("response= " + resp);
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("a", "b", "c", "d"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "keyword", "long", "double"));
            assertThat(getValuesList(resp.values()), contains(List.of(1, "x", 1000000000000L, 1.1d)));
        }
    }

    public void testRowStatsProjectGroupByInt() {
        var query = "row a = 1, b = 2 | stats count(b) by a | keep a";
        var request = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query);
        try (var resp = run(request)) {
            logger.info("response= " + resp);
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer"));
            assertThat(getValuesList(resp.values()), contains(List.of(1)));
        }
    }

    public void testWithDate() {
        var query = "from test | keep count, color";
        var req1 = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).columnar(false);
        var req2 = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).columnar(true);
        try (var resp1 = req1.execute().actionGet(30, SECONDS);
             var resp2 = req2.execute().actionGet(30, SECONDS)) {
            logger.info("response1= " + resp1);
            logger.info("response2= " + resp2);
            assertThat(resp1.columns().stream().map(ColumnInfo::name).toList(), contains("count", "color"));
            assertThat(resp1.columns().stream().map(ColumnInfo::type).toList(), contains("long", "keyword"));
            assertThat(getValuesList(resp1.values()), contains(List.of(1, "x", 1000000000000L, 1.1d)));
        }
    }

    static List<List<Object>> getValuesList(Iterator<Iterator<Object>> values) {
        var valuesList = new ArrayList<List<Object>>();
        values.forEachRemaining(row -> {
            var rowValues = new ArrayList<>();
            row.forEachRemaining(rowValues::add);
            valuesList.add(rowValues);
        });
        return valuesList;
    }

    protected EsqlQueryResponse run(EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request) {
        try {
            if (randomBoolean()) {
                return request.execute().actionGet(30, SECONDS);
            } else {
                return ClientHelper.executeWithHeaders(
                        Map.of("Foo", "bar"),
                        "origin", client(),
                        () -> request.execute().actionGet(30, SECONDS));
            }
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    private void createAndPopulateIndex(String indexName) {
        var client = client().admin().indices();
        var request = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("count", "type=long", "cost", "type=double", "color", "type=keyword", "hire_date", "type=date");
        assertAcked(request);
        for (int i = 0; i < 1; i++) {
            client().prepareBulk()
                .add(new IndexRequest(indexName).id("1" + i)
                   .source("count", 1, "cost", 1d, "color", "red", "hire_date", "1953-09-02T00:00:00Z")
                )
                .add(new IndexRequest(indexName).id("2" + i)
                   .source("count", 2, "cost", 2d, "color", "blue", "hire_date", "1420070400001")
                )
                .add(new IndexRequest(indexName).id("3" + i)
                   .source("count", 3, "cost", 1d, "color", "green", "hire_date", "2023-04-03T00:00:00Z")
                )
                .add(new IndexRequest(indexName).id("4" + i)
                    .source("count", 4, "cost", 2d, "color", "red", "hire_date", "5420070400001")
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        ensureYellow(indexName);
    }
}
