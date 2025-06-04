/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.esql.qa.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;

// A subset of test scenarios exercised through the xpack core ES|QL
// transport API (rather than through the ES|QL request API).
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
        try (EsqlQueryResponse queryResp = run(request)) {
            logger.info("response=" + queryResp);
            EsqlResponse resp = queryResp.response();
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("a", "b", "c", "d"));
            assertThat(
                resp.columns().stream().map(c -> ((ColumnInfoImpl) c).type()).toList(),
                contains(DataType.INTEGER, DataType.KEYWORD, DataType.LONG, DataType.DOUBLE)
            );
            assertThat(getValuesList(resp.rows()), contains(List.of(1, "x", 1000000000000L, 1.1d)));
        }
    }

    public void testRowStatsProjectGroupByInt() {
        var query = "row a = 1, b = 2 | stats count(b) by a | keep a";
        var request = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query);
        try (var queryResp = run(request)) {
            logger.info("response=" + queryResp);
            var resp = queryResp.response();
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(resp.columns().stream().map(c -> ((ColumnInfoImpl) c).type()).toList(), contains(DataType.INTEGER));
            assertThat(getValuesList(resp.rows()), contains(List.of(1)));
        }
    }

    public void testFrom() {
        var query = "from test | keep item, cost, color, sale | sort item";
        var request = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query);
        try (var queryResp = run(request)) {
            var resp = queryResp.response();
            logger.info("response=" + queryResp);
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("item", "cost", "color", "sale"));
            assertThat(
                resp.columns().stream().map(c -> ((ColumnInfoImpl) c).type()).toList(),
                contains(DataType.LONG, DataType.DOUBLE, DataType.KEYWORD, DataType.DATETIME)
            );
            // columnar values
            assertThat(columnValues(resp.column(0)), contains(1L, 2L, 3L, 4L));
            assertThat(columnValues(resp.column(1)), contains(1.1d, 2.1d, 3.1d, 4.1d));
            assertThat(columnValues(resp.column(2)), contains("red", "blue", "green", "red"));
            var d = List.of("2004-03-02T00:00:00.000Z", "1992-06-01T00:00:00.000Z", "1965-06-01T00:00:00.000Z", "2000-03-15T00:00:00.000Z");
            assertThat(columnValues(resp.column(3)), contains(d.toArray()));
            // row values
            List<List<Object>> values = getValuesList(resp.rows());
            assertThat(values.get(0), contains(1L, 1.1d, "red", "2004-03-02T00:00:00.000Z"));
            assertThat(values.get(1), contains(2L, 2.1d, "blue", "1992-06-01T00:00:00.000Z"));
            assertThat(values.get(2), contains(3L, 3.1d, "green", "1965-06-01T00:00:00.000Z"));
            assertThat(values.get(3), contains(4L, 4.1d, "red", "2000-03-15T00:00:00.000Z"));
        }
    }

    public void testAccessAfterClose() {
        for (var closedQueryResp : new boolean[] { true, false }) {
            var query = "row a = 1";
            var request = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query);
            var queryResp = run(request);
            var resp = queryResp.response();
            var rows = resp.rows();
            var rowItr = rows.iterator();
            var cols = resp.column(0);
            var colItr = cols.iterator();

            // must close at least one of them
            if (closedQueryResp) queryResp.close();
            if (randomBoolean() || closedQueryResp == false) resp.close();

            assertThrows(IllegalStateException.class, resp::rows);
            assertThrows(IllegalStateException.class, () -> resp.column(0));
            assertThrows(IllegalStateException.class, () -> rows.iterator());
            assertThrows(IllegalStateException.class, () -> cols.iterator());
            assertThrows(IllegalStateException.class, () -> queryResp.response().rows());
            assertThrows(IllegalStateException.class, () -> queryResp.response().column(0));
            assertThrows(IllegalStateException.class, () -> rowItr.next().iterator().next());
            assertThrows(IllegalStateException.class, () -> colItr.next());
            if (closedQueryResp) {
                assertThrows(IllegalStateException.class, () -> queryResp.response());
            } else {
                queryResp.close(); // we must close the query response if not already closed
            }
        }
    }

    protected EsqlQueryResponse run(EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request) {
        try {
            // The variants here ensure API usage patterns
            if (randomBoolean()) {
                return request.execute().actionGet(30, SECONDS);
            } else if (randomBoolean()) {
                return client().execute(request.action(), request.request()).actionGet(30, SECONDS);
            } else {
                return ClientHelper.executeWithHeaders(
                    Map.of("Foo", "bar"),
                    "origin",
                    client(),
                    () -> request.execute().actionGet(30, SECONDS)
                );
            }
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    static List<List<Object>> getValuesList(Iterable<Iterable<Object>> values) {
        var valuesList = new ArrayList<List<Object>>();
        values.forEach(row -> {
            var rowValues = new ArrayList<>();
            row.forEach(rowValues::add);
            valuesList.add(rowValues);
        });
        return valuesList;
    }

    static List<Object> columnValues(Iterable<Object> values) {
        List<Object> l = new ArrayList<>();
        values.forEach(l::add);
        return l;
    }

    private void createAndPopulateIndex(String indexName) {
        var client = client().admin().indices();
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("item", "type=long", "cost", "type=double", "color", "type=keyword", "sale", "type=date");
        assertAcked(CreateRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("item", 1, "cost", 1.1d, "color", "red", "sale", "2004-03-02T00:00:00.000Z"))
            .add(new IndexRequest(indexName).id("2").source("item", 2, "cost", 2.1d, "color", "blue", "sale", "1992-06-01T00:00:00.000Z"))
            .add(new IndexRequest(indexName).id("3").source("item", 3, "cost", 3.1d, "color", "green", "sale", "1965-06-01T00:00:00.000Z"))
            .add(new IndexRequest(indexName).id("4").source("item", 4, "cost", 4.1d, "color", "red", "sale", "2000-03-15T00:00:00.000Z"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }
}
