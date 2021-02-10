/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.analytics;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

public class AnalyticsAggsIT extends ESRestHighLevelClientTestCase {
    public void testStringStats() throws IOException {
        BulkRequest bulk = new BulkRequest("test").setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        bulk.add(new IndexRequest().source(XContentType.JSON, "message", "trying out elasticsearch"));
        bulk.add(new IndexRequest().source(XContentType.JSON, "message", "more words"));
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);
        SearchRequest search = new SearchRequest("test");
        search.source().aggregation(new StringStatsAggregationBuilder("test").field("message.keyword").showDistribution(true));
        SearchResponse response = highLevelClient().search(search, RequestOptions.DEFAULT);
        ParsedStringStats stats = response.getAggregations().get("test");
        assertThat(stats.getCount(), equalTo(2L));
        assertThat(stats.getMinLength(), equalTo(10));
        assertThat(stats.getMaxLength(), equalTo(24));
        assertThat(stats.getAvgLength(), equalTo(17.0));
        assertThat(stats.getEntropy(), closeTo(4, .1));
        assertThat(stats.getDistribution(), aMapWithSize(18));
        assertThat(stats.getDistribution(), hasEntry(equalTo("o"), closeTo(.09, .005)));
        assertThat(stats.getDistribution(), hasEntry(equalTo("r"), closeTo(.12, .005)));
        assertThat(stats.getDistribution(), hasEntry(equalTo("t"), closeTo(.09, .005)));
    }

    public void testTopMetricsDoubleMetric() throws IOException {
        indexTopMetricsDoubleTestData();
        SearchRequest search = new SearchRequest("test");
        search.source().aggregation(new TopMetricsAggregationBuilder(
                "test", new FieldSortBuilder("s").order(SortOrder.DESC), 1, "v"));
        SearchResponse response = highLevelClient().search(search, RequestOptions.DEFAULT);
        ParsedTopMetrics top = response.getAggregations().get("test");
        assertThat(top.getTopMetrics(), hasSize(1));
        ParsedTopMetrics.TopMetrics metric = top.getTopMetrics().get(0);
        assertThat(metric.getSort(), equalTo(singletonList(2)));
        assertThat(metric.getMetrics(), equalTo(singletonMap("v", 3.0)));
    }

    public void testTopMetricsLongMetric() throws IOException {
        indexTopMetricsLongTestData();
        SearchRequest search = new SearchRequest("test");
        search.source().aggregation(new TopMetricsAggregationBuilder(
                "test", new FieldSortBuilder("s").order(SortOrder.DESC), 1, "v"));
        SearchResponse response = highLevelClient().search(search, RequestOptions.DEFAULT);
        ParsedTopMetrics top = response.getAggregations().get("test");
        assertThat(top.getTopMetrics(), hasSize(1));
        ParsedTopMetrics.TopMetrics metric = top.getTopMetrics().get(0);
        assertThat(metric.getSort(), equalTo(singletonList(2)));
        assertThat(metric.getMetrics(), equalTo(singletonMap("v", 3)));
    }

    public void testTopMetricsDateMetric() throws IOException {
        indexTopMetricsDateTestData();
        SearchRequest search = new SearchRequest("test");
        search.source().aggregation(new TopMetricsAggregationBuilder(
                "test", new FieldSortBuilder("s").order(SortOrder.DESC), 1, "v"));
        SearchResponse response = highLevelClient().search(search, RequestOptions.DEFAULT);
        ParsedTopMetrics top = response.getAggregations().get("test");
        assertThat(top.getTopMetrics(), hasSize(1));
        ParsedTopMetrics.TopMetrics metric = top.getTopMetrics().get(0);
        assertThat(metric.getSort(), equalTo(singletonList(2)));
        assertThat(metric.getMetrics(), equalTo(singletonMap("v", "2020-01-02T01:01:00.000Z")));
    }

    public void testTopMetricsManyMetrics() throws IOException {
        indexTopMetricsDoubleTestData();
        SearchRequest search = new SearchRequest("test");
        search.source().aggregation(new TopMetricsAggregationBuilder(
                "test", new FieldSortBuilder("s").order(SortOrder.DESC), 1, "v", "m"));
        SearchResponse response = highLevelClient().search(search, RequestOptions.DEFAULT);
        ParsedTopMetrics top = response.getAggregations().get("test");
        assertThat(top.getTopMetrics(), hasSize(1));
        ParsedTopMetrics.TopMetrics metric = top.getTopMetrics().get(0);
        assertThat(metric.getSort(), equalTo(singletonList(2)));
        assertThat(metric.getMetrics(), hasEntry("v", 3.0));
        assertThat(metric.getMetrics(), hasEntry("m", 13.0));
    }

    public void testTopMetricsSizeTwo() throws IOException {
        indexTopMetricsDoubleTestData();
        SearchRequest search = new SearchRequest("test");
        search.source().aggregation(new TopMetricsAggregationBuilder(
                "test", new FieldSortBuilder("s").order(SortOrder.DESC), 2, "v"));
        SearchResponse response = highLevelClient().search(search, RequestOptions.DEFAULT);
        ParsedTopMetrics top = response.getAggregations().get("test");
        assertThat(top.getTopMetrics(), hasSize(2));
        ParsedTopMetrics.TopMetrics metric = top.getTopMetrics().get(0);
        assertThat(metric.getSort(), equalTo(singletonList(2)));
        assertThat(metric.getMetrics(), equalTo(singletonMap("v", 3.0)));
        metric = top.getTopMetrics().get(1);
        assertThat(metric.getSort(), equalTo(singletonList(1)));
        assertThat(metric.getMetrics(), equalTo(singletonMap("v", 2.0)));
    }

    private void indexTopMetricsDoubleTestData() throws IOException {
        BulkRequest bulk = new BulkRequest("test").setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        bulk.add(new IndexRequest().source(XContentType.JSON, "s", 1, "v", 2.0, "m", 12.0));
        bulk.add(new IndexRequest().source(XContentType.JSON, "s", 2, "v", 3.0, "m", 13.0));
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);
    }

    private void indexTopMetricsLongTestData() throws IOException {
        BulkRequest bulk = new BulkRequest("test").setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        bulk.add(new IndexRequest().source(XContentType.JSON, "s", 1, "v", 2));
        bulk.add(new IndexRequest().source(XContentType.JSON, "s", 2, "v", 3));
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);
    }

    private void indexTopMetricsDateTestData() throws IOException {
        CreateIndexRequest create = new CreateIndexRequest("test");
        create.mapping("{\"properties\": {\"v\": {\"type\": \"date\"}}}", XContentType.JSON);
        highLevelClient().indices().create(create, RequestOptions.DEFAULT);
        BulkRequest bulk = new BulkRequest("test").setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        bulk.add(new IndexRequest().source(XContentType.JSON, "s", 1, "v", "2020-01-01T01:01:00Z"));
        bulk.add(new IndexRequest().source(XContentType.JSON, "s", 2, "v", "2020-01-02T01:01:00Z"));
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);
    }

}
