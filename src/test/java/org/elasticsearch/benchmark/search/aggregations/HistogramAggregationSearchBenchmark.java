/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.search.aggregations;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.util.Date;
import java.util.Random;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

/**
 *
 */
public class HistogramAggregationSearchBenchmark {

    static final long COUNT = SizeValue.parseSizeValue("20m").singles();
    static final int BATCH = 1000;
    static final int QUERY_WARMUP = 5;
    static final int QUERY_COUNT = 20;
    static final int NUMBER_OF_TERMS = 1000;

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = HistogramAggregationSearchBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder()
                .clusterName(clusterName)
                .settings(settingsBuilder().put(settings).put("name", "node1")).node();

        //Node clientNode = nodeBuilder().clusterName(clusterName).settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();

        Client client = node1.client();

        long[] lValues = new long[NUMBER_OF_TERMS];
        for (int i = 0; i < NUMBER_OF_TERMS; i++) {
            lValues[i] = i;
        }

        Random r = new Random();
        try {
            client.admin().indices().prepareCreate("test")
                    .setSettings(settingsBuilder().put(settings))
                    .addMapping("type1", jsonBuilder()
                        .startObject()
                            .startObject("type1")
                                .startObject("properties")
                                    .startObject("l_value")
                                        .field("type", "long")
                                    .endObject()
                                    .startObject("i_value")
                                        .field("type", "integer")
                                    .endObject()
                                    .startObject("s_value")
                                        .field("type", "short")
                                    .endObject()
                                    .startObject("b_value")
                                        .field("type", "byte")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject())
                    .execute().actionGet();

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + COUNT + "] ...");
            long iters = COUNT / BATCH;
            long i = 1;
            int counter = 0;
            for (; i <= iters; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;
                    final long value = lValues[r.nextInt(lValues.length)];
                    XContentBuilder source = jsonBuilder().startObject()
                            .field("id", Integer.valueOf(counter))
                            .field("l_value", value)
                            .field("i_value", (int) value)
                            .field("s_value", (short) value)
                            .field("b_value", (byte) value)
                            .field("date", new Date())
                            .endObject();
                    request.add(Requests.indexRequest("test").type("type1").id(Integer.toString(counter))
                            .source(source));
                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("--> failures...");
                }
                if (((i * BATCH) % 10000) == 0) {
                    System.out.println("--> Indexed " + (i * BATCH) + " took " + stopWatch.stop().lastTaskTime());
                    stopWatch.start();
                }
            }
            client.admin().indices().prepareFlush("test").execute().actionGet();
            System.out.println("--> Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) (COUNT)) / stopWatch.totalTime().secondsFrac()));
        } catch (Exception e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        if (client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount() != COUNT) {
            throw new Error();
        }
        System.out.println("--> Number of docs in index: " + COUNT);

        System.out.println("--> Warmup...");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addAggregation(histogram("l_value").field("l_value").interval(4))
                    .addAggregation(histogram("i_value").field("i_value").interval(4))
                    .addAggregation(histogram("s_value").field("s_value").interval(4))
                    .addAggregation(histogram("b_value").field("b_value").interval(4))
                    .addAggregation(histogram("date").field("date").interval(1000))
                    .execute().actionGet();
            if (j == 0) {
                System.out.println("--> Warmup took: " + searchResponse.getTook());
            }
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
        }
        System.out.println("--> Warmup DONE");

        long totalQueryTime = 0;
        for (String field : new String[] {"b_value", "s_value", "i_value", "l_value"}) {
            totalQueryTime = 0;
            for (int j = 0; j < QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(matchAllQuery())
                        .addAggregation(histogram(field).field(field).interval(4))
                        .execute().actionGet();
                if (searchResponse.getHits().totalHits() != COUNT) {
                    System.err.println("--> mismatch on hits");
                }
                totalQueryTime += searchResponse.getTookInMillis();
            }
            System.out.println("--> Histogram Aggregation (" + field + ") " + (totalQueryTime / QUERY_COUNT) + "ms");

            totalQueryTime = 0;
            for (int j = 0; j < QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(matchAllQuery())
                        .addAggregation(histogram(field).field(field).subAggregation(stats(field).field(field)).interval(4))
                        .execute().actionGet();
                if (searchResponse.getHits().totalHits() != COUNT) {
                    System.err.println("--> mismatch on hits");
                }
                totalQueryTime += searchResponse.getTookInMillis();
            }
            System.out.println("--> Histogram Aggregation (" + field + "/" + field + ") " + (totalQueryTime / QUERY_COUNT) + "ms");
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addAggregation(dateHistogram("date").field("date").interval(1000))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Histogram Aggregation (date) " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addAggregation(dateHistogram("date").field("date").interval(1000).subAggregation(stats("stats").field("l_value")))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Histogram Aggregation (date/l_value) " + (totalQueryTime / QUERY_COUNT) + "ms");

        node1.close();
    }
}
