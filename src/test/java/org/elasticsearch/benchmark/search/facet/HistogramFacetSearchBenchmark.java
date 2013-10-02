/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.search.facet;

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
import org.elasticsearch.search.facet.FacetBuilder;

import java.util.Date;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.facet.FacetBuilders.dateHistogramFacet;
import static org.elasticsearch.search.facet.FacetBuilders.histogramFacet;

/**
 *
 */
public class HistogramFacetSearchBenchmark {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.engine.robin.refreshInterval", "-1")
                .put("gateway.type", "local")
                .put(SETTING_NUMBER_OF_SHARDS, 2)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        String clusterName = HistogramFacetSearchBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder()
                .clusterName(clusterName)
                .settings(settingsBuilder().put(settings).put("name", "node1")).node();

        //Node clientNode = nodeBuilder().clusterName(clusterName).settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();

        Client client = node1.client();

        long COUNT = SizeValue.parseSizeValue("20m").singles();
        int BATCH = 500;
        int QUERY_WARMUP = 20;
        int QUERY_COUNT = 200;
        int NUMBER_OF_TERMS = 1000;

        long[] lValues = new long[NUMBER_OF_TERMS];
        for (int i = 0; i < NUMBER_OF_TERMS; i++) {
            lValues[i] = i;
        }

        Thread.sleep(10000);
        try {
            client.admin().indices().prepareCreate("test")
                    .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                    .addMapping("test", jsonBuilder()
                        .startObject()
                            .startObject("test")
                                .startObject("properties")
                                    .startObject("name")
                                        .startObject("fields")
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
                                .endObject()
                            .endObject()
                        .endObject())
                    .execute().actionGet();

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + COUNT + "] ...");
            long ITERS = COUNT / BATCH;
            long i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;
                    final long value = lValues[counter % lValues.length];
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
            System.out.println("--> Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) (COUNT)) / stopWatch.totalTime().secondsFrac()));
        } catch (Exception e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        COUNT = client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount();
        System.out.println("--> Number of docs in index: " + COUNT);

        System.out.println("--> Warmup...");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(histogramFacet("l_value").field("l_value").interval(4))
                    .addFacet(histogramFacet("i_value").field("i_value").interval(4))
                    .addFacet(histogramFacet("s_value").field("s_value").interval(4))
                    .addFacet(histogramFacet("b_value").field("b_value").interval(4))
                    .addFacet(histogramFacet("date").field("date").interval(1000))
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
                        .addFacet(histogramFacet(field).field(field).interval(4))
                        .execute().actionGet();
                if (searchResponse.getHits().totalHits() != COUNT) {
                    System.err.println("--> mismatch on hits");
                }
                totalQueryTime += searchResponse.getTookInMillis();
            }
            System.out.println("--> Histogram Facet (" + field + ") " + (totalQueryTime / QUERY_COUNT) + "ms");

            totalQueryTime = 0;
            for (int j = 0; j < QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(matchAllQuery())
                        .addFacet(histogramFacet("l_value").field("l_value").valueField("l_value").interval(4))
                        .execute().actionGet();
                if (searchResponse.getHits().totalHits() != COUNT) {
                    System.err.println("--> mismatch on hits");
                }
                totalQueryTime += searchResponse.getTookInMillis();
            }
            System.out.println("--> Histogram Facet (" + field + "/" + field + ") " + (totalQueryTime / QUERY_COUNT) + "ms");
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(histogramFacet("date").field("date").interval(1000))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Histogram Facet (date) " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(histogramFacet("date").field("date").valueField("l_value").interval(1000))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Histogram Facet (date/l_value) " + (totalQueryTime / QUERY_COUNT) + "ms");


        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(dateHistogramFacet("date").field("date").interval("day").mode(FacetBuilder.Mode.COLLECTOR))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Date Histogram Facet (mode/collector) (date) " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(dateHistogramFacet("date").field("date").interval("day").mode(FacetBuilder.Mode.POST))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Date Histogram Facet (mode/post) (date) " + (totalQueryTime / QUERY_COUNT) + "ms");

        node1.close();
    }
}
