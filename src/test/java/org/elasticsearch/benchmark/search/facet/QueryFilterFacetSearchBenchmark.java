/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.benchmark.search.facet;

import jsr166y.ThreadLocalRandom;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.FacetBuilders;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class QueryFilterFacetSearchBenchmark {

    static long COUNT = SizeValue.parseSizeValue("1m").singles();
    static int BATCH = 100;
    static int QUERY_COUNT = 200;
    static int NUMBER_OF_TERMS = 200;

    static Client client;

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put("gateway.type", "local")
                .put(SETTING_NUMBER_OF_SHARDS, 2)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = QueryFilterFacetSearchBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder()
                .clusterName(clusterName)
                .settings(settingsBuilder().put(settings).put("name", "node1")).node();
        client = node1.client();

        long[] lValues = new long[NUMBER_OF_TERMS];
        for (int i = 0; i < NUMBER_OF_TERMS; i++) {
            lValues[i] = ThreadLocalRandom.current().nextLong();
        }

        Thread.sleep(10000);
        try {
            client.admin().indices().create(createIndexRequest("test")).actionGet();

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + COUNT + "] ...");
            long ITERS = COUNT / BATCH;
            long i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;

                    XContentBuilder builder = jsonBuilder().startObject();
                    builder.field("id", Integer.toString(counter));
                    builder.field("l_value", lValues[counter % lValues.length]);

                    builder.endObject();

                    request.add(Requests.indexRequest("test").type("type1").id(Integer.toString(counter))
                            .source(builder));
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


        long totalQueryTime = 0;

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setQuery(termQuery("l_value", lValues[0]))
                    .execute().actionGet();
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("-->  Simple Query on first l_value " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setQuery(termQuery("l_value", lValues[0]))
                    .addFacet(FacetBuilders.queryFacet("query").query(termQuery("l_value", lValues[0])))
                    .execute().actionGet();
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("-->  Query facet first l_value " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setQuery(termQuery("l_value", lValues[0]))
                    .addFacet(FacetBuilders.queryFacet("query").query(termQuery("l_value", lValues[0])).global(true).mode(FacetBuilder.Mode.COLLECTOR))
                    .execute().actionGet();
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("-->  Query facet first l_value (global) (mode/collector) " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setQuery(termQuery("l_value", lValues[0]))
                    .addFacet(FacetBuilders.queryFacet("query").query(termQuery("l_value", lValues[0])).global(true).mode(FacetBuilder.Mode.POST))
                    .execute().actionGet();
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("-->  Query facet first l_value (global) (mode/post) " + (totalQueryTime / QUERY_COUNT) + "ms");
    }
}
