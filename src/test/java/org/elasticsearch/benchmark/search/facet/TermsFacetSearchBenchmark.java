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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.collect.Lists;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.util.List;
import java.util.Random;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;
import static org.elasticsearch.search.facet.FacetBuilders.termsStatsFacet;

/**
 *
 */
public class TermsFacetSearchBenchmark {

    static long COUNT = SizeValue.parseSizeValue("2m").singles();
    static int BATCH = 100;
    static int QUERY_WARMUP = 20;
    static int QUERY_COUNT = 200;
    static int NUMBER_OF_TERMS = 200;
    static int NUMBER_OF_MULTI_VALUE_TERMS = 10;
    static int STRING_TERM_SIZE = 5;

    static Client client;

    public static void main(String[] args) throws Exception {
        Random random = new Random();

        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put("gateway.type", "local")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = TermsFacetSearchBenchmark.class.getSimpleName();
        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().clusterName(clusterName)
                    .settings(settingsBuilder().put(settings).put("name", "node" + i))
                    .node();
        }

        Node clientNode = nodeBuilder()
                .clusterName(clusterName)
                .settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();

        client = clientNode.client();

        long[] lValues = new long[NUMBER_OF_TERMS];
        for (int i = 0; i < NUMBER_OF_TERMS; i++) {
            lValues[i] = ThreadLocalRandom.current().nextLong();
        }
        String[] sValues = new String[NUMBER_OF_TERMS];
        for (int i = 0; i < NUMBER_OF_TERMS; i++) {
            sValues[i] = RandomStrings.randomAsciiOfLength(random, STRING_TERM_SIZE);
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
                    builder.field("s_value", sValues[counter % sValues.length]);
                    builder.field("l_value", lValues[counter % lValues.length]);

                    builder.startArray("sm_value");
                    for (int k = 0; k < NUMBER_OF_MULTI_VALUE_TERMS; k++) {
                        builder.value(sValues[ThreadLocalRandom.current().nextInt(sValues.length)]);
                    }
                    builder.endArray();

                    builder.startArray("lm_value");
                    for (int k = 0; k < NUMBER_OF_MULTI_VALUE_TERMS; k++) {
                        builder.value(lValues[ThreadLocalRandom.current().nextInt(sValues.length)]);
                    }
                    builder.endArray();

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


        List<StatsResult> stats = Lists.newArrayList();
        stats.add(terms("terms_s", "s_value", null));
        stats.add(terms("terms_map_s", "s_value", "map"));
        stats.add(terms("terms_l", "l_value", null));
        stats.add(terms("terms_map_l", "l_value", "map"));
        stats.add(terms("terms_sm", "sm_value", null));
        stats.add(terms("terms_map_sm", "sm_value", "map"));
        stats.add(terms("terms_lm", "lm_value", null));
        stats.add(terms("terms_map_lm", "lm_value", "map"));

        stats.add(termsStats("terms_stats_s_l", "s_value", "l_value", null));
        stats.add(termsStats("terms_stats_s_lm", "s_value", "lm_value", null));
        stats.add(termsStats("terms_stats_sm_l", "sm_value", "l_value", null));

        System.out.println("------------------ SUMMARY -------------------------------");
        System.out.format("%25s%10s%10s\n", "name", "took", "millis");
        for (StatsResult stat : stats) {
            System.out.format("%25s%10s%10d\n", stat.name, TimeValue.timeValueMillis(stat.took), (stat.took / QUERY_COUNT));
        }
        System.out.println("------------------ SUMMARY -------------------------------");

        clientNode.close();

        for (Node node : nodes) {
            node.close();
        }
    }

    static class StatsResult {
        final String name;
        final long took;

        StatsResult(String name, long took) {
            this.name = name;
            this.took = took;
        }
    }

    private static StatsResult terms(String name, String field, String executionHint) {
        long totalQueryTime;// LM VALUE

        client.admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();

        System.out.println("--> Warmup (" + name + ")...");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet(field).field(field).executionHint(executionHint))
                    .execute().actionGet();
            if (j == 0) {
                System.out.println("--> Loading (" + field + "): took: " + searchResponse.getTook());
            }
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
        }
        System.out.println("--> Warmup (" + name + ") DONE");


        System.out.println("--> Running (" + name + ")...");
        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet(field).field(field).executionHint(executionHint))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Terms Facet (" + field + "), hint(" + executionHint + "): " + (totalQueryTime / QUERY_COUNT) + "ms");
        return new StatsResult(name, totalQueryTime);
    }

    private static StatsResult termsStats(String name, String keyField, String valueField, String executionHint) {
        long totalQueryTime;

        client.admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();

        System.out.println("--> Warmup (" + name + ")...");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setQuery(matchAllQuery())
                    .addFacet(termsStatsFacet(name).keyField(keyField).valueField(valueField))
                    .execute().actionGet();
            if (j == 0) {
                System.out.println("--> Loading (" + name + "): took: " + searchResponse.getTook());
            }
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
        }
        System.out.println("--> Warmup (" + name + ") DONE");


        System.out.println("--> Running (" + name + ")...");
        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setQuery(matchAllQuery())
                    .addFacet(termsStatsFacet(name).keyField(keyField).valueField(valueField))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Terms Facet (" + name + "), hint(" + executionHint + "): " + (totalQueryTime / QUERY_COUNT) + "ms");
        return new StatsResult(name, totalQueryTime);
    }
}
