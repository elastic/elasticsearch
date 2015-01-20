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
package org.elasticsearch.benchmark.search.child;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.Random;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.FilterBuilders.hasChildFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class ChildSearchAndIndexingBenchmark {

    static int PARENT_COUNT = (int) SizeValue.parseSizeValue("1m").singles();
    static int NUM_CHILDREN_PER_PARENT = 12;
    static int QUERY_VALUE_RATIO_PER_PARENT = 3;
    static int QUERY_COUNT = 50;
    static String indexName = "test";
    static Random random = new Random();

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = ChildSearchAndIndexingBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node1"))
                .clusterName(clusterName)
                .node();
        Client client = node1.client();

        client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        try {
            client.admin().indices().create(createIndexRequest(indexName)).actionGet();
            client.admin().indices().preparePutMapping(indexName).setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("child")
                    .startObject("_parent").field("type", "parent").endObject()
                    .endObject().endObject()).execute().actionGet();
            Thread.sleep(5000);

            long startTime = System.currentTimeMillis();
            ParentChildIndexGenerator generator = new ParentChildIndexGenerator(client, PARENT_COUNT, NUM_CHILDREN_PER_PARENT, QUERY_VALUE_RATIO_PER_PARENT);
            generator.index();
            System.out.println("--> Indexing took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds.");
        } catch (IndexAlreadyExistsException e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("--> Number of docs in index: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount());

        SearchThread searchThread = new SearchThread(client);
        new Thread(searchThread).start();
        IndexThread indexThread = new IndexThread(client);
        new Thread(indexThread).start();

        System.in.read();

        indexThread.stop();
        searchThread.stop();
        client.close();
        node1.close();
    }

    static class IndexThread implements Runnable {

        private final Client client;
        private volatile boolean run = true;

        IndexThread(Client client) {
            this.client = client;
        }

        @Override
        public void run() {
            while (run) {
                int childIdLimit = PARENT_COUNT * NUM_CHILDREN_PER_PARENT;
                for (int childId = 1; run && childId < childIdLimit;) {
                    try {
                        for (int j = 0; j < 8; j++) {
                            GetResponse getResponse = client
                                    .prepareGet(indexName, "child", String.valueOf(++childId))
                                    .setFields("_source", "_parent")
                                    .setRouting("1") // Doesn't matter what value, since there is only one shard
                                    .get();
                            client.prepareIndex(indexName, "child", Integer.toString(childId) + "_" + j)
                                    .setParent(getResponse.getField("_parent").getValue().toString())
                                    .setSource(getResponse.getSource())
                                    .get();
                        }
                        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
                        Thread.sleep(1000);
                        if (childId % 500 == 0) {
                            NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                                    .clear().setIndices(true).execute().actionGet();
                            System.out.println("Deleted docs: " + statsResponse.getAt(0).getIndices().getDocs().getDeleted());
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void stop() {
            run = false;
        }

    }

    static class SearchThread implements Runnable {

        private final Client client;
        private final int numValues;
        private volatile boolean run = true;

        SearchThread(Client client) {
            this.client = client;
            this.numValues = NUM_CHILDREN_PER_PARENT / NUM_CHILDREN_PER_PARENT;
        }

        @Override
        public void run() {
            while (run) {
                try {
                    long totalQueryTime = 0;
                    for (int j = 0; j < QUERY_COUNT; j++) {
                        SearchResponse searchResponse = client.prepareSearch(indexName)
                                .setQuery(
                                        filteredQuery(
                                                matchAllQuery(),
                                                hasChildFilter("child", termQuery("field2", "value" + random.nextInt(numValues)))
                                        )
                                )
                                .execute().actionGet();
                        if (searchResponse.getFailedShards() > 0) {
                            System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
                        }
                        totalQueryTime += searchResponse.getTookInMillis();
                    }
                    System.out.println("--> has_child filter with term filter Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

                    totalQueryTime = 0;
                    for (int j = 1; j <= QUERY_COUNT; j++) {
                        SearchResponse searchResponse = client.prepareSearch(indexName)
                                .setQuery(
                                        filteredQuery(
                                                matchAllQuery(),
                                                hasChildFilter("child", matchAllQuery())
                                        )
                                )
                                .execute().actionGet();
                        if (searchResponse.getFailedShards() > 0) {
                            System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
                        }
                        totalQueryTime += searchResponse.getTookInMillis();
                    }
                    System.out.println("--> has_child filter with match_all child query, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

                    NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                            .setJvm(true).execute().actionGet();
                    System.out.println("--> Committed heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapCommitted());
                    System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());
                    Thread.sleep(1000);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        public void stop() {
            run = false;
        }

    }

}
