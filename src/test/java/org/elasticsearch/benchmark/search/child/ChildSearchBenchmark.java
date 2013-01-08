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

package org.elasticsearch.benchmark.search.child;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.hasChildFilter;
import static org.elasticsearch.index.query.FilterBuilders.hasParentFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class ChildSearchBenchmark {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.engine.robin.refreshInterval", "-1")
                .put("gateway.type", "local")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        Node node1 = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node1")).node();
        Client client = node1.client();

        long COUNT = SizeValue.parseSizeValue("1m").singles();
        int CHILD_COUNT = 5;
        int BATCH = 100;
        int QUERY_WARMUP = 20;
        int QUERY_COUNT = 50;

        Thread.sleep(10000);
        try {
            client.admin().indices().create(createIndexRequest("test")).actionGet();
            client.admin().indices().preparePutMapping("test").setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("type")
                    .startObject("_parent").field("type", "parent").endObject()
                    .endObject().endObject()).execute().actionGet();
            Thread.sleep(5000);

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + COUNT + "] parent document and [" + (COUNT * CHILD_COUNT) + " child documents");
            long ITERS = COUNT / BATCH;
            long i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;
                    request.add(Requests.indexRequest("test").type("parent").id(Integer.toString(counter))
                            .source(parentSource(Integer.toString(counter), "test" + counter)));
                    for (int k = 0; k < CHILD_COUNT; k++) {
                        request.add(Requests.indexRequest("test").type("child").id(Integer.toString(counter) + "_" + k)
                                .parent(Integer.toString(counter))
                                .source(childSource(Integer.toString(counter), "tag" + k)));
                    }
                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("--> failures...");
                }
                if (((i * BATCH) % 10000) == 0) {
                    System.out.println("--> Indexed " + (i * BATCH) * (1 + CHILD_COUNT) + " took " + stopWatch.stop().lastTaskTime());
                    stopWatch.start();
                }
            }
            System.out.println("--> Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) (COUNT * (1 + CHILD_COUNT))) / stopWatch.totalTime().secondsFrac()));
        } catch (Exception e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.timedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("--> Number of docs in index: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().count());

        System.out.println("--> Running just child query");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(termQuery("child.tag", "tag1")).execute().actionGet();
            if (j == 0) {
                System.out.println("--> Warmup took: " + searchResponse.took());
            }
            if (searchResponse.hits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
        }

        long totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(termQuery("child.tag", "tag1")).execute().actionGet();
            if (searchResponse.hits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.tookInMillis();
        }
        System.out.println("--> Just Child Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).execute().actionGet();
        System.out.println("--> Committed heap size: " + statsResponse.nodes()[0].getJvm().getMem().getHeapCommitted());
        System.out.println("--> Used heap size: " + statsResponse.nodes()[0].getJvm().getMem().getHeapUsed());

        String[] executionTypes = new String[]{"uid"/*, "bitset"*/};// either uid (faster, in general a bit more memory) or bitset (slower, but in general a bit less memory)
        for (String executionType : executionTypes) {
            System.out.println("--> Running has_child filter with execution type " + executionType);
            // run parent child constant query
            for (int j = 0; j < QUERY_WARMUP; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(
                                filteredQuery(
                                        matchAllQuery(),
                                        hasChildFilter("child", termQuery("tag", "tag1")).executionType(executionType)
                                )
                        )
                        .execute().actionGet();
                if (searchResponse.failedShards() > 0) {
                    System.err.println("Search Failures " + Arrays.toString(searchResponse.shardFailures()));
                }
                if (searchResponse.hits().totalHits() != COUNT) {
                    System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.hits().totalHits() + "], expected [" + COUNT + "]");
                }
            }

            totalQueryTime = 0;
            for (int j = 0; j < QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(
                                filteredQuery(
                                        matchAllQuery(),
                                        hasChildFilter("child", termQuery("tag", "tag1")).executionType(executionType)
                                )
                        )
                        .execute().actionGet();
                if (searchResponse.failedShards() > 0) {
                    System.err.println("Search Failures " + Arrays.toString(searchResponse.shardFailures()));
                }
                if (searchResponse.hits().totalHits() != COUNT) {
                    System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.hits().totalHits() + "], expected [" + COUNT + "]");
                }
                totalQueryTime += searchResponse.tookInMillis();
            }
            System.out.println("--> has_child[" + executionType + "] filter Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

            System.out.println("--> Running has_child[" + executionType + "] filter with match_all child query");
            totalQueryTime = 0;
            for (int j = 1; j <= QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(
                                filteredQuery(
                                        matchAllQuery(),
                                        hasChildFilter("child", matchAllQuery()).executionType(executionType)
                                )
                        )
                        .execute().actionGet();
                if (searchResponse.failedShards() > 0) {
                    System.err.println("Search Failures " + Arrays.toString(searchResponse.shardFailures()));
                }
                long expected = (COUNT / BATCH) * BATCH;
                if (searchResponse.hits().totalHits() != expected) {
                    System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.hits().totalHits() + "], expected [" + expected + "]");
                }
                totalQueryTime += searchResponse.tookInMillis();
            }
            System.out.println("--> has_child[" + executionType + "] filter with match_all child query, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        }

        for (String executionType : executionTypes) {
            System.out.println("--> Running has_parent filter with " + executionType + " execution type");
            // run parent child constant query
            for (int j = 0; j < QUERY_WARMUP; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(
                                filteredQuery(
                                        matchAllQuery(),
                                        hasParentFilter("parent", termQuery("name", "test1")).executionType(executionType)
                                )
                        )
                        .execute().actionGet();
                if (searchResponse.failedShards() > 0) {
                    System.err.println("Search Failures " + Arrays.toString(searchResponse.shardFailures()));
                }
                if (searchResponse.hits().totalHits() != CHILD_COUNT) {
                    System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.hits().totalHits() + "], expected [" + CHILD_COUNT + "]");
                }
            }

            totalQueryTime = 0;
            for (int j = 1; j <= QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(
                                filteredQuery(
                                        matchAllQuery(),
                                        hasParentFilter("parent", termQuery("name", "test1")).executionType(executionType)
                                )
                        )
                        .execute().actionGet();
                if (searchResponse.failedShards() > 0) {
                    System.err.println("Search Failures " + Arrays.toString(searchResponse.shardFailures()));
                }
                if (searchResponse.hits().totalHits() != CHILD_COUNT) {
                    System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.hits().totalHits() + "], expected [" + CHILD_COUNT + "]");
                }
                totalQueryTime += searchResponse.tookInMillis();
            }
            System.out.println("--> has_parent[" + executionType + "] filter Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

            System.out.println("--> Running has_parent[" + executionType + "] filter with match_all parent query ");
            totalQueryTime = 0;
            for (int j = 1; j <= QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch()
                        .setQuery(filteredQuery(
                                matchAllQuery(),
                                hasParentFilter("parent", matchAllQuery()).executionType(executionType)
                        ))
                        .execute().actionGet();
                if (searchResponse.failedShards() > 0) {
                    System.err.println("Search Failures " + Arrays.toString(searchResponse.shardFailures()));
                }
                if (searchResponse.hits().totalHits() != 5000000) {
                    System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.hits().totalHits() + "], expected [" + 5000000 + "]");
                }
                totalQueryTime += searchResponse.tookInMillis();
            }
            System.out.println("--> has_parent[" + executionType + "] filter with match_all parent query, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        }
        System.out.println("--> Running top_children query");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(topChildrenQuery("child", termQuery("tag", "tag1"))).execute().actionGet();
            // we expect to have mismatch on hits here
//            if (searchResponse.hits().totalHits() != COUNT) {
//                System.err.println("mismatch on hits");
//            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(topChildrenQuery("child", termQuery("tag", "tag1"))).execute().actionGet();
            // we expect to have mismatch on hits here
//            if (searchResponse.hits().totalHits() != COUNT) {
//                System.err.println("mismatch on hits");
//            }
            totalQueryTime += searchResponse.tookInMillis();
        }
        System.out.println("--> top_children Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).setIndices(true).execute().actionGet();

        System.out.println("--> Id cache size: " + statsResponse.nodes()[0].getIndices().cache().getIdCacheSize());
        System.out.println("--> Used heap size: " + statsResponse.nodes()[0].getJvm().getMem().getHeapUsed());

        System.out.println("--> Running has_child query with score type");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(hasChildQuery("child", termQuery("tag", "tag1")).scoreType("max")).execute().actionGet();
            if (searchResponse.hits().totalHits() != COUNT) {
                System.err.println("mismatch on hits");
            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(hasChildQuery("child", termQuery("tag", "tag1")).scoreType("max")).execute().actionGet();
            if (searchResponse.hits().totalHits() != COUNT) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.tookInMillis();
        }
        System.out.println("--> has_child Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(hasChildQuery("child", matchAllQuery()).scoreType("max")).execute().actionGet();
            long expected = (COUNT / BATCH) * BATCH;
            if (searchResponse.hits().totalHits() != expected) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.tookInMillis();
        }
        System.out.println("--> has_child query with match_all Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        System.out.println("--> Running has_parent query with score type");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(hasParentQuery("parent", termQuery("name", "test1")).scoreType("score")).execute().actionGet();
            if (searchResponse.hits().totalHits() != CHILD_COUNT) {
                System.err.println("mismatch on hits");
            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(hasParentQuery("parent", termQuery("name", "test1")).scoreType("score")).execute().actionGet();
            if (searchResponse.hits().totalHits() != CHILD_COUNT) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.tookInMillis();
        }
        System.out.println("--> has_parent Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(hasParentQuery("parent", matchAllQuery()).scoreType("score")).execute().actionGet();
            if (searchResponse.hits().totalHits() != 5000000) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.tookInMillis();
        }
        System.out.println("--> has_parent query with match_all Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");


        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).setIndices(true).execute().actionGet();

        System.out.println("--> Id cache size: " + statsResponse.nodes()[0].getIndices().cache().getIdCacheSize());
        System.out.println("--> Used heap size: " + statsResponse.nodes()[0].getJvm().getMem().getHeapUsed());

        client.close();
        node1.close();
    }

    private static XContentBuilder parentSource(String id, String nameValue) throws IOException {
        return jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }

    private static XContentBuilder childSource(String id, String tag) throws IOException {
        return jsonBuilder().startObject().field("id", id).field("tag", tag).endObject();
    }
}
