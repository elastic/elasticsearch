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
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
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

        String clusterName = ChildSearchBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder().clusterName(clusterName)
                .settings(settingsBuilder().put(settings).put("name", "node1")).node();
        Client client = node1.client();

        long COUNT = SizeValue.parseSizeValue("10m").singles();
        int CHILD_COUNT = 5;
        int BATCH = 100;
        int QUERY_WARMUP = 20;
        int QUERY_COUNT = 50;
        String indexName = "test";

        client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        try {
            client.admin().indices().create(createIndexRequest(indexName)).actionGet();
            client.admin().indices().preparePutMapping(indexName).setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("type")
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
                    request.add(Requests.indexRequest(indexName).type("parent").id(Integer.toString(counter))
                            .source(parentSource(counter, "test" + counter)));
                    for (int k = 0; k < CHILD_COUNT; k++) {
                        request.add(Requests.indexRequest(indexName).type("child").id(Integer.toString(counter) + "_" + k)
                                .parent(Integer.toString(counter))
                                .source(childSource(counter, "tag" + k)));
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
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("--> Number of docs in index: " + client.prepareCount(indexName).setQuery(matchAllQuery()).execute().actionGet().getCount());

        System.out.println("--> Running just child query");
        // run just the child query, warm up first
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(termQuery("child.tag", "tag1")).execute().actionGet();
            if (j == 0) {
                System.out.println("--> Warmup took: " + searchResponse.getTook());
            }
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
        }

        long totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(termQuery("child.tag", "tag1")).execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Just Child Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).execute().actionGet();
        System.out.println("--> Committed heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapCommitted());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());
        
        // run parent child constant query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            filteredQuery(
                                    matchAllQuery(),
                                    hasChildFilter("child", termQuery("tag", "tag1"))
                            )
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + COUNT + "]");
            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            filteredQuery(
                                    matchAllQuery(),
                                    hasChildFilter("child", termQuery("tag", "tag1"))
                            )
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + COUNT + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child filter Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        System.out.println("--> Running has_child filter with match_all child query");
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
            long expected = (COUNT / BATCH) * BATCH;
            if (searchResponse.getHits().totalHits() != expected) {
                System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + expected + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child filter with match_all child query, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(
                    filteredQuery(matchAllQuery(), hasChildFilter("child", termQuery("id", Integer.toString(j + 1))))
            ).execute().actionGet();
            long expected = 1;
            if (searchResponse.getHits().totalHits() != expected) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child filter with single parent match Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            double expected = Math.pow((j + 1), 3) * CHILD_COUNT;
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(filteredQuery(matchAllQuery(), hasChildFilter("child", constantScoreQuery(rangeFilter("num").lte(expected)))))
                    .execute().actionGet();
            if (searchResponse.getHits().totalHits() != expected) {
                System.err.println("mismatch on hits: " + searchResponse.getHits().totalHits() + " != " + expected);
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child filter with exponential parent results Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        // run parent child constant query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            filteredQuery(
                                    matchAllQuery(),
                                    hasParentFilter("parent", termQuery("name", "test1"))
                            )
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (searchResponse.getHits().totalHits() != CHILD_COUNT) {
                System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + CHILD_COUNT + "]");
            }
        }

        totalQueryTime = 0;
        for (int j = 1; j <= QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            filteredQuery(
                                    matchAllQuery(),
                                    hasParentFilter("parent", termQuery("name", "test1"))
                            )
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (searchResponse.getHits().totalHits() != CHILD_COUNT) {
                System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + CHILD_COUNT + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_parent filter Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        System.out.println("--> Running has_parent filter with match_all parent query ");
        totalQueryTime = 0;
        for (int j = 1; j <= QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(filteredQuery(
                            matchAllQuery(),
                            hasParentFilter("parent", matchAllQuery())
                    ))
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (searchResponse.getHits().totalHits() != 5000000) {
                System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + 5000000 + "]");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_parent filter with match_all parent query, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        System.out.println("--> Running top_children query");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(topChildrenQuery("child", termQuery("tag", "tag1"))).execute().actionGet();
            // we expect to have mismatch on hits here
//            if (searchResponse.hits().totalHits() != COUNT) {
//                System.err.println("mismatch on hits");
//            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(topChildrenQuery("child", termQuery("tag", "tag1"))).execute().actionGet();
            // we expect to have mismatch on hits here
//            if (searchResponse.hits().totalHits() != COUNT) {
//                System.err.println("mismatch on hits");
//            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> top_children Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        System.out.println("--> Running top_children query, with match_all as child query");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(topChildrenQuery("child", matchAllQuery())).execute().actionGet();
            // we expect to have mismatch on hits here
//            if (searchResponse.hits().totalHits() != COUNT) {
//                System.err.println("mismatch on hits");
//            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(topChildrenQuery("child", matchAllQuery())).execute().actionGet();
            // we expect to have mismatch on hits here
//            if (searchResponse.hits().totalHits() != COUNT) {
//                System.err.println("mismatch on hits");
//            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> top_children, with match_all Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).setIndices(true).execute().actionGet();

        System.out.println("--> Id cache size: " + statsResponse.getNodes()[0].getIndices().getIdCache().getMemorySize());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());

        System.out.println("--> Running has_child query with score type");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasChildQuery("child", termQuery("tag", "tag1")).scoreType("max")).execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("mismatch on hits");
            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasChildQuery("child", termQuery("tag", "tag1")).scoreType("max")).execute().actionGet();
            if (searchResponse.getHits().totalHits() != COUNT) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        
        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasChildQuery("child", matchAllQuery()).scoreType("max")).execute().actionGet();
            long expected = (COUNT / BATCH) * BATCH;
            if (searchResponse.getHits().totalHits() != expected) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child query with match_all Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        
        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasChildQuery("child", termQuery("id", Integer.toString(j + 1))).scoreType("max")).execute().actionGet();
            long expected = 1;
            if (searchResponse.getHits().totalHits() != expected) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child query with single parent match Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        
        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            double expected = Math.pow((j + 1), 3) * CHILD_COUNT;
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasChildQuery("child", constantScoreQuery(rangeFilter("num").lte(expected))).scoreType("max")).execute().actionGet();
            if (searchResponse.getHits().totalHits() != expected) {
                System.err.println("mismatch on hits: " + searchResponse.getHits().totalHits() + " != " + expected);
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_child query with exponential parent results Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        
        /*System.out.println("--> Running has_parent query with score type");
        // run parent child score query
        for (int j = 0; j < QUERY_WARMUP; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasParentQuery("parent", termQuery("name", "test1")).scoreType("score")).execute().actionGet();
            if (searchResponse.getHits().totalHits() != CHILD_COUNT) {
                System.err.println("mismatch on hits");
            }
        }

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasParentQuery("parent", termQuery("name", "test1")).scoreType("score")).execute().actionGet();
            if (searchResponse.getHits().totalHits() != CHILD_COUNT) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_parent Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

        totalQueryTime = 0;
        for (int j = 0; j < QUERY_COUNT; j++) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(hasParentQuery("parent", matchAllQuery()).scoreType("score")).execute().actionGet();
            if (searchResponse.getHits().totalHits() != 5000000) {
                System.err.println("mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> has_parent query with match_all Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");*/

        System.gc();
        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).setIndices(true).execute().actionGet();

        System.out.println("--> Id cache size: " + statsResponse.getNodes()[0].getIndices().getIdCache().getMemorySize());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());

        client.close();
        node1.close();
    }

    private static XContentBuilder parentSource(int id, String nameValue) throws IOException {
        return jsonBuilder().startObject().field("id", Integer.toString(id)).field("num", id).field("name", nameValue).endObject();
    }

    private static XContentBuilder childSource(int id, String tag) throws IOException {
        return jsonBuilder().startObject().field("id", Integer.toString(id)).field("num", id).field("tag", tag).endObject();
    }
}
