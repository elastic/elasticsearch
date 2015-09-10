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
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class ChildSearchShortCircuitBenchmark {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = ChildSearchShortCircuitBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder().clusterName(clusterName)
                .settings(settingsBuilder().put(settings).put("name", "node1"))
                .node();
        Client client = node1.client();

        long PARENT_COUNT = SizeValue.parseSizeValue("10M").singles();
        int BATCH = 100;
        int QUERY_WARMUP = 5;
        int QUERY_COUNT = 25;
        String indexName = "test";

        client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        try {
            client.admin().indices().create(createIndexRequest(indexName)).actionGet();
            client.admin().indices().preparePutMapping(indexName).setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("child")
                    .startObject("_parent").field("type", "parent").endObject()
                    .endObject().endObject()).execute().actionGet();
            Thread.sleep(5000);

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + PARENT_COUNT + "] parent document and some child documents");
            long ITERS = PARENT_COUNT / BATCH;
            int i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;
                    request.add(Requests.indexRequest(indexName).type("parent").id(Integer.toString(counter))
                            .source(parentSource(counter)));

                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("--> failures...");
                }
                if (((i * BATCH) % 10000) == 0) {
                    System.out.println("--> Indexed " + (i * BATCH) + "parent docs; took " + stopWatch.stop().lastTaskTime());
                    stopWatch.start();
                }
            }

            int id = 0;
            for (i = 1; i <= PARENT_COUNT; i *= 2) {
                int parentId = 1;
                for (int j = 0; j < i; j++) {
                    client.prepareIndex(indexName, "child", Integer.toString(id++))
                            .setParent(Integer.toString(parentId++))
                            .setSource(childSource(i))
                            .execute().actionGet();
                }
            }

            System.out.println("--> Indexing took " + stopWatch.totalTime());
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
        for (int i = 1; i <= 10000; i *= 2) {
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(matchQuery("child.field2", i)).execute().actionGet();
            System.out.println("--> Warmup took["+ i +"]: " + searchResponse.getTook());
            if (searchResponse.getHits().totalHits() != i) {
                System.err.println("--> mismatch on hits");
            }
        }

        NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).execute().actionGet();
        System.out.println("--> Committed heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapCommitted());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());

        // run parent child constant query
        for (int j = 1; j < QUERY_WARMUP; j *= 2) {
            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setQuery(
                            hasChildQuery("child", matchQuery("field2", j))
                    )
                    .execute().actionGet();
            if (searchResponse.getFailedShards() > 0) {
                System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
            }
            if (searchResponse.getHits().totalHits() != j) {
                System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + PARENT_COUNT + "]");
            }
        }

        long totalQueryTime = 0;
        for (int i = 1; i < PARENT_COUNT; i *= 2) {
            for (int j = 0; j < QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch(indexName)
                        .setQuery(boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", matchQuery("field2", i))))
                        .execute().actionGet();
                if (searchResponse.getHits().totalHits() != i) {
                    System.err.println("--> mismatch on hits");
                }
                totalQueryTime += searchResponse.getTookInMillis();
            }
            System.out.println("--> has_child filter " + i +" Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        }

        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).setIndices(true).execute().actionGet();

        System.out.println("--> Field data size: " + statsResponse.getNodes()[0].getIndices().getFieldData().getMemorySize());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());

        totalQueryTime = 0;
        for (int i = 1; i < PARENT_COUNT; i *= 2) {
            for (int j = 0; j < QUERY_COUNT; j++) {
                SearchResponse searchResponse = client.prepareSearch(indexName)
                        .setQuery(hasChildQuery("child", matchQuery("field2", i)).scoreMode("max"))
                        .execute().actionGet();
                if (searchResponse.getHits().totalHits() != i) {
                    System.err.println("--> mismatch on hits");
                }
                totalQueryTime += searchResponse.getTookInMillis();
            }
            System.out.println("--> has_child query " + i +" Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");
        }

        System.gc();
        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).setIndices(true).execute().actionGet();

        System.out.println("--> Field data size: " + statsResponse.getNodes()[0].getIndices().getFieldData().getMemorySize());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());

        client.close();
        node1.close();
    }

    private static XContentBuilder parentSource(int val) throws IOException {
        return jsonBuilder().startObject().field("field1", Integer.toString(val)).endObject();
    }

    private static XContentBuilder childSource(int val) throws IOException {
        return jsonBuilder().startObject().field("field2", Integer.toString(val)).endObject();
    }
}
