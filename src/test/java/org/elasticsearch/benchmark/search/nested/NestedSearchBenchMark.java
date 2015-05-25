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

package org.elasticsearch.benchmark.search.nested;

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
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class NestedSearchBenchMark {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        Node node1 = nodeBuilder()
                .settings(settingsBuilder().put(settings).put("name", "node1"))
                .node();
        Client client = node1.client();

        int count = (int) SizeValue.parseSizeValue("1m").singles();
        int nestedCount = 10;
        int rootDocs = count / nestedCount;
        int batch = 100;
        int queryWarmup = 5;
        int queryCount = 500;
        String indexName = "test";
        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth()
                .setWaitForGreenStatus().execute().actionGet();
        if (clusterHealthResponse.isTimedOut()) {
            System.err.println("--> Timed out waiting for cluster health");
        }

        try {
            client.admin().indices().prepareCreate(indexName)
                    .addMapping("type", XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("type")
                            .startObject("properties")
                            .startObject("field1")
                            .field("type", "integer")
                            .endObject()
                            .startObject("field2")
                            .field("type", "nested")
                            .startObject("properties")
                            .startObject("field3")
                            .field("type", "integer")
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                    ).execute().actionGet();
            clusterHealthResponse = client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + rootDocs + "] root documents and [" + (rootDocs * nestedCount) + "] nested objects");
            long ITERS = rootDocs / batch;
            long i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < batch; j++) {
                    counter++;
                    XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                            .field("field1", counter)
                            .startArray("field2");
                    for (int k = 0; k < nestedCount; k++) {
                        doc = doc.startObject()
                                .field("field3", k)
                                .endObject();
                    }
                    doc = doc.endArray();
                    request.add(
                            Requests.indexRequest(indexName).type("type").id(Integer.toString(counter)).source(doc)
                    );
                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("--> failures...");
                }
                if (((i * batch) % 10000) == 0) {
                    System.out.println("--> Indexed " + (i * batch) + " took " + stopWatch.stop().lastTaskTime());
                    stopWatch.start();
                }
            }
            System.out.println("--> Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) (count * (1 + nestedCount))) / stopWatch.totalTime().secondsFrac()));
        } catch (Exception e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            clusterHealthResponse = client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("--> Number of docs in index: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount());

        NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).execute().actionGet();
        System.out.println("--> Committed heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapCommitted());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());

        System.out.println("--> Running match_all with sorting on nested field");
        // run just the child query, warm up first
        for (int j = 0; j < queryWarmup; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addSort(
                            SortBuilders.fieldSort("field2.field3")
                                    .setNestedPath("field2")
                                    .sortMode("avg")
                                    .order(SortOrder.ASC)
                    )
                    .execute().actionGet();
            if (j == 0) {
                System.out.println("--> Warmup took: " + searchResponse.getTook());
            }
            if (searchResponse.getHits().totalHits() != rootDocs) {
                System.err.println("--> mismatch on hits");
            }
        }

        long totalQueryTime = 0;
        for (int j = 0; j < queryCount; j++) {
            SearchResponse searchResponse = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .addSort(
                            SortBuilders.fieldSort("field2.field3")
                                    .setNestedPath("field2")
                                    .sortMode("avg")
                                    .order(j % 2 == 0 ? SortOrder.ASC : SortOrder.DESC)
                    )
                    .execute().actionGet();

            if (searchResponse.getHits().totalHits() != rootDocs) {
                System.err.println("--> mismatch on hits");
            }
            totalQueryTime += searchResponse.getTookInMillis();
        }
        System.out.println("--> Sorting by nested fields took: " + (totalQueryTime / queryCount) + "ms");

        statsResponse = client.admin().cluster().prepareNodesStats()
                .setJvm(true).execute().actionGet();
        System.out.println("--> Committed heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapCommitted());
        System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());
    }

}
