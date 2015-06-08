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

package org.elasticsearch.benchmark.search.scroll;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class ScrollSearchBenchmark {

    // Run with: -Xms1G -Xms1G
    public static void main(String[] args) {
        String indexName = "test";
        String typeName = "type";
        String clusterName = ScrollSearchBenchmark.class.getSimpleName();
        long numDocs = SizeValue.parseSizeValue("300k").singles();
        int requestSize = 50;

        Settings settings = settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 3)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        Node[] nodes = new Node[3];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder()
                    .clusterName(clusterName)
                    .settings(settingsBuilder().put(settings).put("name", "node" + i))
                    .node();
        }

        Client client = nodes[0].client();

        try {
            client.admin().indices().prepareCreate(indexName).get();
            for (int counter = 1; counter <= numDocs;) {
                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                for (int bulkCounter = 0; bulkCounter < 100; bulkCounter++) {
                    if (counter > numDocs) {
                        break;
                    }
                    bulkRequestBuilder.add(
                            client.prepareIndex(indexName, typeName, String.valueOf(counter))
                                    .setSource("field1", counter++)
                    );
                }
                int indexedDocs = counter - 1;
                if (indexedDocs % 100000 == 0) {
                     System.out.printf(Locale.ENGLISH, "--> Indexed %d so far\n", indexedDocs);
                }
                bulkRequestBuilder.get();
            }
        } catch (IndexAlreadyExistsException e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }

        client.admin().indices().prepareRefresh(indexName).get();
        System.out.printf(Locale.ENGLISH, "--> Number of docs in index: %d\n", client.prepareCount().get().getCount());

        Long counter = numDocs;
        SearchResponse searchResponse = client.prepareSearch(indexName)
                .addSort("field1", SortOrder.DESC)
                .setSize(requestSize)
                .setScroll("10m").get();

        if (searchResponse.getHits().getTotalHits() != numDocs) {
            System.err.printf(Locale.ENGLISH, "Expected total hits [%d] but got [%d]\n", numDocs, searchResponse.getHits().getTotalHits());
        }

        if (searchResponse.getHits().hits().length != requestSize) {
            System.err.printf(Locale.ENGLISH, "Expected hits length [%d] but got [%d]\n", requestSize, searchResponse.getHits().hits().length);
        }

        for (SearchHit hit : searchResponse.getHits()) {
            if (!hit.sortValues()[0].equals(counter--)) {
                System.err.printf(Locale.ENGLISH, "Expected sort value [%d] but got [%s]\n", counter + 1, hit.sortValues()[0]);
            }
        }
        String scrollId = searchResponse.getScrollId();
        int scrollRequestCounter = 0;
        long sumTimeSpent = 0;
        while (true) {
            long timeSpent = System.currentTimeMillis();
            searchResponse = client.prepareSearchScroll(scrollId).setScroll("10m").get();
            sumTimeSpent += (System.currentTimeMillis() - timeSpent);
            scrollRequestCounter++;
            if (searchResponse.getHits().getTotalHits() != numDocs) {
                System.err.printf(Locale.ENGLISH, "Expected total hits [%d] but got [%d]\n", numDocs, searchResponse.getHits().getTotalHits());
            }
            if (scrollRequestCounter % 20 == 0) {
                long avgTimeSpent = sumTimeSpent / 20;
                JvmStats.Mem mem = JvmStats.jvmStats().getMem();
                System.out.printf(Locale.ENGLISH, "Cursor location=%d, avg time spent=%d ms\n", (requestSize * scrollRequestCounter), (avgTimeSpent));
                System.out.printf(Locale.ENGLISH, "heap max=%s, used=%s, percentage=%d\n", mem.getHeapMax(), mem.getHeapUsed(), mem.getHeapUsedPercent());
                sumTimeSpent = 0;
            }
            if (searchResponse.getHits().hits().length == 0) {
                break;
            }
            if (searchResponse.getHits().hits().length != requestSize) {
                System.err.printf(Locale.ENGLISH, "Expected hits length [%d] but got [%d]\n", requestSize, searchResponse.getHits().hits().length);
            }
            for (SearchHit hit : searchResponse.getHits()) {
                if (!hit.sortValues()[0].equals(counter--)) {
                    System.err.printf(Locale.ENGLISH, "Expected sort value [%d] but got [%s]\n", counter + 1, hit.sortValues()[0]);
                }
            }
            scrollId = searchResponse.getScrollId();
        }
        if (counter != 0) {
            System.err.printf(Locale.ENGLISH, "Counter should be 0 because scroll has been consumed\n");
        }

        for (Node node : nodes) {
            node.close();
        }
    }

}
