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

package org.elasticsearch.search.basic;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class DisruptedSearchTests extends ElasticsearchIntegrationTest {
    private static final Settings SETTINGS = settingsBuilder().put("gateway.type", "local").build();

    @Test
    public void searchWithRelocationAndSlowClusterStateProcessing() throws Exception {
        final String masterNode = internalCluster().startNode(ImmutableSettings.builder().put(SETTINGS).put("node.data", false));
        final String red_node = internalCluster().startNode(ImmutableSettings.builder().put(SETTINGS).put("node.color", "red"));
        final String blue_node = internalCluster().startNode(ImmutableSettings.builder().put(SETTINGS).put("node.color", "blue"));
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        ImmutableSettings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                                .put("index.routing.allocation.include.color", "red"))
        );
        ensureGreen("test");

        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            indexRequestBuilderList.add(client().prepareIndex().setIndex("test").setType("doc").setSource("{\"int_field\":1}"));
        }
        indexRandom(true, indexRequestBuilderList);
        SearchThread searchThread = new SearchThread();

        searchThread.start();
        SlowClusterStateProcessing disruption = null;
        disruption = new SlowClusterStateProcessing(blue_node, getRandom(), 0, 0, 1000, 2000);
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        logger.info("--> move shard from node_1 to node_3, and wait for relocation to finish");
        internalCluster().client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put("index.routing.allocation.include.color", "blue")).get();
        ensureGreen("test");
        searchThread.stopSearching = true;
        disruption.stopDisrupting();
    }

    public static class SearchThread extends Thread {
        public volatile boolean stopSearching = false;
        @Override
        public void run() {
            while (stopSearching == false) {
                assertSearchResponse(client().prepareSearch("test").get());
            }
        }
    }
}
