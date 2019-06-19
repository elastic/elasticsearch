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

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Tests that the index creation operation waits for the appropriate
 * number of active shards to be started before returning.
 */
public class ActiveShardsObserverIT extends ESIntegTestCase {

    public void testCreateIndexNoActiveShardsTimesOut() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder()
                                               .put(indexSettings())
                                               .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5))
                                               .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
        if (internalCluster().getNodeNames().length > 0) {
            String exclude = String.join(",", internalCluster().getNodeNames());
            settingsBuilder.put("index.routing.allocation.exclude._name", exclude);
        }
        Settings settings = settingsBuilder.build();
        final String indexName = "test-idx";
        assertFalse(prepareCreate(indexName)
                       .setSettings(settings)
                       .setWaitForActiveShards(randomBoolean() ? ActiveShardCount.from(1) : ActiveShardCount.ALL)
                       .setTimeout("100ms")
                       .get()
                       .isShardsAcknowledged());
        waitForIndexCreationToComplete(indexName);
    }

    public void testCreateIndexNoActiveShardsNoWaiting() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder()
                                               .put(indexSettings())
                                               .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5))
                                               .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
        if (internalCluster().getNodeNames().length > 0) {
            String exclude = String.join(",", internalCluster().getNodeNames());
            settingsBuilder.put("index.routing.allocation.exclude._name", exclude);
        }
        Settings settings = settingsBuilder.build();
        CreateIndexResponse response = prepareCreate("test-idx")
                                           .setSettings(settings)
                                           .setWaitForActiveShards(ActiveShardCount.NONE)
                                           .get();
        assertTrue(response.isAcknowledged());
    }

    public void testCreateIndexNotEnoughActiveShardsTimesOut() throws Exception {
        final int numDataNodes = internalCluster().numDataNodes();
        final int numReplicas = numDataNodes + randomInt(4);
        Settings settings = Settings.builder()
                                .put(indexSettings())
                                .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5))
                                .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicas)
                                .build();
        final String indexName = "test-idx";
        assertFalse(prepareCreate(indexName)
                       .setSettings(settings)
                       .setWaitForActiveShards(randomIntBetween(numDataNodes + 1, numReplicas + 1))
                       .setTimeout("100ms")
                       .get()
                       .isShardsAcknowledged());
        waitForIndexCreationToComplete(indexName);
    }

    public void testCreateIndexEnoughActiveShards() throws Exception {
        final String indexName = "test-idx";
        Settings settings = Settings.builder()
                                .put(indexSettings())
                                .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5))
                                .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), internalCluster().numDataNodes() + randomIntBetween(0, 3))
                                .build();
        assertAcked(prepareCreate(indexName).setSettings(settings)
                        .setWaitForActiveShards(randomIntBetween(0, internalCluster().numDataNodes()))
                        .get());
    }

    public void testCreateIndexWaitsForAllActiveShards() throws Exception {
        // not enough data nodes, index creation times out
        final int numReplicas = internalCluster().numDataNodes() + randomInt(4);
        Settings settings = Settings.builder()
                                .put(indexSettings())
                                .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5))
                                .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicas)
                                .build();
        final String indexName = "test-idx";
        assertFalse(prepareCreate(indexName)
                       .setSettings(settings)
                       .setWaitForActiveShards(ActiveShardCount.ALL)
                       .setTimeout("100ms")
                       .get()
                       .isShardsAcknowledged());
        waitForIndexCreationToComplete(indexName);
        if (indexExists(indexName)) {
            client().admin().indices().prepareDelete(indexName).get();
        }

        // enough data nodes, all shards are active
        settings = Settings.builder()
                        .put(indexSettings())
                        .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 7))
                        .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), internalCluster().numDataNodes() - 1)
                        .build();
        assertAcked(prepareCreate(indexName).setSettings(settings).setWaitForActiveShards(ActiveShardCount.ALL).get());
    }

    public void testCreateIndexStopsWaitingWhenIndexDeleted() throws Exception {
        final String indexName = "test-idx";
        Settings settings = Settings.builder()
                                .put(indexSettings())
                                .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5))
                                .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), internalCluster().numDataNodes() - 1)
                                .build();

        logger.info("--> start the index creation process");
        ActionFuture<CreateIndexResponse> responseListener =
            prepareCreate(indexName)
                .setSettings(settings)
                .setWaitForActiveShards(ActiveShardCount.ALL)
                .execute();

        logger.info("--> wait until the cluster state contains the new index");
        assertBusy(() -> assertTrue(client().admin().cluster().prepareState().get().getState().metaData().hasIndex(indexName)));

        logger.info("--> delete the index");
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> ensure the create index request completes");
        assertAcked(responseListener.get());
    }

    // Its possible that the cluster state update task that includes the create index hasn't processed before we timeout,
    // and subsequently the test cleanup process does not delete the index in question because it does not see it, and
    // only after the test cleanup does the index creation manifest in the cluster state.  To take care of this problem
    // and its potential ramifications, we wait here for the index creation cluster state update task to finish
    private void waitForIndexCreationToComplete(final String indexName) {
        client().admin().cluster().prepareHealth(indexName).setWaitForEvents(Priority.URGENT).get();
    }

}
