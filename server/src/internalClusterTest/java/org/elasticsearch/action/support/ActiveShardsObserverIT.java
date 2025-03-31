/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
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
        assertFalse(
            prepareCreate(indexName).setSettings(settings)
                .setWaitForActiveShards(randomBoolean() ? ActiveShardCount.from(1) : ActiveShardCount.ALL)
                .setTimeout(TimeValue.timeValueMillis(100))
                .get()
                .isShardsAcknowledged()
        );
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
        assertAcked(prepareCreate("test-idx").setSettings(settings).setWaitForActiveShards(ActiveShardCount.NONE));
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
        assertFalse(
            prepareCreate(indexName).setSettings(settings)
                .setWaitForActiveShards(randomIntBetween(numDataNodes + 1, numReplicas + 1))
                .setTimeout(TimeValue.timeValueMillis(100))
                .get()
                .isShardsAcknowledged()
        );
        waitForIndexCreationToComplete(indexName);
    }

    public void testCreateIndexEnoughActiveShards() throws Exception {
        final String indexName = "test-idx";
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5))
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), internalCluster().numDataNodes() + randomIntBetween(0, 3))
            .build();
        assertAcked(
            prepareCreate(indexName).setSettings(settings).setWaitForActiveShards(randomIntBetween(0, internalCluster().numDataNodes()))
        );
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
        assertFalse(
            prepareCreate(indexName).setSettings(settings)
                .setWaitForActiveShards(ActiveShardCount.ALL)
                .setTimeout(TimeValue.timeValueMillis(100))
                .get()
                .isShardsAcknowledged()
        );
        waitForIndexCreationToComplete(indexName);
        if (indexExists(indexName)) {
            indicesAdmin().prepareDelete(indexName).get();
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
        ActionFuture<CreateIndexResponse> responseListener = prepareCreate(indexName).setSettings(settings)
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .execute();

        logger.info("--> wait until the cluster state contains the new index");
        assertBusy(
            () -> assertTrue(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata().getProject().hasIndex(indexName))
        );

        logger.info("--> delete the index");
        assertAcked(indicesAdmin().prepareDelete(indexName));

        logger.info("--> ensure the create index request completes");
        assertAcked(responseListener.get());
    }

    // Its possible that the cluster state update task that includes the create index hasn't processed before we timeout,
    // and subsequently the test cleanup process does not delete the index in question because it does not see it, and
    // only after the test cleanup does the index creation manifest in the cluster state. To take care of this problem
    // and its potential ramifications, we wait here for the index creation cluster state update task to finish
    private void waitForIndexCreationToComplete(final String indexName) {
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).setWaitForEvents(Priority.URGENT).get();
    }

}
