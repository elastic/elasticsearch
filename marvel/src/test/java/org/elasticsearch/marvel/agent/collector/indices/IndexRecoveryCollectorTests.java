/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class IndexRecoveryCollectorTests extends ESIntegTestCase {

    private boolean activeOnly = false;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettingsService.INDEX_RECOVERY_ACTIVE_ONLY, activeOnly)
                .build();
    }

    @Test
    public void testIndexRecoveryCollector() throws Exception {
        final String indexName = "test";

        logger.info("--> start first node");
        final String node1 = internalCluster().startNode();
        waitForNoBlocksOnNode(node1);

        logger.info("--> collect index recovery data");
        Collection<MarvelDoc> results = newIndexRecoveryCollector().doCollect();

        logger.info("--> no indices created, expecting 0 marvel documents");
        assertNotNull(results);
        assertThat(results, is(empty()));

        logger.info("--> create index on node: {}", node1);
        assertAcked(prepareCreate(indexName, 1, settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1)));

        logger.info("--> indexing sample data");
        final int numDocs = between(50, 150);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName, "foo").setSource("value", randomInt()).get();
        }
        flushAndRefresh(indexName);
        assertHitCount(client().prepareCount(indexName).get(), numDocs);

        logger.info("--> start second node");
        final String node2 = internalCluster().startNode();
        waitForNoBlocksOnNode(node2);
        waitForRelocation();

        for (MarvelSettingsService marvelSettingsService : internalCluster().getInstances(MarvelSettingsService.class)) {
            assertThat(marvelSettingsService.recoveryActiveOnly(), equalTo(activeOnly));
        }

        logger.info("--> collect index recovery data");
        results = newIndexRecoveryCollector().doCollect();

        logger.info("--> we should have at least 1 shard in relocation state");
        assertNotNull(results);
        assertThat(results, hasSize(1));

        MarvelDoc marvelDoc = results.iterator().next();
        assertNotNull(marvelDoc);
        assertThat(marvelDoc, instanceOf(IndexRecoveryMarvelDoc.class));

        IndexRecoveryMarvelDoc indexRecoveryMarvelDoc = (IndexRecoveryMarvelDoc) marvelDoc;
        assertThat(indexRecoveryMarvelDoc.clusterName(), equalTo(client().admin().cluster().prepareHealth().get().getClusterName()));
        assertThat(indexRecoveryMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(indexRecoveryMarvelDoc.type(), equalTo(IndexRecoveryCollector.TYPE));

        IndexRecoveryMarvelDoc.Payload payload = indexRecoveryMarvelDoc.payload();
        assertNotNull(payload);

        RecoveryResponse recovery = payload.getRecoveryResponse();
        assertNotNull(recovery);

        Map<String, List<ShardRecoveryResponse>> shards = recovery.shardResponses();
        assertThat(shards.size(), greaterThan(0));

        for (Map.Entry<String, List<ShardRecoveryResponse>> shard : shards.entrySet()) {
            List<ShardRecoveryResponse> shardRecoveries = shard.getValue();
            assertNotNull(shardRecoveries);
            assertThat(shardRecoveries.size(), greaterThan(0));

            for (ShardRecoveryResponse shardRecovery : shardRecoveries) {
                assertThat(shardRecovery.recoveryState().getType(), anyOf(equalTo(RecoveryState.Type.RELOCATION), equalTo(RecoveryState.Type.STORE), equalTo(RecoveryState.Type.REPLICA)));
            }
        }
    }

    private IndexRecoveryCollector newIndexRecoveryCollector() {
        return new IndexRecoveryCollector(internalCluster().getInstance(Settings.class),
                internalCluster().getInstance(ClusterService.class),
                internalCluster().getInstance(ClusterName.class),
                internalCluster().getInstance(MarvelSettingsService.class),
                client());
    }

    public void waitForNoBlocksOnNode(final String nodeId) throws Exception {
        assertBusy(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ClusterBlocks clusterBlocks = client(nodeId).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState().blocks();
                assertTrue(clusterBlocks.global().isEmpty());
                assertTrue(clusterBlocks.indices().values().isEmpty());
                return null;
            }
        }, 30L, TimeUnit.SECONDS);
    }
}
