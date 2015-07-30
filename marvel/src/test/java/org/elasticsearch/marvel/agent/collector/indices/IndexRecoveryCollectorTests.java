/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettingsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

@Slow
@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 0)
public class IndexRecoveryCollectorTests extends ElasticsearchIntegrationTest {


    @Override
    protected Settings nodeSettings(int nodeOrdinal) {

        return super.nodeSettings(nodeOrdinal);
    }

    @Test
    @AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/366")
    public void testIndexRecoveryCollector() throws Exception {
        final String indexName = "test";

        logger.info("--> start first node");
        final String node1 = internalCluster().startNode();
        ensureYellow();

        logger.info("--> collect index recovery data");
        Collection<MarvelDoc> results = newIndexRecoveryCollector().doCollect();

        logger.info("--> no indices created, expecting 0 marvel documents");
        assertNotNull(results);
        assertThat(results, is(empty()));

        logger.info("--> create index on node: {}", node1);
        assertAcked(prepareCreate(indexName, 1, settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen(indexName);

        logger.info("--> indexing sample data");
        final int numDocs = between(50, 150);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName, "foo").setSource("value", randomInt()).get();
        }
        flushAndRefresh(indexName);
        assertHitCount(client().prepareCount(indexName).get(), numDocs);

        ByteSizeValue storeSize = client().admin().indices().prepareStats(indexName).get().getTotal().getStore().getSize();

        logger.info("--> start another node with very low recovery settings");
        internalCluster().startNode(settingsBuilder()
                        .put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_STREAMS, 1)
                        .put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, 1)
                        .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC, storeSize.bytes() / 10, ByteSizeUnit.BYTES)
                        .put(RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE, storeSize.bytes() / 10, ByteSizeUnit.BYTES)
        );

        logger.info("--> wait for at least 1 shard relocation");
        results = assertBusy(new Callable<Collection<MarvelDoc>>() {
            @Override
            public Collection<MarvelDoc> call() throws Exception {
                RecoveryResponse response = client().admin().indices().prepareRecoveries().setActiveOnly(true).get();
                assertTrue(response.hasRecoveries());

                List<ShardRecoveryResponse> shardResponses = response.shardResponses().get(indexName);
                assertFalse(shardResponses.isEmpty());

                boolean foundRelocation = false;
                for (ShardRecoveryResponse shardResponse : shardResponses) {
                    if (RecoveryState.Type.RELOCATION.equals(shardResponse.recoveryState().getType())) {
                        foundRelocation = true;
                        break;
                    }
                }
                assertTrue("found at least one relocation", foundRelocation);

                logger.info("--> collect index recovery data");
                return newIndexRecoveryCollector().doCollect();
            }
        });

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
                assertThat(shardRecovery.recoveryState().getType(), equalTo(RecoveryState.Type.RELOCATION));
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
}
