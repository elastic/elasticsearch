/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

@ClusterScope(numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class IndexRecoveryCollectorTests extends AbstractCollectorTestCase {

    private final boolean activeOnly = false;
    private final String indexName = "test";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY, activeOnly)
                .put(MarvelSettings.INDICES, indexName)
                .build();
    }

    @Test
    public void testIndexRecoveryCollector() throws Exception {

        logger.info("--> start first node");
        final String node1 = internalCluster().startNode();
        waitForNoBlocksOnNode(node1);

        logger.info("--> collect index recovery data");
        Collection<MarvelDoc> results = newIndexRecoveryCollector(node1).doCollect();

        logger.info("--> no indices created, expecting 0 marvel documents");
        assertNotNull(results);
        assertThat(results, is(empty()));

        logger.info("--> create index [{}] on node [{}]", indexName, node1);
        assertAcked(prepareCreate(indexName, 1, settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1)));

        logger.info("--> indexing sample data");
        final int numDocs = between(50, 150);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName, "foo").setSource("value", randomInt()).get();
        }

        logger.info("--> create a second index [other] that won't be part of stats collection", indexName, node1);
        client().prepareIndex("other", "bar").setSource("value", randomInt()).get();

        flushAndRefresh();
        assertHitCount(client().prepareCount(indexName).get(), numDocs);
        assertHitCount(client().prepareCount("other").get(), 1L);

        logger.info("--> start second node");
        final String node2 = internalCluster().startNode();
        waitForNoBlocksOnNode(node2);
        waitForRelocation();

        for (MarvelSettings marvelSettings : internalCluster().getInstances(MarvelSettings.class)) {
            assertThat(marvelSettings.recoveryActiveOnly(), equalTo(activeOnly));
        }

        logger.info("--> collect index recovery data");
        results = newIndexRecoveryCollector(null).doCollect();

        logger.info("--> we should have at least 1 shard in relocation state");
        assertNotNull(results);
        assertThat(results, hasSize(1));

        MarvelDoc marvelDoc = results.iterator().next();
        assertNotNull(marvelDoc);
        assertThat(marvelDoc, instanceOf(IndexRecoveryMarvelDoc.class));

        IndexRecoveryMarvelDoc indexRecoveryMarvelDoc = (IndexRecoveryMarvelDoc) marvelDoc;
        assertThat(indexRecoveryMarvelDoc.clusterUUID(), equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(indexRecoveryMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(indexRecoveryMarvelDoc.type(), equalTo(IndexRecoveryCollector.TYPE));

        RecoveryResponse recovery = indexRecoveryMarvelDoc.getRecoveryResponse();
        assertNotNull(recovery);

        Map<String, List<RecoveryState>> shards = recovery.shardRecoveryStates();
        assertThat(shards.size(), greaterThan(0));

        for (Map.Entry<String, List<RecoveryState>> shard : shards.entrySet()) {
            List<RecoveryState> shardRecoveries = shard.getValue();
            assertNotNull(shardRecoveries);
            assertThat(shardRecoveries.size(), greaterThan(0));

            for (RecoveryState shardRecovery : shardRecoveries) {
                assertThat(shard.getKey(), equalTo(indexName));
                assertThat(shardRecovery.getType(), anyOf(equalTo(RecoveryState.Type.RELOCATION), equalTo(RecoveryState.Type.STORE), equalTo(RecoveryState.Type.REPLICA)));
            }
        }
    }

    @Test @AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/683")
    public void testIndexRecoveryCollectorWithLicensing() {
        String[] nodes = internalCluster().getNodeNames();
        for (String node : nodes) {
            logger.debug("--> creating a new instance of the collector");
            IndexRecoveryCollector collector = newIndexRecoveryCollector(node);
            assertNotNull(collector);

            logger.debug("--> enabling license and checks that the collector can collect data if node is master");
            enableLicense();
            if (node.equals(internalCluster().getMasterName())) {
                assertCanCollect(collector);
            } else {
                assertCannotCollect(collector);
            }

            logger.debug("--> starting graceful period and checks that the collector can still collect data if node is master");
            beginGracefulPeriod();
            if (node.equals(internalCluster().getMasterName())) {
                assertCanCollect(collector);
            } else {
                assertCannotCollect(collector);
            }

            logger.debug("--> ending graceful period and checks that the collector cannot collect data");
            endGracefulPeriod();
            assertCannotCollect(collector);

            logger.debug("--> disabling license and checks that the collector cannot collect data");
            disableLicense();
            assertCannotCollect(collector);
        }
    }

    private IndexRecoveryCollector newIndexRecoveryCollector(String nodeId) {
        if (!Strings.hasText(nodeId)) {
            nodeId = randomFrom(internalCluster().getNodeNames());
        }
        return new IndexRecoveryCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MarvelSettings.class, nodeId),
                internalCluster().getInstance(MarvelLicensee.class, nodeId),
                securedClient(nodeId));
    }
}
