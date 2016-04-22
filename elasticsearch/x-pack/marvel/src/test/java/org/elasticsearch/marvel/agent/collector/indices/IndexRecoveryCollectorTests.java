/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class IndexRecoveryCollectorTests extends AbstractCollectorTestCase {

    private final boolean activeOnly = false;
    private final String indexName = "test";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INDEX_RECOVERY_ACTIVE_ONLY.getKey(), activeOnly)
                .put(MonitoringSettings.INDICES.getKey(), indexName)
                .build();
    }

    public void testIndexRecoveryCollector() throws Exception {
        logger.info("--> start first node");
        final String node1 = internalCluster().startNode();
        waitForNoBlocksOnNode(node1);

        logger.info("--> collect index recovery data");
        Collection<MonitoringDoc> results = newIndexRecoveryCollector(node1).doCollect();

        logger.info("--> no indices created, expecting 0 monitoring documents");
        assertNotNull(results);
        assertThat(results, is(empty()));

        logger.info("--> create index [{}] on node [{}]", indexName, node1);
        assertAcked(prepareCreate(indexName, 1, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1)));

        logger.info("--> indexing sample data");
        final int numDocs = between(50, 150);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName, "foo").setSource("value", randomInt()).get();
        }

        logger.info("--> create a second index [{}] on node [{}] that won't be part of stats collection", indexName, node1);
        client().prepareIndex("other", "bar").setSource("value", randomInt()).get();

        flushAndRefresh();
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numDocs);
        assertHitCount(client().prepareSearch("other").setSize(0).get(), 1L);

        logger.info("--> start second node");
        final String node2 = internalCluster().startNode();
        waitForNoBlocksOnNode(node2);
        waitForRelocation();

        for (MonitoringSettings monitoringSettings : internalCluster().getInstances(MonitoringSettings.class)) {
            assertThat(monitoringSettings.recoveryActiveOnly(), equalTo(activeOnly));
        }

        logger.info("--> collect index recovery data");
        results = newIndexRecoveryCollector(null).doCollect();

        logger.info("--> we should have at least 1 shard in relocation state");
        assertNotNull(results);
        assertThat(results, hasSize(1));

        MonitoringDoc monitoringDoc = results.iterator().next();
        assertNotNull(monitoringDoc);
        assertThat(monitoringDoc, instanceOf(IndexRecoveryMonitoringDoc.class));

        IndexRecoveryMonitoringDoc indexRecoveryMarvelDoc = (IndexRecoveryMonitoringDoc) monitoringDoc;
        assertThat(indexRecoveryMarvelDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(indexRecoveryMarvelDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(indexRecoveryMarvelDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(indexRecoveryMarvelDoc.getTimestamp(), greaterThan(0L));
        assertThat(indexRecoveryMarvelDoc.getSourceNode(), notNullValue());

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
                assertThat(shardRecovery.getType(), anyOf(equalTo(RecoveryState.Type.PRIMARY_RELOCATION), equalTo(RecoveryState.Type.STORE),
                        equalTo(RecoveryState.Type.REPLICA), equalTo(RecoveryState.Type.SNAPSHOT)));
            }
        }
    }

    public void testIndexRecoveryCollectorWithLicensing() throws Exception {
        List<String> nodesIds = internalCluster().startNodesAsync(randomIntBetween(2, 5)).get();
        waitForNoBlocksOnNodes();

        try {
            for (String node : nodesIds) {
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
        } finally {
            // Ensure license is enabled before finishing the test
            enableLicense();
        }
    }

    public void testEmptyCluster() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(),
                Strings.EMPTY_ARRAY));
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndexRecoveryCollector(node).doCollect(), hasSize(0));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    public void testEmptyClusterAllIndices() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(), MetaData.ALL));
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndexRecoveryCollector(node).doCollect(), hasSize(0));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    public void testEmptyClusterMissingIndex() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(), "unknown"));
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndexRecoveryCollector(node).doCollect(), hasSize(0));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    private IndexRecoveryCollector newIndexRecoveryCollector(String nodeId) {
        if (!Strings.hasText(nodeId)) {
            nodeId = randomFrom(internalCluster().getNodeNames());
        }
        return new IndexRecoveryCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MonitoringSettings.class, nodeId),
                internalCluster().getInstance(MonitoringLicensee.class, nodeId),
                securedClient(nodeId));
    }
}
