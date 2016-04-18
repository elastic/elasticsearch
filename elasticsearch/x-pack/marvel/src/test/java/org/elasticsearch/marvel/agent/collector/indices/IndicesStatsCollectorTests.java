/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.List;


import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class IndicesStatsCollectorTests extends AbstractCollectorTestCase {

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testEmptyCluster() throws Exception {
        final String node = internalCluster().startNode();
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndicesStatsCollector(node).doCollect(), hasSize(shieldEnabled ? 0 : 1));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    public void testEmptyClusterAllIndices() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(), MetaData.ALL));
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndicesStatsCollector(node).doCollect(), hasSize(shieldEnabled ? 0 : 1));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    public void testEmptyClusterMissingIndex() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(), "unknown"));
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndicesStatsCollector(node).doCollect(), hasSize(1));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    public void testIndicesStatsCollectorOneIndex() throws Exception {
        final String node = internalCluster().startNode();
        waitForNoBlocksOnNode(node);

        final String indexName = "one-index";
        createIndex(indexName);
        securedEnsureGreen(indexName);

        final int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            client().prepareIndex(indexName, "test").setSource("num", i).get();
        }

        securedFlush();
        securedRefresh();

        assertHitCount(client().prepareSearch().setSize(0).get(), nbDocs);

        Collection<MonitoringDoc> results = newIndicesStatsCollector().doCollect();
        assertThat(results, hasSize(1));

        MonitoringDoc monitoringDoc = results.iterator().next();
        assertThat(monitoringDoc, instanceOf(IndicesStatsMonitoringDoc.class));

        IndicesStatsMonitoringDoc indicesStatsMarvelDoc = (IndicesStatsMonitoringDoc) monitoringDoc;
        assertThat(indicesStatsMarvelDoc.getClusterUUID(), equalTo(client().admin().cluster().
                prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(indicesStatsMarvelDoc.getTimestamp(), greaterThan(0L));
        assertThat(indicesStatsMarvelDoc.getSourceNode(), notNullValue());

        IndicesStatsResponse indicesStats = indicesStatsMarvelDoc.getIndicesStats();
        assertNotNull(indicesStats);
        assertThat(indicesStats.getIndices().keySet(), hasSize(1));

        IndexStats indexStats = indicesStats.getIndex(indexName);
        assertThat(indexStats.getShards(), arrayWithSize(getNumShards(indexName).totalNumShards));
    }

    public void testIndicesStatsCollectorMultipleIndices() throws Exception {
        final String node = internalCluster().startNode();
        waitForNoBlocksOnNode(node);

        final String indexPrefix = "multi-indices-";
        final int nbIndices = randomIntBetween(1, 5);
        int[] docsPerIndex = new int[nbIndices];

        for (int i = 0; i < nbIndices; i++) {
            String index = indexPrefix + i;
            createIndex(index);
            securedEnsureGreen(index);

            docsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < docsPerIndex[i]; j++) {
                client().prepareIndex(index, "test").setSource("num", i).get();
            }
        }

        securedFlush();
        securedRefresh();

        for (int i = 0; i < nbIndices; i++) {
            assertHitCount(client().prepareSearch(indexPrefix + i).setSize(0).get(), docsPerIndex[i]);
        }

        Collection<MonitoringDoc> results = newIndicesStatsCollector().doCollect();
        assertThat(results, hasSize(1));

        MonitoringDoc monitoringDoc = results.iterator().next();
        assertThat(monitoringDoc, instanceOf(IndicesStatsMonitoringDoc.class));

        IndicesStatsMonitoringDoc indicesStatsMarvelDoc = (IndicesStatsMonitoringDoc) monitoringDoc;
        assertThat(indicesStatsMarvelDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(indicesStatsMarvelDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(indicesStatsMarvelDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(indicesStatsMarvelDoc.getTimestamp(), greaterThan(0L));

        IndicesStatsResponse indicesStats = indicesStatsMarvelDoc.getIndicesStats();
        assertNotNull(indicesStats);
        assertThat(indicesStats.getIndices().keySet(), hasSize(nbIndices));
    }

    public void testIndicesStatsCollectorWithLicensing() throws Exception {
        List<String> nodesIds = internalCluster().startNodesAsync(randomIntBetween(2, 5)).get();
        waitForNoBlocksOnNodes();

        try {
            final int nbDocs = randomIntBetween(1, 20);
            for (int i = 0; i < nbDocs; i++) {
                client().prepareIndex("test", "test").setSource("num", i).get();
            }

            securedFlush();
            securedRefresh();
            securedEnsureGreen("test");

            for (String node : nodesIds) {
                logger.debug("--> creating a new instance of the collector");
                IndicesStatsCollector collector = newIndicesStatsCollector(node);
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

    private IndicesStatsCollector newIndicesStatsCollector() {
        // This collector runs on master node only
        return newIndicesStatsCollector(internalCluster().getMasterName());
    }

    private IndicesStatsCollector newIndicesStatsCollector(String nodeId) {
        if (!Strings.hasText(nodeId)) {
            nodeId = randomFrom(internalCluster().getNodeNames());
        }
        return new IndicesStatsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MonitoringSettings.class, nodeId),
                internalCluster().getInstance(MonitoringLicensee.class, nodeId),
                securedClient(nodeId));
    }
}
