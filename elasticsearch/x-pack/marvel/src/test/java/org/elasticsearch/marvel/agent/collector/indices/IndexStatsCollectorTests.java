/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;


import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class IndexStatsCollectorTests extends AbstractCollectorTestCase {

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testEmptyCluster() throws Exception {
        final String node = internalCluster().startNode();
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndexStatsCollector(node).doCollect(), hasSize(0));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    public void testEmptyClusterAllIndices() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(), MetaData.ALL));
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndexStatsCollector(node).doCollect(), hasSize(0));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    public void testEmptyClusterMissingIndex() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(), "unknown"));
        waitForNoBlocksOnNode(node);

        try {
            assertThat(newIndexStatsCollector(node).doCollect(), hasSize(0));
        } catch (IndexNotFoundException e) {
            fail("IndexNotFoundException has been thrown but it should have been swallowed by the collector");
        }
    }

    public void testIndexStatsCollectorOneIndex() throws Exception {
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

        Collection<MonitoringDoc> results = newIndexStatsCollector().doCollect();
        assertThat(results, hasSize(1));

        MonitoringDoc monitoringDoc = results.iterator().next();
        assertNotNull(monitoringDoc);
        assertThat(monitoringDoc, instanceOf(IndexStatsMonitoringDoc.class));

        IndexStatsMonitoringDoc indexStatsMarvelDoc = (IndexStatsMonitoringDoc) monitoringDoc;
        assertThat(indexStatsMarvelDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(indexStatsMarvelDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(indexStatsMarvelDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(indexStatsMarvelDoc.getTimestamp(), greaterThan(0L));
        assertThat(indexStatsMarvelDoc.getSourceNode(), notNullValue());

        IndexStats indexStats = indexStatsMarvelDoc.getIndexStats();
        assertNotNull(indexStats);

        assertThat(indexStats.getIndex(), equalTo(indexName));
        assertThat(indexStats.getPrimaries().getDocs().getCount(), equalTo((long) nbDocs));
        assertNotNull(indexStats.getTotal().getStore());
        assertThat(indexStats.getTotal().getStore().getSizeInBytes(), greaterThan(0L));
        assertThat(indexStats.getTotal().getStore().getThrottleTime().millis(), equalTo(0L));
        assertNotNull(indexStats.getTotal().getIndexing());
        assertThat(indexStats.getTotal().getIndexing().getTotal().getThrottleTime().millis(), equalTo(0L));
    }

    public void testIndexStatsCollectorMultipleIndices() throws Exception {
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

        String clusterUUID = client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID();

        Collection<MonitoringDoc> results = newIndexStatsCollector().doCollect();
        assertThat(results, hasSize(nbIndices));

        for (int i = 0; i < nbIndices; i++) {
            String indexName = indexPrefix + i;
            boolean found = false;

            Iterator<MonitoringDoc> it = results.iterator();
            while (!found && it.hasNext()) {
                MonitoringDoc monitoringDoc = it.next();
                assertThat(monitoringDoc, instanceOf(IndexStatsMonitoringDoc.class));

                IndexStatsMonitoringDoc indexStatsMarvelDoc = (IndexStatsMonitoringDoc) monitoringDoc;
                IndexStats indexStats = indexStatsMarvelDoc.getIndexStats();
                assertNotNull(indexStats);

                if (indexStats.getIndex().equals(indexPrefix + i)) {
                    assertThat(indexStatsMarvelDoc.getClusterUUID(), equalTo(clusterUUID));
                    assertThat(indexStatsMarvelDoc.getTimestamp(), greaterThan(0L));
                    assertThat(indexStatsMarvelDoc.getSourceNode(), notNullValue());

                    assertThat(indexStats.getIndex(), equalTo(indexName));
                    assertNotNull(indexStats.getTotal().getDocs());
                    assertThat(indexStats.getPrimaries().getDocs().getCount(), equalTo((long) docsPerIndex[i]));
                    assertNotNull(indexStats.getTotal().getStore());
                    assertThat(indexStats.getTotal().getStore().getSizeInBytes(), greaterThanOrEqualTo(0L));
                    assertThat(indexStats.getTotal().getStore().getThrottleTime().millis(), equalTo(0L));
                    assertNotNull(indexStats.getTotal().getIndexing());
                    assertThat(indexStats.getTotal().getIndexing().getTotal().getThrottleTime().millis(), equalTo(0L));
                    found = true;
                }
            }
            assertThat("could not find collected stats for index [" + indexPrefix + i + "]", found, is(true));
        }
    }

    public void testIndexStatsCollectorWithLicensing() throws Exception {
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
                IndexStatsCollector collector = newIndexStatsCollector(node);
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

    private IndexStatsCollector newIndexStatsCollector() {
        // This collector runs on master node only
        return newIndexStatsCollector(internalCluster().getMasterName());
    }

    private IndexStatsCollector newIndexStatsCollector(String nodeId) {
        assertNotNull(nodeId);
        return new IndexStatsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MonitoringSettings.class, nodeId),
                internalCluster().getInstance(MonitoringLicensee.class, nodeId),
                securedClient(nodeId));
    }
}
