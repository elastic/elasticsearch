/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.AbstractCollectorTestCase;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.hamcrest.Matchers;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
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
        assertThat(newIndicesStatsCollector(node).doCollect(), hasSize(1));
    }

    public void testEmptyClusterAllIndices() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(), MetaData.ALL));
        waitForNoBlocksOnNode(node);
        assertThat(newIndicesStatsCollector(node).doCollect(), hasSize(1));
    }

    public void testEmptyClusterMissingIndex() throws Exception {
        final String node = internalCluster().startNode(Settings.builder().put(MonitoringSettings.INDICES.getKey(), "unknown"));
        waitForNoBlocksOnNode(node);
        assertThat(newIndicesStatsCollector(node).doCollect(), hasSize(1));
    }

    public void testIndicesStatsCollectorOneIndex() throws Exception {
        final String node = internalCluster().startNode();
        waitForNoBlocksOnNode(node);

        final String indexName = "one-index";
        createIndex(indexName);
        ensureGreen(indexName);


        final int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            client().prepareIndex(indexName, "test").setSource("num", i).get();
        }

        flush();
        refresh();

        assertHitCount(client().prepareSearch().setSize(0).get(), nbDocs);

        Collection<MonitoringDoc> results = newIndicesStatsCollector().doCollect();
        assertThat(results, hasSize(1));

        MonitoringDoc monitoringDoc = results.iterator().next();
        assertThat(monitoringDoc, instanceOf(IndicesStatsMonitoringDoc.class));

        IndicesStatsMonitoringDoc indicesStatsMonitoringDoc = (IndicesStatsMonitoringDoc) monitoringDoc;
        assertThat(indicesStatsMonitoringDoc.getClusterUUID(), equalTo(client().admin().cluster().
                prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(indicesStatsMonitoringDoc.getTimestamp(), greaterThan(0L));
        assertThat(indicesStatsMonitoringDoc.getSourceNode(), notNullValue());

        IndicesStatsResponse indicesStats = indicesStatsMonitoringDoc.getIndicesStats();
        assertNotNull(indicesStats);
        assertThat(indicesStats.getIndices().keySet(), hasSize(1));

        IndexStats indexStats = indicesStats.getIndex(indexName);
        assertThat(indexStats.getShards(), Matchers.arrayWithSize(getNumShards(indexName).totalNumShards));
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
            ensureGreen(index);

            docsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < docsPerIndex[i]; j++) {
                client().prepareIndex(index, "test").setSource("num", i).get();
            }
        }

        flush();
        refresh();

        for (int i = 0; i < nbIndices; i++) {
            assertHitCount(client().prepareSearch(indexPrefix + i).setSize(0).get(), docsPerIndex[i]);
        }

        Collection<MonitoringDoc> results = newIndicesStatsCollector().doCollect();
        assertThat(results, hasSize(1));

        MonitoringDoc monitoringDoc = results.iterator().next();
        assertThat(monitoringDoc, instanceOf(IndicesStatsMonitoringDoc.class));

        IndicesStatsMonitoringDoc indicesStatsMonitoringDoc = (IndicesStatsMonitoringDoc) monitoringDoc;
        assertThat(indicesStatsMonitoringDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(indicesStatsMonitoringDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(indicesStatsMonitoringDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(indicesStatsMonitoringDoc.getTimestamp(), greaterThan(0L));

        IndicesStatsResponse indicesStats = indicesStatsMonitoringDoc.getIndicesStats();
        assertNotNull(indicesStats);
        assertThat(indicesStats.getIndices().keySet(), hasSize(nbIndices));
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
                internalCluster().getInstance(XPackLicenseState.class, nodeId),
                securedClient(nodeId));
    }
}
