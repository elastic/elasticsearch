/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.shards;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.AbstractCollectorTestCase;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ShardsCollectorTests extends AbstractCollectorTestCase {

    public void testShardsCollectorNoIndices() throws Exception {
        Collection<MonitoringDoc> results = newShardsCollector().doCollect();
        assertThat(results, hasSize(0));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INDICES.getKey(), "test-shards*")
                .build();
    }

    public void testShardsCollectorOneIndex() throws Exception {
        int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            client().prepareIndex("test-shards", "test").setSource("num", i).get();
        }

        waitForRelocation();
        ensureGreen();
        refresh();

        assertHitCount(client().prepareSearch().setSize(0).get(), nbDocs);

        Collection<MonitoringDoc> results = newShardsCollector().doCollect();
        assertThat(results, hasSize(getNumShards("test-shards").totalNumShards));

        final ClusterState clusterState = client().admin().cluster().prepareState().setMetaData(true).get().getState();

        int primaries = 0;
        int replicas = 0;

        for (MonitoringDoc monitoringDoc : results) {
            assertNotNull(monitoringDoc);
            assertThat(monitoringDoc, instanceOf(ShardMonitoringDoc.class));

            ShardMonitoringDoc shardMonitoringDoc = (ShardMonitoringDoc) monitoringDoc;
            assertThat(shardMonitoringDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
            assertThat(shardMonitoringDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
            assertThat(shardMonitoringDoc.getClusterUUID(), equalTo(clusterState.metaData().clusterUUID()));
            assertThat(shardMonitoringDoc.getTimestamp(), greaterThan(0L));
            assertThat(shardMonitoringDoc.getSourceNode(), notNullValue());
            assertThat(shardMonitoringDoc.getClusterStateUUID(), equalTo(clusterState.stateUUID()));

            ShardRouting shardRouting = shardMonitoringDoc.getShardRouting();
            assertNotNull(shardRouting);
            assertThat(shardMonitoringDoc.getShardRouting().assignedToNode(), is(true));

            if (shardRouting.primary()) {
                primaries++;
            } else {
                replicas++;
            }
        }

        int expectedPrimaries = getNumShards("test-shards").numPrimaries;
        int expectedReplicas = expectedPrimaries * getNumShards("test-shards").numReplicas;
        assertThat(primaries, equalTo(expectedPrimaries));
        assertThat(replicas, equalTo(expectedReplicas));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/96")
    public void testShardsCollectorMultipleIndices() throws Exception {
        final String indexPrefix = "test-shards-";
        final int nbIndices = randomIntBetween(1, 3);
        final int[] nbDocsPerIndex = new int[nbIndices];

        for (int i = 0; i < nbIndices; i++) {
            String index = indexPrefix + String.valueOf(i);
            assertAcked(prepareCreate(index));

            nbDocsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < nbDocsPerIndex[i]; j++) {
                client().prepareIndex(index, "test").setSource("num", i).get();
            }
        }

        waitForRelocation();
        refresh();

        int totalShards = 0;
        for (int i = 0; i < nbIndices; i++) {
            String index = indexPrefix + String.valueOf(i);

            assertHitCount(client().prepareSearch(index).setSize(0).get(), nbDocsPerIndex[i]);
            disableAllocation(index);
            totalShards += getNumShards(index).totalNumShards;
        }

        Collection<MonitoringDoc> results = newShardsCollector().doCollect();
        assertThat(results, hasSize(totalShards));

        final ClusterState clusterState = client().admin().cluster().prepareState().setMetaData(true).get().getState();

        for (MonitoringDoc doc : results) {
            assertNotNull(doc);
            assertThat(doc, instanceOf(ShardMonitoringDoc.class));

            ShardMonitoringDoc shardDoc = (ShardMonitoringDoc) doc;
            assertThat(shardDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
            assertThat(shardDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
            assertThat(shardDoc.getClusterUUID(), equalTo(clusterState.metaData().clusterUUID()));
            assertThat(shardDoc.getTimestamp(), greaterThan(0L));
            assertThat(shardDoc.getClusterStateUUID(), equalTo(clusterState.stateUUID()));

            ShardRouting shardRouting = shardDoc.getShardRouting();
            assertNotNull(shardRouting);

            if (shardRouting.assignedToNode()) {
                assertThat(shardDoc.getSourceNode(), notNullValue());
            } else {
                assertThat(shardDoc.getSourceNode(), nullValue());
            }
        }

        // Checks that a correct number of ShardMonitoringDoc documents has been created for each index
        int[] shards = new int[nbIndices];
        for (MonitoringDoc monitoringDoc : results) {
            ShardRouting routing = ((ShardMonitoringDoc) monitoringDoc).getShardRouting();
            int index = Integer.parseInt(routing.getIndexName().substring(indexPrefix.length()));
            shards[index]++;
        }

        for (int i = 0; i < nbIndices; i++) {
            String index = indexPrefix + String.valueOf(i);
            int total = getNumShards(index).totalNumShards;
            assertThat("expecting " + total + " shards monitoring documents for index [" + index + "]", shards[i], equalTo(total));
        }
    }

    private ShardsCollector newShardsCollector() {
        // This collector runs on master node only
        return newShardsCollector(internalCluster().getMasterName());
    }

    private ShardsCollector newShardsCollector(String nodeId) {
        assertNotNull(nodeId);
        return new ShardsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MonitoringSettings.class, nodeId),
                internalCluster().getInstance(XPackLicenseState.class, nodeId));
    }
}
