/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class ClusterStateCollectorTests extends AbstractCollectorTestCase {

    public void testClusterStateCollectorNoIndices() throws Exception {
        assertMarvelDocs(newClusterStateCollector().doCollect(), 0);
    }

    public void testClusterStateCollectorOneIndex() throws Exception {
        int nbShards = randomIntBetween(1, 5);
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, nbShards)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            client().prepareIndex("test", "test").setSource("num", i).get();
        }

        securedFlush();
        securedRefresh();

        assertHitCount(client().prepareSearch().setSize(0).get(), nbDocs);
        assertMarvelDocs(newClusterStateCollector().doCollect(), nbShards);
    }

    public void testClusterStateCollectorMultipleIndices() throws Exception {
        int nbIndices = randomIntBetween(1, 5);
        int[] docsPerIndex = new int[nbIndices];
        int[] shardsPerIndex = new int[nbIndices];
        int nbShards = 0;

        for (int i = 0; i < nbIndices; i++) {
            shardsPerIndex[i] = randomIntBetween(1, 5);
            nbShards += shardsPerIndex[i];
            assertAcked(prepareCreate("test-" + i).setSettings(Settings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, shardsPerIndex[i])
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()));

            docsPerIndex[i] = randomIntBetween(1, 20);
            for (int j = 0; j < docsPerIndex[i]; j++) {
                client().prepareIndex("test-" + i, "test").setSource("num", i).get();
            }
        }

        securedFlush();
        securedRefresh();

        for (int i = 0; i < nbIndices; i++) {
            assertHitCount(client().prepareSearch("test-" + i).setSize(0).get(), docsPerIndex[i]);
        }

        Collection<MonitoringDoc> results = newClusterStateCollector().doCollect();
        assertMarvelDocs(results, nbShards);

        MonitoringDoc monitoringDoc = results.iterator().next();
        assertNotNull(monitoringDoc);
        assertThat(monitoringDoc, instanceOf(ClusterStateMonitoringDoc.class));

        ClusterStateMonitoringDoc clusterStateMarvelDoc = (ClusterStateMonitoringDoc) monitoringDoc;

        assertThat(clusterStateMarvelDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
        assertThat(clusterStateMarvelDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(clusterStateMarvelDoc.getClusterUUID(),
                equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
        assertThat(clusterStateMarvelDoc.getTimestamp(), greaterThan(0L));
        assertThat(clusterStateMarvelDoc.getSourceNode(), notNullValue());
        assertNotNull(clusterStateMarvelDoc.getClusterState());

        ClusterState clusterState = clusterStateMarvelDoc.getClusterState();
        for (int i = 0; i < nbIndices; i++) {
            assertThat(clusterState.getRoutingTable().allShards("test-" + i), hasSize(shardsPerIndex[i]));
        }
    }

    public void testClusterStateCollectorWithLicensing() {
        try {
            String[] nodes = internalCluster().getNodeNames();
            for (String node : nodes) {
                logger.debug("--> creating a new instance of the collector");
                ClusterStateCollector collector = newClusterStateCollector(node);
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

    private ClusterStateCollector newClusterStateCollector() {
        // This collector runs on master node only
        return newClusterStateCollector(internalCluster().getMasterName());
    }

    private ClusterStateCollector newClusterStateCollector(String nodeId) {
        assertNotNull(nodeId);
        return new ClusterStateCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MonitoringSettings.class, nodeId),
                internalCluster().getInstance(MonitoringLicensee.class, nodeId),
                securedClient(nodeId));
    }

    private void assertMarvelDocs(Collection<MonitoringDoc> results, final int nbShards) {
        assertThat("expecting 1 document for cluster state and 2 documents per node", results, hasSize(1 + internalCluster().size() * 2));

        final ClusterState clusterState = securedClient().admin().cluster().prepareState().get().getState();
        final String clusterUUID = clusterState.getMetaData().clusterUUID();
        final String stateUUID = clusterState.stateUUID();

        List<ClusterStateNodeMonitoringDoc> clusterStateNodes = new ArrayList<>();
        List<DiscoveryNodeMonitoringDoc> discoveryNodes = new ArrayList<>();

        for (MonitoringDoc doc : results) {
            assertThat(doc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
            assertThat(doc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
            assertThat(doc.getClusterUUID(), equalTo(clusterUUID));
            assertThat(doc.getTimestamp(), greaterThan(0L));
            assertThat(doc.getSourceNode(), notNullValue());
            assertThat(doc, anyOf(instanceOf(ClusterStateMonitoringDoc.class),
                    instanceOf(ClusterStateNodeMonitoringDoc.class), instanceOf(DiscoveryNodeMonitoringDoc.class)));

            if (doc instanceof ClusterStateMonitoringDoc) {
                ClusterStateMonitoringDoc clusterStateMarvelDoc = (ClusterStateMonitoringDoc) doc;
                assertThat(clusterStateMarvelDoc.getClusterState().getRoutingTable().allShards(), hasSize(nbShards));
                assertThat(clusterStateMarvelDoc.getClusterState().getNodes().getSize(), equalTo(internalCluster().size()));

            } else if (doc instanceof ClusterStateNodeMonitoringDoc) {
                ClusterStateNodeMonitoringDoc clusterStateNodeMarvelDoc = (ClusterStateNodeMonitoringDoc) doc;
                assertThat(clusterStateNodeMarvelDoc.getStateUUID(), equalTo(stateUUID));
                assertThat(clusterStateNodeMarvelDoc.getNodeId(), not(isEmptyOrNullString()));
                clusterStateNodes.add(clusterStateNodeMarvelDoc);

            } else if (doc instanceof DiscoveryNodeMonitoringDoc) {
                DiscoveryNodeMonitoringDoc discoveryNodeMarvelDoc = (DiscoveryNodeMonitoringDoc) doc;
                assertNotNull(discoveryNodeMarvelDoc.getNode());
                discoveryNodes.add(discoveryNodeMarvelDoc);

            } else {
                fail("unknown monitoring document type " + doc);
            }
        }

        assertThat(clusterStateNodes, hasSize(internalCluster().size()));
        assertThat(discoveryNodes, hasSize(internalCluster().size()));

        for (final String nodeName : internalCluster().getNodeNames()) {
            final String nodeId = internalCluster().clusterService(nodeName).localNode().getId();

            boolean found = false;
            for (ClusterStateNodeMonitoringDoc doc : clusterStateNodes) {
                if (nodeId.equals(doc.getNodeId())) {
                    found = true;
                    break;
                }
            }
            assertTrue("Could not find node id [" + nodeName + "]", found);

            found = false;
            for (DiscoveryNodeMonitoringDoc doc : discoveryNodes) {
                if (nodeName.equals(doc.getNode().getName())) {
                    found = true;
                    break;
                }
            }
            assertTrue("Could not find node name [" + nodeName + "]", found);
        }
    }
}
