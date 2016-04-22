/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.node;

import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.AbstractCollectorTestCase;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

// numClientNodes is set to 0 in this test because the NodeStatsCollector never collects data on client nodes:
// the NodeStatsCollector.shouldCollect() method checks if the node has node files and client nodes don't have
// such files.
@ClusterScope(numClientNodes = 0)
public class NodeStatsCollectorTests extends AbstractCollectorTestCase {

    public void testNodeStatsCollector() throws Exception {
        String[] nodes = internalCluster().getNodeNames();
        for (String node : nodes) {
            logger.info("--> collecting node stats on node [{}]", node);
            Collection<MonitoringDoc> results = newNodeStatsCollector(node).doCollect();
            assertThat(results, hasSize(1));

            MonitoringDoc monitoringDoc = results.iterator().next();
            assertNotNull(monitoringDoc);
            assertThat(monitoringDoc, instanceOf(NodeStatsMonitoringDoc.class));

            NodeStatsMonitoringDoc nodeStatsMarvelDoc = (NodeStatsMonitoringDoc) monitoringDoc;
            assertThat(nodeStatsMarvelDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
            assertThat(nodeStatsMarvelDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
            assertThat(nodeStatsMarvelDoc.getClusterUUID(),
                    equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
            assertThat(nodeStatsMarvelDoc.getTimestamp(), greaterThan(0L));
            assertThat(nodeStatsMarvelDoc.getSourceNode(), notNullValue());

            assertThat(nodeStatsMarvelDoc.getNodeId(),
                    equalTo(internalCluster().getInstance(ClusterService.class, node).localNode().getId()));
            assertThat(nodeStatsMarvelDoc.isNodeMaster(), equalTo(node.equals(internalCluster().getMasterName())));
            assertThat(nodeStatsMarvelDoc.isMlockall(), equalTo(BootstrapInfo.isMemoryLocked()));
            assertNotNull(nodeStatsMarvelDoc.isDiskThresholdDeciderEnabled());
            assertNotNull(nodeStatsMarvelDoc.getDiskThresholdWaterMarkHigh());

            assertNotNull(nodeStatsMarvelDoc.getNodeStats());
        }
    }

    public void testNodeStatsCollectorWithLicensing() {
        try {
            String[] nodes = internalCluster().getNodeNames();
            for (String node : nodes) {
                logger.debug("--> creating a new instance of the collector");
                NodeStatsCollector collector = newNodeStatsCollector(node);
                assertNotNull(collector);

                logger.debug("--> enabling license and checks that the collector can collect data");
                enableLicense();
                assertCanCollect(collector);

                logger.debug("--> starting graceful period and checks that the collector can still collect data");
                beginGracefulPeriod();
                assertCanCollect(collector);

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

    private NodeStatsCollector newNodeStatsCollector(final String nodeId) {
        return new NodeStatsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MonitoringSettings.class, nodeId),
                internalCluster().getInstance(MonitoringLicensee.class, nodeId),
                internalCluster().getInstance(InternalClient.class, nodeId),
                internalCluster().getInstance(NodeEnvironment.class, nodeId),
                internalCluster().getInstance(DiskThresholdDecider.class, nodeId));
    }
}
