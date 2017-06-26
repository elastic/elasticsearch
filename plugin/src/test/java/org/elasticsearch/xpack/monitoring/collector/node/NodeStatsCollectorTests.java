/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.node;

import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.AbstractCollectorTestCase;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.security.InternalClient;

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
            Collection<MonitoringDoc> results = newNodeStatsCollector(node).doCollect();
            assertThat(results, hasSize(1));

            MonitoringDoc monitoringDoc = results.iterator().next();
            assertNotNull(monitoringDoc);
            assertThat(monitoringDoc, instanceOf(NodeStatsMonitoringDoc.class));

            NodeStatsMonitoringDoc nodeStatsMonitoringDoc = (NodeStatsMonitoringDoc) monitoringDoc;
            assertThat(nodeStatsMonitoringDoc.getMonitoringId(), equalTo(MonitoredSystem.ES.getSystem()));
            assertThat(nodeStatsMonitoringDoc.getMonitoringVersion(), equalTo(Version.CURRENT.toString()));
            assertThat(nodeStatsMonitoringDoc.getClusterUUID(),
                    equalTo(client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID()));
            assertThat(nodeStatsMonitoringDoc.getTimestamp(), greaterThan(0L));
            assertThat(nodeStatsMonitoringDoc.getSourceNode(), notNullValue());

            assertThat(nodeStatsMonitoringDoc.getNodeId(),
                    equalTo(internalCluster().getInstance(ClusterService.class, node).localNode().getId()));
            assertThat(nodeStatsMonitoringDoc.isNodeMaster(), equalTo(node.equals(internalCluster().getMasterName())));
            assertThat(nodeStatsMonitoringDoc.isMlockall(), equalTo(BootstrapInfo.isMemoryLocked()));

            assertNotNull(nodeStatsMonitoringDoc.getNodeStats());
        }
    }

    private NodeStatsCollector newNodeStatsCollector(final String nodeId) {
        return new NodeStatsCollector(internalCluster().getInstance(Settings.class, nodeId),
                internalCluster().getInstance(ClusterService.class, nodeId),
                internalCluster().getInstance(MonitoringSettings.class, nodeId),
                internalCluster().getInstance(XPackLicenseState.class, nodeId),
                internalCluster().getInstance(InternalClient.class, nodeId));
    }
}
