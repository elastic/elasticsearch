/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;

@ESIntegTestCase.ClusterScope(
    scope = ESIntegTestCase.Scope.TEST,
    numDataNodes = 0,
    numClientNodes = 0,
    autoManageMasterNodes = false
)
public class NodeJoiningIT extends MasterElectionTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            MockTransportService.TestPlugin.class
        );
    }

    public void testNodeJoinsCluster() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String masterNodeName = internalCluster().startMasterOnlyNode();
        String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        String newNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        // Assert the new data node was added
        ClusterState state = internalCluster().clusterService().state();
        assertEquals(3, state.nodes().getSize());
        assertEquals(2, state.nodes().getDataNodes().size());
        assertEquals(1, state.nodes().getMasterNodes().size());

        assertEquals(masterNodeName, internalCluster().getMasterName());
        assertTrue(state.nodes().nodeExistsWithName(dataNodeName));
        assertTrue(state.nodes().nodeExistsWithName(newNodeName));
    }

    public void testNodeFailsToJoinClusterWhenMasterNodeCannotPublishState() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String masterNodeName = internalCluster().startMasterOnlyNode();
        String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        final var masterNodeTransportService = MockTransportService.getInstance(masterNodeName);
        masterNodeTransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> channel.sendResponse(new IllegalStateException("cluster state updates blocked"))
        );

        // This will fail because the master cannot publish state
        String newNodeName = internalCluster().startNode();

        // Assert no new node was added
        ClusterState state = internalCluster().clusterService().state();
        assertEquals(2, state.nodes().getSize());
        assertEquals(1, state.nodes().getDataNodes().size());
        assertEquals(1, state.nodes().getMasterNodes().size());

        // Assert the only nodes in the cluster are the original ones
        assertEquals(masterNodeName, internalCluster().getMasterName());
        assertTrue(state.nodes().nodeExistsWithName(dataNodeName));
        assertFalse(state.nodes().nodeExistsWithName(newNodeName));

        masterNodeTransportService.clearAllRules();
    }

    public void testNodeFailsToJoinClusterWhenDataNodeCannotReceiveState() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String masterNodeName = internalCluster().startMasterOnlyNode();
        String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // block publications received by non-master node.
        final var dataNodeTransportService = MockTransportService.getInstance(dataNodeName);
        dataNodeTransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> channel.sendResponse(new IllegalStateException("cluster state updates blocked"))
        );

        // This will fail because the cluster state was not published to the data node, and therefore not acknowledged
        String newNodeName = internalCluster().startNode();

        // Assert no new node was added
        ClusterState state = internalCluster().clusterService().state();
        assertEquals(2, state.nodes().getSize());
        assertEquals(1, state.nodes().getDataNodes().size());
        assertEquals(1, state.nodes().getMasterNodes().size());

        // Assert the only nodes in the cluster are the original ones
        assertEquals(masterNodeName, internalCluster().getMasterName());
        assertTrue(state.nodes().nodeExistsWithName(dataNodeName));
        assertFalse(state.nodes().nodeExistsWithName(newNodeName));

        dataNodeTransportService.clearAllRules();
    }
}
