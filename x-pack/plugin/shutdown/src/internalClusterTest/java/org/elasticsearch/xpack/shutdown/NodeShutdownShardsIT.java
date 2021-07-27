/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.COMPLETE;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeShutdownShardsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ShutdownPlugin.class);
    }

    public void testReproduceNotStartedBug() throws Exception {
        assumeTrue("must be on a snapshot build of ES to run in order for the feature flag to be set", Build.CURRENT.isSnapshot());
        final String nodeToRestartName = internalCluster().startNode();
        final String nodeToRestartId = getNodeId(nodeToRestartName);
        Settings nodeToRestartDataPathSettings = internalCluster().dataPathSettings(nodeToRestartName);
        internalCluster().startNode();

        // Mark the node for shutdown
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.REMOVE,
            this.getTestName()
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        internalCluster().stopNode(nodeToRestartName);

        assertBusy(() -> {
            NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
            assertThat(nodes.getNodes().size(), equalTo(1));
        });

        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeToRestartId)
        ).get();

        assertThat(getResp.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
    }

    private String getNodeId(String nodeName) throws Exception {
        NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
        return nodes.getNodes()
            .stream()
            .map(NodeInfo::getNode)
            .filter(node -> node.getName().equals(nodeName))
            .map(DiscoveryNode::getId)
            .findFirst()
            .orElseThrow();
    }
}
