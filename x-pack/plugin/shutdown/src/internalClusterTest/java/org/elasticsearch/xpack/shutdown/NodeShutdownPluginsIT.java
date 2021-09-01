/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0)
public class NodeShutdownPluginsIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(NodeShutdownPluginsIT.class);

    public static final AtomicBoolean safe = new AtomicBoolean(true);
    public static final AtomicReference<Collection<String>> triggeredNodes = new AtomicReference<>(null);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ShutdownPlugin.class, TestShutdownAwarePlugin.class);
    }

    public void testShutdownAwarePlugin() throws Exception {
        final String node1 = internalCluster().startNode();
        final String node2 = internalCluster().startNode();

        final String shutdownNode;
        final String remainNode;
        NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
        final String node1Id = nodes.getNodes()
            .stream()
            .map(NodeInfo::getNode)
            .filter(node -> node.getName().equals(node1))
            .map(DiscoveryNode::getId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("fail"));
        final String node2Id = nodes.getNodes()
            .stream()
            .map(NodeInfo::getNode)
            .filter(node -> node.getName().equals(node2))
            .map(DiscoveryNode::getId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("fail"));

        if (randomBoolean()) {
            shutdownNode = node1Id;
            remainNode = node2Id;
        } else {
            shutdownNode = node2Id;
            remainNode = node1Id;
        }
        logger.info("--> node {} will be shut down, {} will remain", shutdownNode, remainNode);

        // First, mark the plugin as not yet safe
        safe.set(false);

        // Mark the node as shutting down
        client().execute(
            PutShutdownNodeAction.INSTANCE,
            new PutShutdownNodeAction.Request(shutdownNode, SingleNodeShutdownMetadata.Type.REMOVE, "removal for testing", null)
        ).get();

        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(remainNode)
        ).get();

        assertTrue(getResp.getShutdownStatuses().isEmpty());

        // The plugin should be in progress
        getResp = client().execute(GetShutdownStatusAction.INSTANCE, new GetShutdownStatusAction.Request(shutdownNode)).get();
        assertThat(
            getResp.getShutdownStatuses().get(0).pluginsStatus().getStatus(),
            equalTo(SingleNodeShutdownMetadata.Status.IN_PROGRESS)
        );

        // Change the plugin to be "done" shutting down
        safe.set(true);

        // The plugin should be complete
        getResp = client().execute(GetShutdownStatusAction.INSTANCE, new GetShutdownStatusAction.Request(shutdownNode)).get();
        assertThat(getResp.getShutdownStatuses().get(0).pluginsStatus().getStatus(), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE));

        // The shutdown node should be in the triggered list
        assertThat(triggeredNodes.get(), contains(shutdownNode));

        client().execute(DeleteShutdownNodeAction.INSTANCE, new DeleteShutdownNodeAction.Request(shutdownNode)).get();

        // The shutdown node should now not in the triggered list
        assertThat(triggeredNodes.get(), empty());
    }

    public static class TestShutdownAwarePlugin extends Plugin implements ShutdownAwarePlugin {

        @Override
        public boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
            logger.info("--> checking whether safe to shutdown for node [{}], type [{}] answer: ({})", nodeId, shutdownType, safe.get());
            return safe.get();
        }

        @Override
        public void signalShutdown(Collection<String> shutdownNodeIds) {
            logger.info("--> shutdown triggered for {}", shutdownNodeIds);
            triggeredNodes.set(shutdownNodeIds);
        }
    }
}
