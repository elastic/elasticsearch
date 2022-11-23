/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class VotingConfigurationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testAbdicateAfterVotingConfigExclusionAdded() throws ExecutionException, InterruptedException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNodes(2);
        final String originalMaster = internalCluster().getMasterName();

        logger.info("--> excluding master node {}", originalMaster);
        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(originalMaster)).get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();
        assertNotEquals(originalMaster, internalCluster().getMasterName());
    }

    public void testElectsNodeNotInVotingConfiguration() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final List<String> nodeNames = internalCluster().startNodes(4);

        // a 4-node cluster settles on a 3-node configuration; we then prevent the nodes in the configuration from winning an election
        // by failing at the pre-voting stage, so that the extra node must be elected instead when the master shuts down. This extra node
        // should then add itself into the voting configuration.

        assertFalse(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareHealth()
                .setWaitForNodes("4")
                .setWaitForEvents(Priority.LANGUID)
                .get()
                .isTimedOut()
        );

        String excludedNodeName = null;
        final ClusterState clusterState = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .clear()
            .setNodes(true)
            .setMetadata(true)
            .get()
            .getState();
        final Set<String> votingConfiguration = clusterState.getLastCommittedConfiguration().getNodeIds();
        assertThat(votingConfiguration, hasSize(3));
        assertThat(clusterState.nodes().getSize(), equalTo(4));
        assertThat(votingConfiguration, hasItem(clusterState.nodes().getMasterNodeId()));
        for (DiscoveryNode discoveryNode : clusterState.nodes()) {
            if (votingConfiguration.contains(discoveryNode.getId()) == false) {
                assertThat(excludedNodeName, nullValue());
                excludedNodeName = discoveryNode.getName();
            }
        }

        for (final String sender : nodeNames) {
            if (sender.equals(excludedNodeName)) {
                continue;
            }
            final MockTransportService senderTransportService = (MockTransportService) internalCluster().getInstance(
                TransportService.class,
                sender
            );
            for (final String receiver : nodeNames) {
                senderTransportService.addSendBehavior(
                    internalCluster().getInstance(TransportService.class, receiver),
                    (connection, requestId, action, request, options) -> {
                        if (action.equals(PreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)) {
                            throw new ElasticsearchException("rejected");
                        }
                        connection.sendRequest(requestId, action, request, options);
                    }
                );
            }
        }

        internalCluster().stopCurrentMasterNode();
        assertFalse(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareHealth()
                .setWaitForNodes("3")
                .setWaitForEvents(Priority.LANGUID)
                .get()
                .isTimedOut()
        );

        final ClusterState newClusterState = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .clear()
            .setNodes(true)
            .setMetadata(true)
            .get()
            .getState();
        assertThat(newClusterState.nodes().getMasterNode().getName(), equalTo(excludedNodeName));
        assertThat(newClusterState.getLastCommittedConfiguration().getNodeIds(), hasItem(newClusterState.nodes().getMasterNodeId()));
    }
}
