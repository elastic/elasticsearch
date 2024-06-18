/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.coordination.StatefulPreVoteCollector;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TransportMasterNodeActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @TestLogging(reason = "wip", value = "org.elasticsearch.action.support.master.TransportMasterNodeAction:DEBUG")
    public void testRoutingLoopProtection() {

        final var newMaster = internalCluster().startMasterOnlyNode();
        final var stateApplierBarrier = new CyclicBarrier(2);

        try {
            /*
             * Ensure that we've got 5 voting nodes in the cluster, this means even if the original
             * master manages to accept its own failed state update before standing down, we can still
             * establish a quorum without its (or our own) join.
             */
            final var enoughVotingMastersLatch = new CountDownLatch(1);
            internalCluster().getInstance(ClusterService.class, newMaster).addStateApplier(event -> {
                if (5 <= event.state().coordinationMetadata().getLastCommittedConfiguration().getNodeIds().size()) {
                    enoughVotingMastersLatch.countDown();
                }
            });
            internalCluster().startMasterOnlyNode();
            internalCluster().startMasterOnlyNode();
            internalCluster().startMasterOnlyNode();
            safeAwait(enoughVotingMastersLatch);
            long originalTerm = internalCluster().masterClient().admin().cluster().prepareState().get().getState().term();

            // Configure a latch that will be released when the existing master knows of the new master's election
            final String originalMasterName = internalCluster().getMasterName();
            logger.info("Original master was {}, new master will be {}", originalMasterName, newMaster);
            final var previousMasterKnowsNewMasterIsElectedLatch = new CountDownLatch(1);
            internalCluster().getInstance(ClusterService.class, originalMasterName).addStateApplier(event -> {
                DiscoveryNode masterNode = event.state().nodes().getMasterNode();
                if (masterNode != null && masterNode.getName().equals(newMaster)) {
                    previousMasterKnowsNewMasterIsElectedLatch.countDown();
                }
            });

            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                if (transportService.getLocalNode().getName().equals(newMaster)) {
                    continue;
                }

                /*
                 * Disable every other nodes' ability to send pre-vote and publish requests
                 */
                final var mockTransportService = asInstanceOf(MockTransportService.class, transportService);
                mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (action.equals(StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)
                        || action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                        throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                    } else {
                        connection.sendRequest(requestId, action, request, options);
                    }
                });

                /*
                 * Assert that no other node receives the re-routed message more than once, and only
                 * from a node in the original term
                 */
                final var reroutedMessageReceived = new AtomicBoolean(false);
                mockTransportService.addRequestHandlingBehavior(
                    TransportClusterHealthAction.TYPE.name(),
                    (handler, request, channel, task) -> {
                        assertThat(asInstanceOf(MasterNodeRequest.class, request).masterTerm(), equalTo(originalTerm));
                        assertTrue("rerouted message received exactly once", reroutedMessageReceived.compareAndSet(false, true));
                        handler.messageReceived(request, channel, task);
                    }
                );
            }

            /*
             * Count down latch when the new master receives the re-routed message, ensure it only receives it once, and
             * only from a node in the newMaster term
             */
            final var newMasterReceivedReroutedMessageLatch = new CountDownLatch(1);
            MockTransportService.getInstance(newMaster)
                .addRequestHandlingBehavior(TransportClusterHealthAction.TYPE.name(), (handler, request, channel, task) -> {
                    assertThat(asInstanceOf(MasterNodeRequest.class, request).masterTerm(), greaterThan(originalTerm));
                    assertThat(newMasterReceivedReroutedMessageLatch.getCount(), greaterThan(0L));
                    newMasterReceivedReroutedMessageLatch.countDown();
                    handler.messageReceived(request, channel, task);
                });

            /*
             * Block cluster state applier on newMaster to delay clearing of old master, and identifying self as
             * new master
             */
            internalCluster().getInstance(ClusterService.class, newMaster).getClusterApplierService().onNewClusterState("test", () -> {
                // Meet to signify application is blocked
                safeAwait(stateApplierBarrier);
                // Wait for the signal to unblock
                safeAwait(stateApplierBarrier);
                return null;
            }, ActionListener.noop());

            // Wait until state application is blocked
            safeAwait(stateApplierBarrier);

            // trigger a cluster state update, which fails, causing a master failover
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                .submitUnbatchedStateUpdateTask("no-op", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).build();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // expected
                    }
                });

            // Wait until the old master has acknowledged the new master's election
            safeAwait(previousMasterKnowsNewMasterIsElectedLatch);
            logger.info("New master is elected");

            // perform a TransportMasterNodeAction on the new master, which doesn't know it's the master yet
            final var stateFuture = client(newMaster).admin().cluster().prepareHealth().execute();

            // wait for the request to come back to the new master
            safeAwait(newMasterReceivedReroutedMessageLatch);

            // Unblock state application on new master, allow it to know of its election win
            safeAwait(stateApplierBarrier);

            assertFalse(stateFuture.isDone());

            safeGet(stateFuture);
        } finally {
            // Unblock applier loop if it was left blocked
            stateApplierBarrier.reset();
            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                asInstanceOf(MockTransportService.class, transportService).clearAllRules();
            }
        }
    }
}
