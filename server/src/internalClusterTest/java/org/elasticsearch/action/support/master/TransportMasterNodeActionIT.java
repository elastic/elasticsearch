/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.coordination.StatefulPreVoteCollector;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.greaterThan;

public class TransportMasterNodeActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @TestLogging(reason = "wip", value = "org.elasticsearch.transport.TransportService.tracer:TRACE")
    public void testRoutingLoopProtection() {

        final var newMaster = internalCluster().startMasterOnlyNode();
        final var enoughVotingMastersLatch = new CountDownLatch(1);
        final var electionWonLatch = new CountDownLatch(1);
        final var releaseMasterLatch = new CountDownLatch(1);

        final ClusterStateApplier blockingApplier = event -> {
            if (3 <= event.state().coordinationMetadata().getLastCommittedConfiguration().getNodeIds().size()) {
                enoughVotingMastersLatch.countDown();
            }

            if (event.state().nodes().isLocalNodeElectedMaster()) {
                logger.info("--> new master elected as planned");
                electionWonLatch.countDown();
                safeAwait(releaseMasterLatch);
                logger.info("--> cluster state applications released on new master");
            }
        };

        try {
            internalCluster().getInstance(ClusterService.class, newMaster).addStateApplier(blockingApplier);

            // need at least 3 voting master nodes for failover
            internalCluster().startMasterOnlyNode();
            safeAwait(enoughVotingMastersLatch);

            final var reroutedMessageReceived = new AtomicBoolean(false);
            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                if (transportService.getLocalNode().getName().equals(newMaster)) {
                    continue;
                }
                final var mockTransportService = asInstanceOf(MockTransportService.class, transportService);
                mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (action.equals(StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)
                        || action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                        throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                    } else {
                        connection.sendRequest(requestId, action, request, options);
                    }
                });
                mockTransportService.addRequestHandlingBehavior(ClusterStateAction.NAME, (handler, request, channel, task) -> {
                    // assertThat(asInstanceOf(MasterNodeRequest.class, request).masterTerm(), equalTo(originalTerm)) TODO
                    assertTrue("rerouted message received exactly once", reroutedMessageReceived.compareAndSet(false, true));
                    handler.messageReceived(request, channel, task);
                });
            }

            final var doubleReroutedMessageLatch = new CountDownLatch(1);
            MockTransportService.getInstance(newMaster)
                .addRequestHandlingBehavior(ClusterStateAction.NAME, (handler, request, channel, task) -> {
                    // assertThat(asInstanceOf(MasterNodeRequest.class, request).masterTerm(), greaterThan(originalTerm)) TODO
                    assertThat(doubleReroutedMessageLatch.getCount(), greaterThan(0L));
                    doubleReroutedMessageLatch.countDown();
                });

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

            safeAwait(electionWonLatch);

            // perform a TransportMasterNodeAction on the new master, which doesn't know it's the master yet
            final var stateFuture = client(newMaster).admin().cluster().prepareState().clear().execute();

            // wait for the request to come back to the new master, which should now wait for its local term to advance
            safeAwait(doubleReroutedMessageLatch);

            assertFalse(stateFuture.isDone());

            releaseMasterLatch.countDown();
            safeGet(stateFuture);

        } finally {
            enoughVotingMastersLatch.countDown();
            electionWonLatch.countDown();
            releaseMasterLatch.countDown();
            internalCluster().getInstance(ClusterService.class, newMaster).removeApplier(blockingApplier);
            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                asInstanceOf(MockTransportService.class, transportService).clearAllRules();
            }
        }
    }
}
