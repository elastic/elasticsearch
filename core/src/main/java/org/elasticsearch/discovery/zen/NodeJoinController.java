/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.discovery.zen;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.membership.MembershipAction;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class NodeJoinController {

    final ESLogger logger;
    final ClusterService clusterService;
    final RoutingService routingService;
    final DiscoverySettings discoverySettings;
    final AtomicBoolean accumulateJoins = new AtomicBoolean(false);

    // this is site while trying to become a master
    final AtomicReference<ElectionContext> electionContext = new AtomicReference<>();


    protected final Map<DiscoveryNode, List<MembershipAction.JoinCallback>> pendingJoinRequests = new HashMap<>();

    public NodeJoinController(ClusterService clusterService, RoutingService routingService, DiscoverySettings discoverySettings, ESLogger logger) {
        this.clusterService = clusterService;
        this.logger = logger;
        this.routingService = routingService;
        this.discoverySettings = discoverySettings;
    }

    public void waitToBeElectedAsMaster(int requiredJoins, TimeValue timeValue, final Callback callback) {
        final CountDownLatch done = new CountDownLatch(1);
        final ElectionContext newContext = new ElectionContext();
        newContext.requiredJoins = requiredJoins;
        newContext.callback = new Callback() {
            @Override
            public void onElectedAsMaster(ClusterState state) {
                done.countDown();
                if (electionContext.compareAndSet(newContext, null)) {
                    stopAccumulatingJoins();
                    callback.onElectedAsMaster(state);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                done.countDown();
                if (electionContext.compareAndSet(newContext, null)) {
                    stopAccumulatingJoins();
                    callback.onFailure(t);
                }
            }
        };

        if (electionContext.compareAndSet(null, newContext) == false) {
            // should never happen, but be conservative
            callback.onFailure(new IllegalStateException("double waiting for election"));
            return;
        }

        // check what we have so far..
        checkAndElect();

        try {
            if (done.await(timeValue.millis(), TimeUnit.MILLISECONDS)) {
                // callback handles everything
                return;
            }
        } catch (InterruptedException e) {

        }
        if (logger.isTraceEnabled()) {
            final int pendingNodes;
            synchronized (pendingJoinRequests) {
                pendingNodes = pendingJoinRequests.size();
            }
            logger.trace("timed out waiting to be elected. waited [{}]. pending node joins [{}]", timeValue, pendingNodes);
        }
        // callback will clear the context, if it's active
        newContext.callback.onFailure(new ElasticsearchTimeoutException("timed out waiting to be elected"));
    }

    public void startAccumulatingJoins() {
        logger.trace("starting to accumulate joins");
        boolean b = accumulateJoins.getAndSet(true);
        assert b == false : "double startAccumulatingJoins() calls";
        assert electionContext.get() == null : "startAccumulatingJoins() called, but there is an ongoing election context";
    }

    public void stopAccumulatingJoins() {
        logger.trace("stopping joins accumulation");
        assert electionContext.get() == null : "stopAccumulatingJoins() called, but there is an ongoing election context";
        boolean b = accumulateJoins.getAndSet(false);
        assert b : "stopAccumulatingJoins() called but not accumulating";
        synchronized (pendingJoinRequests) {
            if (pendingJoinRequests.size() > 0) {
                processJoins("stopping to accumulate joins");
            }
        }
    }

    public void handleJoinRequest(final DiscoveryNode node, final MembershipAction.JoinCallback callback) {
        synchronized (pendingJoinRequests) {
            List<MembershipAction.JoinCallback> nodeCallbacks = pendingJoinRequests.get(node);
            if (nodeCallbacks == null) {
                nodeCallbacks = new ArrayList<>();
                pendingJoinRequests.put(node, nodeCallbacks);
            }
            nodeCallbacks.add(callback);
        }
        if (accumulateJoins.get() == false) {
            processJoins("join from node[" + node + "]");
        } else {
            checkAndElect();
        }
    }

    private void checkAndElect() {
        assert accumulateJoins.get() : "election check requested but we are not accumulating joins";
        final ElectionContext context = electionContext.get();
        if (context == null) {
            return;
        }
        final int pendingJoins;
        synchronized (pendingJoinRequests) {
            pendingJoins = pendingJoinRequests.size();
        }
        if (pendingJoins < context.requiredJoins) {
            logger.trace("not enough joins for election. Got [{}], required [{}]", pendingJoins, context.requiredJoins);
            return;
        }
        final String source = "zen-disco-join(elected_as_master, [" + pendingJoins + "] joins received)";
        clusterService.submitStateUpdateTask(source, Priority.IMMEDIATE, new ProcessJoinsTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // Take into account the previous known nodes, if they happen not to be available
                // then fault detection will remove these nodes.

                if (currentState.nodes().masterNode() != null) {
                    // TODO can we tie break here? we don't have a remote master cluster state version to decide on
                    logger.trace("join thread elected local node as master, but there is already a master in place: {}", currentState.nodes().masterNode());
                    throw new NotMasterException("Node [" + clusterService.localNode() + "] not master for join request");
                }

                DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder(currentState.nodes()).masterNodeId(currentState.nodes().localNode().id());
                // update the fact that we are the master...
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(discoverySettings.getNoMasterBlock()).build();
                currentState = ClusterState.builder(currentState).nodes(builder).blocks(clusterBlocks).build();

                // add the incoming join requests and reroute
                return super.execute(currentState);
            }

            @Override
            public boolean runOnlyOnMaster() {
                return false;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                super.onFailure(source, t);
                context.callback.onFailure(t);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                super.clusterStateProcessed(source, oldState, newState);
                context.callback.onElectedAsMaster(newState);
            }
        });
    }

    private void processJoins(String reason) {
        clusterService.submitStateUpdateTask("zen-disco-join(" + reason + ")", Priority.URGENT, new ProcessJoinsTask());
    }


    public interface Callback {
        void onElectedAsMaster(ClusterState state);

        void onFailure(Throwable t);
    }

    static class ElectionContext {
        Callback callback;
        int requiredJoins;
    }


    class ProcessJoinsTask extends ProcessedClusterStateUpdateTask {

        private final List<MembershipAction.JoinCallback> joinCallbacksToRespondTo = new ArrayList<>();
        private boolean nodeAdded = false;

        @Override
        public ClusterState execute(ClusterState currentState) {
            DiscoveryNodes.Builder nodesBuilder;
            synchronized (pendingJoinRequests) {
                if (pendingJoinRequests.isEmpty()) {
                    return currentState;
                }

                nodesBuilder = DiscoveryNodes.builder(currentState.nodes());
                Iterator<Map.Entry<DiscoveryNode, List<MembershipAction.JoinCallback>>> iterator = pendingJoinRequests.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<DiscoveryNode, List<MembershipAction.JoinCallback>> entry = iterator.next();
                    final DiscoveryNode node = entry.getKey();
                    joinCallbacksToRespondTo.addAll(entry.getValue());
                    iterator.remove();
                    if (currentState.nodes().nodeExists(node.id())) {
                        logger.debug("received a join request for an existing node [{}]", node);
                    } else {
                        nodeAdded = true;
                        nodesBuilder.put(node);
                        for (DiscoveryNode existingNode : currentState.nodes()) {
                            if (node.address().equals(existingNode.address())) {
                                nodesBuilder.remove(existingNode.id());
                                logger.warn("received join request from node [{}], but found existing node {} with same address, removing existing node", node, existingNode);
                            }
                        }
                    }
                }
            }

            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize it's join and set us as a master
            final ClusterState.Builder newState = ClusterState.builder(currentState);
            if (nodeAdded) {
                newState.nodes(nodesBuilder);
            }

            return newState.build();
        }

        @Override
        public void onNoLongerMaster(String source) {
            // we are rejected, so drain all pending task (execute never run)
            synchronized (pendingJoinRequests) {
                Iterator<Map.Entry<DiscoveryNode, List<MembershipAction.JoinCallback>>> iterator = pendingJoinRequests.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<DiscoveryNode, List<MembershipAction.JoinCallback>> entry = iterator.next();
                    joinCallbacksToRespondTo.addAll(entry.getValue());
                    iterator.remove();
                }
            }
            Exception e = new NotMasterException("Node [" + clusterService.localNode() + "] not master for join request");
            innerOnFailure(e);
        }

        void innerOnFailure(Throwable t) {
            for (MembershipAction.JoinCallback callback : joinCallbacksToRespondTo) {
                try {
                    callback.onFailure(t);
                } catch (Exception e) {
                    logger.error("error during task failure", e);
                }
            }
        }

        @Override
        public void onFailure(String source, Throwable t) {
            logger.error("unexpected failure during [{}]", t, source);
            innerOnFailure(t);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            if (nodeAdded) {
                // we reroute not in the same cluster state update since in certain areas we rely on
                // the node to be in the cluster state (sampled from ClusterService#state) to be there, also
                // shard transitions need to better be handled in such cases
                routingService.reroute("post_node_add");
            }
            for (MembershipAction.JoinCallback callback : joinCallbacksToRespondTo) {
                try {
                    callback.onSuccess();
                } catch (Exception e) {
                    logger.error("unexpected error during [{}]", e, source);
                }
            }
        }
    }
}