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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.membership.MembershipAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class processes incoming join request (passed zia {@link ZenDiscovery}). Incoming nodes
 * are directly added to the cluster state or are accumulated during master election.
 */
public class NodeJoinController extends AbstractComponent {

    final ClusterService clusterService;
    final RoutingService routingService;
    final ElectMasterService electMaster;
    final DiscoverySettings discoverySettings;
    final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor();

    // this is set while trying to become a master mutation should be done under lock
    ElectionContext electionContext = null;



    public NodeJoinController(ClusterService clusterService, RoutingService routingService, ElectMasterService electMaster, DiscoverySettings discoverySettings, Settings settings) {
        super(settings);
        this.clusterService = clusterService;
        this.routingService = routingService;
        this.electMaster = electMaster;
        this.discoverySettings = discoverySettings;
    }

    /**
     * waits for enough incoming joins from master eligible nodes to complete the master election
     * <p>
     * You must start accumulating joins before calling this method. See {@link #startElectionContext()}
     * <p>
     * The method will return once the local node has been elected as master or some failure/timeout has happened.
     * The exact outcome is communicated via the callback parameter, which is guaranteed to be called.
     *
     * @param requiredMasterJoins the number of joins from master eligible needed to complete the election
     * @param timeValue           how long to wait before failing. a timeout is communicated via the callback's onFailure method.
     * @param callback            the result of the election (success or failure) will be communicated by calling methods on this
     *                            object
     **/
    public void waitToBeElectedAsMaster(int requiredMasterJoins, TimeValue timeValue, final ElectionCallback callback) {
        final CountDownLatch done = new CountDownLatch(1);
        final ElectionCallback wrapperCallback = new ElectionCallback() {
            @Override
            public void onElectedAsMaster(ClusterState state) {
                done.countDown();
                callback.onElectedAsMaster(state);
            }

            @Override
            public void onFailure(Throwable t) {
                done.countDown();
                callback.onFailure(t);
            }
        };


        // capture the context we add the callback to make sure we fail our own
        final ElectionContext myElectionContext;
        synchronized (this) {
            assert electionContext != null : "waitToBeElectedAsMaster is called we are not accumulating joins";
            electionContext.attemptToBeElected(requiredMasterJoins, wrapperCallback);
            myElectionContext = electionContext;
        }

        try {
            // check what we have so far..
            checkPendingJoinsAndElectIfNeeded();

            try {
                if (done.await(timeValue.millis(), TimeUnit.MILLISECONDS)) {
                    // callback handles everything
                    return;
                }
            } catch (InterruptedException e) {

            }
            if (logger.isTraceEnabled()) {
                final int pendingNodes;
                synchronized (this) {
                    pendingNodes = myElectionContext.getPendingMasterJoinsCount();
                }
                logger.trace("timed out waiting to be elected. waited [{}]. pending master node joins [{}]", timeValue, pendingNodes);
            }
            // callback will clear the context, if it's active
            failContext(myElectionContext, new ElasticsearchTimeoutException("timed out waiting to be elected"));
        } catch (Throwable t) {
            logger.error("unexpected failure while waiting for incoming joins", t);
            failContext(myElectionContext, "unexpected failure while waiting for pending joins", t);
        }
    }

    private void failContext(final ElectionContext context, final Throwable throwable) {
        failContext(context, throwable.getMessage(), throwable);
    }

    /** utility method to fail the given election context under the cluster state thread */
    private synchronized void failContext(final ElectionContext context, final String reason, final Throwable throwable) {
        if (electionContext == context) {
            // remove the context if still active
            electionContext = null;
        }
        clusterService.submitStateUpdateTask("zen-disco-join(failure [" + reason + "])", new ClusterStateUpdateTask(Priority.IMMEDIATE) {

            @Override
            public boolean runOnlyOnMaster() {
                return false;
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                context.onFailure(throwable);
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable updateFailure) {
                logger.warn("unexpected error while trying to fail election context due to [{}]. original exception [{}]", updateFailure, reason, throwable);
                context.onFailure(updateFailure);
            }
        });

    }

    /**
     * Accumulates any future incoming join request. Pending join requests will be processed in the final steps of becoming a
     * master or when {@link #stopElectionContext(String)} is called.
     */
    public synchronized void startElectionContext() {
        logger.trace("starting an election context, will accumulate joins");
        assert electionContext == null : "double startElectionContext() calls";
        electionContext = new ElectionContext(logger);
    }

    /**
     * Stopped accumulating joins. All pending joins will be processed. Future joins will be processed immediately
     */
    public void stopElectionContext(String reason) {
        logger.trace("stopping election ([{}])", reason);
        synchronized (this) {
            assert electionContext != null : "stopElectionContext() called but not accumulating";
            assert electionContext.getCallback() == null : "stopElectionContext is called, but we are also actively";
            clusterService.submitStateUpdateTasks("election stop [" + reason + "]",
                electionContext.getPendingAsTasks(), ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
            electionContext = null;
        }
    }

    /**
     * processes or queues an incoming join request.
     * <p>
     * Note: doesn't do any validation. This should have been done before.
     */
    public synchronized void handleJoinRequest(final DiscoveryNode node, final MembershipAction.JoinCallback callback) {
        if (electionContext != null) {
            electionContext.addIncomingJoin(node, callback);
            checkPendingJoinsAndElectIfNeeded();
        } else {
            clusterService.submitStateUpdateTask("zen-disco-join(node " + node + "])",
                node, ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new JoinTaskListener(callback, logger));
        }
    }

    /**
     * checks if there is an on going request to become master and if it has enough pending joins. If so, the node will
     * become master via a ClusterState update task.
     */
    private synchronized void checkPendingJoinsAndElectIfNeeded() {
        assert electionContext != null : "election check requested but no active context";
        final int pendingMasterJoins = electionContext.getPendingMasterJoinsCount();
        if (electionContext.isEnoughPendingJoins(pendingMasterJoins) == false) {
            if (logger.isTraceEnabled()) {
                logger.trace("not enough joins for election. Got [{}], required [{}]", pendingMasterJoins,
                    electionContext.requiredMasterJoins);
            }
        } else {
            final String source = "zen-disco-join(elected_as_master, [" + pendingMasterJoins + "] joins received)";
            ElectionContext context = electionContext;
            electionContext = null; // clear this out so future joins won't be accumulated
            Map<DiscoveryNode, ClusterStateTaskListener> tasks = context.getPendingAsTasks();
            tasks.put(BECOME_MASTER_TASK, new ClusterStateTaskListener() {

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    assert newState.nodes().isLocalNodeElectedMaster() : "should have become a master but isn't " + newState.prettyPrint();
                    context.onElectedAsMaster(newState);
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    context.onFailure(t);
                }
            });
            clusterService.submitStateUpdateTasks(source,
                tasks, ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor);
        }
    }

    public interface ElectionCallback {
        /**
         * called when the local node is successfully elected as master
         * Guaranteed to be called on the cluster state update thread
         **/
        void onElectedAsMaster(ClusterState state);

        /**
         * called when the local node failed to be elected as master
         * Guaranteed to be called on the cluster state update thread
         **/
        void onFailure(Throwable t);
    }

    static class ElectionContext implements ElectionCallback {
        private ElectionCallback callback = null;
        private int requiredMasterJoins = -1;
        private final Map<DiscoveryNode, List<MembershipAction.JoinCallback>> joinRequestAccumulator = new HashMap<>();

        final AtomicBoolean closed = new AtomicBoolean();
        final private ESLogger logger;

        ElectionContext(ESLogger logger) {
            this.logger = logger;
        }

        protected void onClose() {

        }

        public synchronized void attemptToBeElected(int requiredMasterJoins, ElectionCallback callback) {
            assert this.requiredMasterJoins < 0;
            assert this.callback == null;
            this.requiredMasterJoins = requiredMasterJoins;
            this.callback = callback;
        }

        public synchronized void addIncomingJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {
            joinRequestAccumulator.computeIfAbsent(node, n -> new ArrayList<>()).add(callback);
        }


        public synchronized boolean isEnoughPendingJoins(int pendingMasterJoins) {
            final boolean hasEngough;
            if (requiredMasterJoins < 0) {
                // requiredMasterNodes is unknown yet, return false and keep on waiting
                hasEngough = false;
            } else {
                assert callback != null : "requiredMasterJoins is set but not the callback";
                hasEngough = pendingMasterJoins >= requiredMasterJoins;
            }
            return hasEngough;
        }

        public synchronized Map<DiscoveryNode, ClusterStateTaskListener> getPendingAsTasks() {
            Map<DiscoveryNode, ClusterStateTaskListener> tasks = new HashMap<>();
            joinRequestAccumulator.entrySet().stream().forEach(e -> tasks.put(e.getKey(), new JoinTaskListener(e.getValue(), logger)));
            return tasks;
        }

        public synchronized int getPendingMasterJoinsCount() {
            int pendingMasterJoins = 0;
            for (DiscoveryNode node : joinRequestAccumulator.keySet()) {
                if (node.isMasterNode()) {
                    pendingMasterJoins++;
                }
            }
            return pendingMasterJoins;
        }

        private synchronized ElectionCallback getCallback() {
            return callback;
        }

        @Override
        public void onElectedAsMaster(ClusterState state) {
            ClusterService.assertClusterStateThread();
            assert state.nodes().isLocalNodeElectedMaster() : "onElectedAsMaster called but local node is not master";
            if (closed.compareAndSet(false, true)) {
                ElectionCallback callback = getCallback(); // get under lock
                try {
                    onClose();
                } finally {
                    callback.onElectedAsMaster(state);
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            ClusterService.assertClusterStateThread();
            if (closed.compareAndSet(false, true)) {
                ElectionCallback callback = getCallback(); // get under lock
                try {
                    onClose();
                } finally {
                    callback.onFailure(t);
                }
            }
        }
    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        final List<MembershipAction.JoinCallback> callbacks;
        final private ESLogger logger;

        JoinTaskListener(MembershipAction.JoinCallback callback, ESLogger logger) {
            this(Collections.singletonList(callback), logger);
        }

        JoinTaskListener(List<MembershipAction.JoinCallback> callbacks, ESLogger logger) {
            this.callbacks = callbacks;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Throwable t) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onFailure(t);
                } catch (Exception e) {
                    logger.error("error during task failure", e);
                }
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onSuccess();
                } catch (Exception e) {
                    logger.error("unexpected error during [{}]", e, source);
                }
            }
        }
    }

    // a task indicated that the current node should become master, if no current master is known
    private final static DiscoveryNode BECOME_MASTER_TASK = new DiscoveryNode("_BECOME_MASTER_TASK_", DummyTransportAddress.INSTANCE,
        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

    class JoinTaskExecutor implements ClusterStateTaskExecutor<DiscoveryNode> {

        @Override
        public BatchResult<DiscoveryNode> execute(ClusterState currentState, List<DiscoveryNode> joiningNodes) throws Exception {
            final DiscoveryNodes currentNodes = currentState.nodes();
            final BatchResult.Builder<DiscoveryNode> results = BatchResult.builder();
            boolean nodesChanged = false;
            ClusterState.Builder newState = ClusterState.builder(currentState);
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);

            if (currentNodes.getMasterNode() == null && joiningNodes.contains(BECOME_MASTER_TASK)) {
                // use these joins to try and become the master.
                // Note that we don't have to do any validation of the amount of joining nodes - the commit
                // during the cluster state publishing guarantees that we have enough

                nodesBuilder.masterNodeId(currentNodes.getLocalNodeId());
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks())
                    .removeGlobalBlock(discoverySettings.getNoMasterBlock()).build();
                newState.blocks(clusterBlocks);
                newState.nodes(nodesBuilder);
                nodesChanged = true;

                // reroute now to remove any dead nodes (master may have stepped down when they left and didn't update the routing table)
                // Note: also do it now to avoid assigning shards to these nodes. We will have another reroute after the cluster
                // state is published.
                // TODO: this publishing of a cluster state with no nodes assigned to joining nodes shouldn't be needed anymore. remove.

                final ClusterState tmpState = newState.build();
                RoutingAllocation.Result result = routingService.getAllocationService().reroute(tmpState, "nodes joined");
                newState = ClusterState.builder(tmpState);
                if (result.changed()) {
                    newState.routingResult(result);
                }
                nodesBuilder = DiscoveryNodes.builder(tmpState.nodes());
            }

            if (nodesBuilder.isLocalNodeElectedMaster() == false) {
                logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
                throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
            }

            for (final DiscoveryNode node : joiningNodes) {
                if (node.equals(BECOME_MASTER_TASK)) {
                    // noop
                } else if (currentNodes.nodeExists(node.getId())) {
                    logger.debug("received a join request for an existing node [{}]", node);
                } else {
                    nodesChanged = true;
                    nodesBuilder.put(node);
                    for (DiscoveryNode existingNode : currentNodes) {
                        if (node.getAddress().equals(existingNode.getAddress())) {
                            nodesBuilder.remove(existingNode.getId());
                            logger.warn("received join request from node [{}], but found existing node {} with same address, removing existing node", node, existingNode);
                        }
                    }
                }
                results.success(node);
            }

            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize it's join and set us as a master
            if (nodesChanged) {
                newState.nodes(nodesBuilder);
            }

            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize it's join and set us as a master

            return results.build(newState.build());
        }

        @Override
        public boolean runOnlyOnMaster() {
            // we validate that we are allowed to change the cluster state during cluster state processing
            return false;
        }

        @Override
        public void clusterStatePublished(ClusterChangedEvent event) {
            if (event.nodesDelta().hasChanges()) {
                // we reroute not in the same cluster state update since in certain areas we rely on
                // the node to be in the cluster state (sampled from ClusterService#state) to be there, also
                // shard transitions need to better be handled in such cases
                routingService.reroute("post_node_add");
            }

            NodeJoinController.this.electMaster.logMinimumMasterNodesWarningIfNecessary(event.previousState(), event.state());
        }
    }
}
