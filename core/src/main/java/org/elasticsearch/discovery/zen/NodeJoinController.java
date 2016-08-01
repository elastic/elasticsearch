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

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
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

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final ElectMasterService electMaster;
    private final DiscoverySettings discoverySettings;
    private final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor();

    // this is set while trying to become a master
    // mutation should be done under lock
    private ElectionContext electionContext = null;


    public NodeJoinController(ClusterService clusterService, AllocationService allocationService, ElectMasterService electMaster,
                              DiscoverySettings discoverySettings, Settings settings) {
        super(settings);
        this.clusterService = clusterService;
        this.allocationService = allocationService;
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

        ElectionContext myElectionContext = null;

        try {
            // check what we have so far..
            // capture the context we add the callback to make sure we fail our own
            synchronized (this) {
                assert electionContext != null : "waitToBeElectedAsMaster is called we are not accumulating joins";
                myElectionContext = electionContext;
                electionContext.onAttemptToBeElected(requiredMasterJoins, wrapperCallback);
                checkPendingJoinsAndElectIfNeeded();
            }

            try {
                if (done.await(timeValue.millis(), TimeUnit.MILLISECONDS)) {
                    // callback handles everything
                    return;
                }
            } catch (InterruptedException e) {

            }
            if (logger.isTraceEnabled()) {
                final int pendingNodes = myElectionContext.getPendingMasterJoinsCount();
                logger.trace("timed out waiting to be elected. waited [{}]. pending master node joins [{}]", timeValue, pendingNodes);
            }
            failContextIfNeeded(myElectionContext, "timed out waiting to be elected");
        } catch (Exception e) {
            logger.error("unexpected failure while waiting for incoming joins", e);
            if (myElectionContext != null) {
                failContextIfNeeded(myElectionContext, "unexpected failure while waiting for pending joins [" + e.getMessage() + "]");
            }
        }
    }

    /**
     * utility method to fail the given election context under the cluster state thread
     */
    private synchronized void failContextIfNeeded(final ElectionContext context, final String reason) {
        if (electionContext == context) {
            stopElectionContext(reason);
        }
    }

    /**
     * Accumulates any future incoming join request. Pending join requests will be processed in the final steps of becoming a
     * master or when {@link #stopElectionContext(String)} is called.
     */
    public synchronized void startElectionContext() {
        logger.trace("starting an election context, will accumulate joins");
        assert electionContext == null : "double startElectionContext() calls";
        electionContext = new ElectionContext();
    }

    /**
     * Stopped accumulating joins. All pending joins will be processed. Future joins will be processed immediately
     */
    public void stopElectionContext(String reason) {
        logger.trace("stopping election ([{}])", reason);
        synchronized (this) {
            assert electionContext != null : "stopElectionContext() called but not accumulating";
            electionContext.closeAndProcessPending(reason);
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
            clusterService.submitStateUpdateTask("zen-disco-node-join",
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
            if (logger.isTraceEnabled()) {
                logger.trace("have enough joins for election. Got [{}], required [{}]", pendingMasterJoins,
                    electionContext.requiredMasterJoins);
            }
            electionContext.closeAndBecomeMaster();
            electionContext = null; // clear this out so future joins won't be accumulated
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

    class ElectionContext {
        private ElectionCallback callback = null;
        private int requiredMasterJoins = -1;
        private final Map<DiscoveryNode, List<MembershipAction.JoinCallback>> joinRequestAccumulator = new HashMap<>();

        final AtomicBoolean closed = new AtomicBoolean();

        public synchronized void onAttemptToBeElected(int requiredMasterJoins, ElectionCallback callback) {
            ensureOpen();
            assert this.requiredMasterJoins < 0;
            assert this.callback == null;
            this.requiredMasterJoins = requiredMasterJoins;
            this.callback = callback;
        }

        public synchronized void addIncomingJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {
            ensureOpen();
            joinRequestAccumulator.computeIfAbsent(node, n -> new ArrayList<>()).add(callback);
        }


        public synchronized boolean isEnoughPendingJoins(int pendingMasterJoins) {
            final boolean hasEnough;
            if (requiredMasterJoins < 0) {
                // requiredMasterNodes is unknown yet, return false and keep on waiting
                hasEnough = false;
            } else {
                assert callback != null : "requiredMasterJoins is set but not the callback";
                hasEnough = pendingMasterJoins >= requiredMasterJoins;
            }
            return hasEnough;
        }

        private Map<DiscoveryNode, ClusterStateTaskListener> getPendingAsTasks() {
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

        public synchronized void closeAndBecomeMaster() {
            assert callback != null : "becoming a master but the callback is not yet set";
            assert isEnoughPendingJoins(getPendingMasterJoinsCount()) : "becoming a master but pending joins of "
                + getPendingMasterJoinsCount() + " are not enough. needs [" + requiredMasterJoins + "];";

            innerClose();

            Map<DiscoveryNode, ClusterStateTaskListener> tasks = getPendingAsTasks();
            final String source = "zen-disco-elected-as-master ([" + tasks.size() + "] nodes joined)";

            tasks.put(BECOME_MASTER_TASK, (source1, e) -> {}); // noop listener, the election finished listener determines result
            tasks.put(FINISH_ELECTION_TASK, electionFinishedListener);
            clusterService.submitStateUpdateTasks(source, tasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }

        public synchronized void closeAndProcessPending(String reason) {
            innerClose();
            Map<DiscoveryNode, ClusterStateTaskListener> tasks = getPendingAsTasks();
            final String source = "zen-disco-election-stop [" + reason + "]";
            tasks.put(FINISH_ELECTION_TASK, electionFinishedListener);
            clusterService.submitStateUpdateTasks(source, tasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }

        private void innerClose() {
            if (closed.getAndSet(true)) {
                throw new AlreadyClosedException("election context is already closed");
            }
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new AlreadyClosedException("election context is already closed");
            }
        }

        private synchronized ElectionCallback getCallback() {
            return callback;
        }

        private void onElectedAsMaster(ClusterState state) {
            ClusterService.assertClusterStateThread();
            assert state.nodes().isLocalNodeElectedMaster() : "onElectedAsMaster called but local node is not master";
            ElectionCallback callback = getCallback(); // get under lock
            if (callback != null) {
                callback.onElectedAsMaster(state);
            }
        }

        private void onFailure(Throwable t) {
            ClusterService.assertClusterStateThread();
            ElectionCallback callback = getCallback(); // get under lock
            if (callback != null) {
                callback.onFailure(t);
            }
        }

        private final ClusterStateTaskListener electionFinishedListener = new ClusterStateTaskListener() {

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (newState.nodes().isLocalNodeElectedMaster()) {
                    ElectionContext.this.onElectedAsMaster(newState);
                } else {
                    onFailure(source, new NotMasterException("election stopped [" + source + "]"));
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                ElectionContext.this.onFailure(e);
            }
        };

    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        final List<MembershipAction.JoinCallback> callbacks;
        private final ESLogger logger;

        JoinTaskListener(MembershipAction.JoinCallback callback, ESLogger logger) {
            this(Collections.singletonList(callback), logger);
        }

        JoinTaskListener(List<MembershipAction.JoinCallback> callbacks, ESLogger logger) {
            this.callbacks = callbacks;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onFailure(e);
                } catch (Exception inner) {
                    logger.error("error handling task failure [{}]", inner, e);
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

    /**
     * a task indicated that the current node should become master, if no current master is known
     */
    private static final DiscoveryNode BECOME_MASTER_TASK = new DiscoveryNode("_BECOME_MASTER_TASK_", LocalTransportAddress.buildUnique(),
        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT) {
        @Override
        public String toString() {
            return ""; // this is not really task , so don't log anything about it...
        }
    };

    /**
     * a task that is used to signal the election is stopped and we should process pending joins.
     * it may be use in combination with {@link #BECOME_MASTER_TASK}
     */
    private static final DiscoveryNode FINISH_ELECTION_TASK = new DiscoveryNode("_FINISH_ELECTION_",
        LocalTransportAddress.buildUnique(), Collections.emptyMap(), Collections.emptySet(), Version.CURRENT) {
            @Override
            public String toString() {
                return ""; // this is not really task , so don't log anything about it...
            }
    };

    class JoinTaskExecutor implements ClusterStateTaskExecutor<DiscoveryNode> {

        @Override
        public BatchResult<DiscoveryNode> execute(ClusterState currentState, List<DiscoveryNode> joiningNodes) throws Exception {
            final BatchResult.Builder<DiscoveryNode> results = BatchResult.builder();

            final DiscoveryNodes currentNodes = currentState.nodes();
            boolean nodesChanged = false;
            ClusterState.Builder newState;

            if (joiningNodes.size() == 1  && joiningNodes.get(0).equals(FINISH_ELECTION_TASK)) {
                return results.successes(joiningNodes).build(currentState);
            } else if (currentNodes.getMasterNode() == null && joiningNodes.contains(BECOME_MASTER_TASK)) {
                assert joiningNodes.contains(FINISH_ELECTION_TASK) : "becoming a master but election is not finished " + joiningNodes;
                // use these joins to try and become the master.
                // Note that we don't have to do any validation of the amount of joining nodes - the commit
                // during the cluster state publishing guarantees that we have enough
                newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);
                nodesChanged = true;
            } else if (currentNodes.isLocalNodeElectedMaster() == false) {
                logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
                throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
            } else {
                newState = ClusterState.builder(currentState);
            }

            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

            assert nodesBuilder.isLocalNodeElectedMaster();

            // processing any joins
            for (final DiscoveryNode node : joiningNodes) {
                if (node.equals(BECOME_MASTER_TASK) || node.equals(FINISH_ELECTION_TASK)) {
                    // noop
                } else if (currentNodes.nodeExists(node)) {
                    logger.debug("received a join request for an existing node [{}]", node);
                } else {
                    try {
                        nodesBuilder.add(node);
                        nodesChanged = true;
                    } catch (IllegalArgumentException e) {
                        results.failure(node, e);
                        continue;
                    }
                }
                results.success(node);
            }

            if (nodesChanged) {
                newState.nodes(nodesBuilder);
                final ClusterState tmpState = newState.build();
                RoutingAllocation.Result result = allocationService.reroute(tmpState, "node_join");
                newState = ClusterState.builder(tmpState);
                if (result.changed()) {
                    newState.routingResult(result);
                }
            }

            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize its join and set us as a master
            return results.build(newState.build());
        }

        private ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<DiscoveryNode> joiningNodes) {
            assert currentState.nodes().getMasterNodeId() == null : currentState.prettyPrint();
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentState.nodes());
            nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());
            ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks())
                .removeGlobalBlock(discoverySettings.getNoMasterBlock()).build();
            for (final DiscoveryNode joiningNode : joiningNodes) {
                final DiscoveryNode existingNode = nodesBuilder.get(joiningNode.getId());
                if (existingNode != null && existingNode.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", existingNode, joiningNode);
                    nodesBuilder.remove(existingNode.getId());
                }
            }

            // now trim any left over dead nodes - either left there when the previous master stepped down
            // or removed by us above
            ClusterState tmpState = ClusterState.builder(currentState).nodes(nodesBuilder).blocks(clusterBlocks).build();
            RoutingAllocation.Result result = allocationService.deassociateDeadNodes(tmpState, false,
                "removed dead nodes on election");
            return ClusterState.builder(tmpState).routingResult(result);
        }

        @Override
        public boolean runOnlyOnMaster() {
            // we validate that we are allowed to change the cluster state during cluster state processing
            return false;
        }

        @Override
        public void clusterStatePublished(ClusterChangedEvent event) {
            NodeJoinController.this.electMaster.logMinimumMasterNodesWarningIfNecessary(event.previousState(), event.state());
        }
    }
}
