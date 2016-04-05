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
package org.elasticsearch.discovery.zen.publish;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Objects;

/**
 * A queue that holds all "in-flight" incoming cluster states from the master. Once a master commits a cluster
 * state, it is made available via {@link #getNextClusterStateToProcess()}. The class also takes care of batching
 * cluster states for processing and failures.
 * <p>
 * The queue is bound by {@link #maxQueueSize}. When the queue is at capacity and a new cluster state is inserted
 * the oldest cluster state will be dropped. This is safe because:
 * 1) Under normal operations, master will publish &amp; commit a cluster state before processing another change (i.e., the queue length is 1)
 * 2) If the master fails to commit a change, it will step down, causing a master election, which will flush the queue.
 * 3) In general it's safe to process the incoming cluster state as a replacement to the cluster state that's dropped.
 * a) If the dropped cluster is from the same master as the incoming one is, it is likely to be superseded by the incoming state (or another state in the queue).
 * This is only not true in very extreme cases of out of order delivery.
 * b) If the dropping cluster state is not from the same master, it means that:
 * i) we are no longer following the master of the dropped cluster state but follow the incoming one
 * ii) we are no longer following any master, in which case it doesn't matter which cluster state will be processed first.
 * <p>
 * The class is fully thread safe and can be used concurrently.
 */
public class PendingClusterStatesQueue {

    interface StateProcessedListener {

        void onNewClusterStateProcessed();

        void onNewClusterStateFailed(Throwable t);
    }

    final ArrayList<ClusterStateContext> pendingStates = new ArrayList<>();
    final ESLogger logger;
    final int maxQueueSize;

    public PendingClusterStatesQueue(ESLogger logger, int maxQueueSize) {
        this.logger = logger;
        this.maxQueueSize = maxQueueSize;
    }

    /** Add an incoming, not yet committed cluster state */
    public synchronized void addPending(ClusterState state) {
        pendingStates.add(new ClusterStateContext(state));
        if (pendingStates.size() > maxQueueSize) {
            ClusterStateContext context = pendingStates.remove(0);
            logger.warn("dropping pending state [{}]. more than [{}] pending states.", context, maxQueueSize);
            if (context.committed()) {
                context.listener.onNewClusterStateFailed(new ElasticsearchException("too many pending states ([{}] pending)", maxQueueSize));
            }
        }
    }

    /**
     * Mark a previously added cluster state as committed. This will make it available via {@link #getNextClusterStateToProcess()}
     * When the cluster state is processed (or failed), the supplied listener will be called
     **/
    public synchronized ClusterState markAsCommitted(String stateUUID, StateProcessedListener listener) {
        final ClusterStateContext context = findState(stateUUID);
        if (context == null) {
            listener.onNewClusterStateFailed(new IllegalStateException("can't resolve cluster state with uuid [" + stateUUID + "] to commit"));
            return null;
        }
        if (context.committed()) {
            listener.onNewClusterStateFailed(new IllegalStateException("cluster state with uuid [" + stateUUID + "] is already committed"));
            return null;
        }
        context.markAsCommitted(listener);
        return context.state;
    }

    /**
     * mark that the processing of the given state has failed. All committed states that are {@link ClusterState#supersedes(ClusterState)}-ed
     * by this failed state, will be failed as well
     */
    public synchronized void markAsFailed(ClusterState state, Throwable reason) {
        final ClusterStateContext failedContext = findState(state.stateUUID());
        if (failedContext == null) {
            throw new IllegalArgumentException("can't resolve failed cluster state with uuid [" + state.stateUUID() + "], version [" + state.version() + "]");
        }
        if (failedContext.committed() == false) {
            throw new IllegalArgumentException("failed cluster state is not committed " + state);
        }

        // fail all committed states which are batch together with the failed state
        ArrayList<ClusterStateContext> statesToRemove = new ArrayList<>();
        for (int index = 0; index < pendingStates.size(); index++) {
            final ClusterStateContext pendingContext = pendingStates.get(index);
            if (pendingContext.committed() == false) {
                continue;
            }
            final ClusterState pendingState = pendingContext.state;
            if (pendingContext.equals(failedContext)) {
                statesToRemove.add(pendingContext);
                pendingContext.listener.onNewClusterStateFailed(reason);
            } else if (state.supersedes(pendingState)) {
                statesToRemove.add(pendingContext);
                logger.debug("failing committed state {} together with state {}", pendingContext, failedContext);
                pendingContext.listener.onNewClusterStateFailed(reason);
            }
        }
        pendingStates.removeAll(statesToRemove);
        assert findState(state.stateUUID()) == null : "state was marked as processed but can still be found in pending list " + state;
    }

    /**
     * indicates that a cluster state was successfully processed. Any committed state that is {@link ClusterState#supersedes(ClusterState)}-ed
     * by the processed state will be marked as processed as well.
     * <p>
     * NOTE: successfully processing a state indicates we are following the master it came from. Any committed state from another master will
     * be failed by this method
     */
    public synchronized void markAsProcessed(ClusterState state) {
        if (findState(state.stateUUID()) == null) {
            throw new IllegalStateException("can't resolve processed cluster state with uuid [" + state.stateUUID() + "], version [" + state.version() + "]");
        }
        final DiscoveryNode currentMaster = state.nodes().getMasterNode();
        assert currentMaster != null : "processed cluster state mast have a master. " + state;

        // fail or remove any incoming state from a different master
        // respond to any committed state from the same master with same or lower version (we processed a higher version)
        ArrayList<ClusterStateContext> contextsToRemove = new ArrayList<>();
        for (int index = 0; index < pendingStates.size(); index++) {
            final ClusterStateContext pendingContext = pendingStates.get(index);
            final ClusterState pendingState = pendingContext.state;
            final DiscoveryNode pendingMasterNode = pendingState.nodes().getMasterNode();
            if (Objects.equals(currentMaster, pendingMasterNode) == false) {
                contextsToRemove.add(pendingContext);
                if (pendingContext.committed()) {
                    // this is a committed state , warn
                    logger.warn("received a cluster state (uuid[{}]/v[{}]) from a different master than the current one, rejecting (received {}, current {})",
                            pendingState.stateUUID(), pendingState.version(),
                            pendingMasterNode, currentMaster);
                    pendingContext.listener.onNewClusterStateFailed(
                            new IllegalStateException("cluster state from a different master than the current one, rejecting (received " + pendingMasterNode + ", current " + currentMaster + ")")
                    );
                } else {
                    logger.trace("removing non-committed state with uuid[{}]/v[{}] from [{}] - a state from [{}] was successfully processed",
                            pendingState.stateUUID(), pendingState.version(), pendingMasterNode,
                            currentMaster
                    );
                }
            } else if (pendingState.stateUUID().equals(state.stateUUID())) {
                assert pendingContext.committed() : "processed cluster state is not committed " + state;
                contextsToRemove.add(pendingContext);
                pendingContext.listener.onNewClusterStateProcessed();
            } else if (state.version() >= pendingState.version()) {
                logger.trace("processing pending state uuid[{}]/v[{}] together with state uuid[{}]/v[{}]",
                        pendingState.stateUUID(), pendingState.version(), state.stateUUID(), state.version()
                );
                contextsToRemove.add(pendingContext);
                if (pendingContext.committed()) {
                    pendingContext.listener.onNewClusterStateProcessed();
                }
            }
        }
        // now ack the processed state
        pendingStates.removeAll(contextsToRemove);
        assert findState(state.stateUUID()) == null : "state was marked as processed but can still be found in pending list " + state;

    }

    ClusterStateContext findState(String stateUUID) {
        for (int i = 0; i < pendingStates.size(); i++) {
            final ClusterStateContext context = pendingStates.get(i);
            if (context.stateUUID().equals(stateUUID)) {
                return context;
            }
        }
        return null;
    }

    /** clear the incoming queue. any committed state will be failed */
    public synchronized void failAllStatesAndClear(Throwable reason) {
        for (ClusterStateContext pendingState : pendingStates) {
            if (pendingState.committed()) {
                pendingState.listener.onNewClusterStateFailed(reason);
            }
        }
        pendingStates.clear();
    }

    /**
     * Gets the next committed state to process.
     * <p>
     * The method tries to batch operation by getting the cluster state the highest possible committed states
     * which succeeds the first committed state in queue (i.e., it comes from the same master).
     */
    public synchronized ClusterState getNextClusterStateToProcess() {
        if (pendingStates.isEmpty()) {
            return null;
        }

        ClusterStateContext stateToProcess = null;
        int index = 0;
        for (; index < pendingStates.size(); index++) {
            ClusterStateContext potentialState = pendingStates.get(index);
            if (potentialState.committed()) {
                stateToProcess = potentialState;
                break;
            }
        }
        if (stateToProcess == null) {
            return null;
        }

        // now try to find the highest committed state from the same master
        for (; index < pendingStates.size(); index++) {
            ClusterStateContext potentialState = pendingStates.get(index);

            if (potentialState.state.supersedes(stateToProcess.state) && potentialState.committed()) {
                // we found a new one
                stateToProcess = potentialState;
            }
        }
        assert stateToProcess.committed() : "should only return committed cluster state. found " + stateToProcess.state;
        return stateToProcess.state;
    }

    /** returns all pending states, committed or not */
    public synchronized ClusterState[] pendingClusterStates() {
        ArrayList<ClusterState> states = new ArrayList<>();
        for (ClusterStateContext context : pendingStates) {
            states.add(context.state);
        }
        return states.toArray(new ClusterState[states.size()]);
    }

    static class ClusterStateContext {
        final ClusterState state;
        StateProcessedListener listener;

        ClusterStateContext(ClusterState clusterState) {
            this.state = clusterState;
        }

        void markAsCommitted(StateProcessedListener listener) {
            if (this.listener != null) {
                throw new IllegalStateException(toString() + "is already committed");
            }
            this.listener = listener;
        }

        boolean committed() {
            return listener != null;
        }

        public String stateUUID() {
            return state.stateUUID();
        }

        @Override
        public String toString() {
            return String.format(
                    Locale.ROOT,
                    "[uuid[%s], v[%d], m[%s]]",
                    stateUUID(),
                    state.version(),
                    state.nodes().getMasterNodeId()
            );
        }
    }

    public synchronized PendingClusterStateStats stats() {

        // calculate committed cluster state
        int committed = 0;
        for (ClusterStateContext clusterStatsContext : pendingStates) {
            if (clusterStatsContext.committed()) {
                committed += 1;
            }
        }

        return new PendingClusterStateStats(pendingStates.size(), pendingStates.size() - committed, committed);
    }

}
