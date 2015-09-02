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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;

import java.util.ArrayList;
import java.util.Objects;

public class PendingClusterStatesQueue {

    interface StateProcessedListener {

        void onNewClusterStateProcessed();

        void onNewClusterStateFailed(Throwable t);
    }

    final ArrayList<ClusterStateContext> pendingStates = new ArrayList<>();
    final ESLogger logger;

    public PendingClusterStatesQueue(ESLogger logger) {
        this.logger = logger;
    }

    public synchronized void addPending(ClusterState state) {
        pendingStates.add(new ClusterStateContext(state));
    }

    public synchronized ClusterState markAsCommitted(String stateUUID, StateProcessedListener listener) {
        final int stateIndex = findState(stateUUID);
        if (stateIndex < 0) {
            listener.onNewClusterStateFailed(new IllegalStateException("can't resolve cluster state with uuid [" + stateUUID + "] to commit"));
            return null;
        }
        ClusterStateContext context = pendingStates.get(stateIndex);
        if (context.committed()) {
            listener.onNewClusterStateFailed(new IllegalStateException("cluster state with uuid [" + stateUUID + "] is already committed"));
            return null;
        }
        context.markAsCommitted(listener);
        return context.clusterState;
    }

    public synchronized void markAsFailed(ClusterState state, Throwable reason) {
        final int failedIndex = findState(state.stateUUID());
        if (failedIndex < 0) {
            throw new IllegalStateException("can't resolve failed cluster state with uuid [" + state.stateUUID() + "], version [" + state.version() + "]");
        }
        assert pendingStates.get(failedIndex).committed() : "failed cluster state is not committed " + state;

        // fail all committed states which are batch together with the failed state
        ArrayList<ClusterStateContext> statesToRemove = new ArrayList<>();
        for (int index = 0; index < pendingStates.size(); index++) {
            final ClusterStateContext pendingContext = pendingStates.get(index);
            if (pendingContext.committed() == false) {
                continue;
            }
            final ClusterState pendingState = pendingContext.clusterState;
            if (pendingState.equals(state)) {
                statesToRemove.add(pendingContext);
                pendingContext.listener.onNewClusterStateFailed(reason);
            } else if (state.supersedes(pendingState)) {
                statesToRemove.add(pendingContext);
                logger.debug("failing committed state uuid[{}]/v[{}] together with state uuid[{}]/v[{}]",
                        pendingState.stateUUID(), pendingState.version(), state.stateUUID(), state.version()
                );
                pendingContext.listener.onNewClusterStateFailed(reason);
            }
        }
        pendingStates.removeAll(statesToRemove);
        assert findState(state.stateUUID()) == -1 : "state was marked as processed but can still be found in pending list " + state;
    }

    public synchronized void markAsProcessed(ClusterState state) {
        final int processedIndex = findState(state.stateUUID());
        if (processedIndex < 0) {
            throw new IllegalStateException("can't resolve processed cluster state with uuid [" + state.stateUUID() + "], version [" + state.version() + "]");
        }
        final DiscoveryNode currentMaster = state.nodes().masterNode();
        assert currentMaster != null : "processed cluster state mast have a master. " + state;

        // fail or remove any incoming state from a different master
        // respond to any committed state from the same master with same or lower version (we processed a higher version)
        ArrayList<ClusterStateContext> contextsToRemove = new ArrayList<>();
        for (int index = 0; index < pendingStates.size(); index++) {
            final ClusterStateContext pendingContext = pendingStates.get(index);
            final ClusterState pendingState = pendingContext.clusterState;
            final DiscoveryNode pendingMasterNode = pendingState.nodes().masterNode();
            if (Objects.equals(currentMaster, pendingMasterNode) == false) {
                contextsToRemove.add(pendingContext);
                if (pendingContext.listener != null) {
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
            } else if (state.supersedes(pendingState) && pendingContext.committed()) {
                    logger.trace("processing pending state uuid[{}]/v[{}] together with state uuid[{}]/v[{}]",
                            pendingState.stateUUID(), pendingState.version(), state.stateUUID(), state.version()
                    );
                contextsToRemove.add(pendingContext);
                pendingContext.listener.onNewClusterStateProcessed();
            } else if (pendingState.equals(state)) {
                assert pendingStates.get(processedIndex).committed() : "processed cluster state is not committed " + state;
                contextsToRemove.add(pendingContext);
                pendingContext.listener.onNewClusterStateProcessed();
            }
        }
        // now ack the processed state
        pendingStates.removeAll(contextsToRemove);
        assert findState(state.stateUUID()) == -1 : "state was marked as processed but can still be found in pending list " + state;

    }

    private int findState(String stateUUID) {
        for (int i = 0; i < pendingStates.size(); i++) {
            if (pendingStates.get(i).clusterState.stateUUID().equals(stateUUID)) {
                return i;
            }
        }
        return -1;
    }

    public synchronized void failAllStatesAndClear(Throwable reason) {
        for (ClusterStateContext pendingState : pendingStates) {
            if (pendingState.committed()) {
                pendingState.listener.onNewClusterStateFailed(reason);
            }
        }
        pendingStates.clear();
    }

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

            if (potentialState.clusterState.supersedes(stateToProcess.clusterState) && potentialState.committed()) {
                // we found a new one
                stateToProcess = potentialState;
            }
        }
        assert stateToProcess.committed() : "should only return committed cluster state. found " + stateToProcess.clusterState;
        return stateToProcess.clusterState;
    }

    static class ClusterStateContext {
        final ClusterState clusterState;
        StateProcessedListener listener;

        ClusterStateContext(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        void markAsCommitted(StateProcessedListener listener) {
            assert this.listener == null : "double committing of " + clusterState;
            this.listener = listener;
        }

        boolean committed() {
            return listener != null;
        }
    }

}
