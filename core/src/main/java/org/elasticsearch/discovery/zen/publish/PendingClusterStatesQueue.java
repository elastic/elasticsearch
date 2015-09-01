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

    final ArrayList<PendingClusterState> pendingStates = new ArrayList<>();
    final ESLogger logger;

    public PendingClusterStatesQueue(ESLogger logger) {
        this.logger = logger;
    }

    public synchronized void addPending(ClusterState state) {
        pendingStates.add(new PendingClusterState(state));
    }

    public synchronized ClusterState markAsCommitted(String stateUUID, StateProcessedListener listener) {
        final int stateIndex = findState(stateUUID);
        if (stateIndex < 0) {
            listener.onNewClusterStateFailed(new IllegalStateException("can't resolve cluster state with uuid [" + stateUUID + "] to commit"));
            return null;
        }
        PendingClusterState pendingClusterState = pendingStates.get(stateIndex);
        if (pendingClusterState.committed()) {
            listener.onNewClusterStateFailed(new IllegalStateException("cluster state with uuid [" + stateUUID + "] is already committed"));
            return null;
        }
        pendingClusterState.markAsCommitted(listener);
        return pendingClusterState.clusterState;
    }

    public synchronized void markAsFailed(ClusterState state, Throwable reason) {
        final int failedIndex = findState(state.stateUUID());
        if (failedIndex < 0) {
            throw new IllegalStateException("can't resolve failed cluster state with uuid [" + state.stateUUID() + "], version [" + state.version() + "]");
        }
        assert pendingStates.get(failedIndex).committed() : "failed cluster state is not committed " + state;

        // fail or process all states *with the same master** up to and including the given one
        ArrayList<PendingClusterState> statesToRemove = new ArrayList<>();
        for (int index = 0; index < failedIndex; index++) {
            final StateProcessedListener pendingListener = pendingStates.get(index).listener;
            final ClusterState pendingState = pendingStates.get(index).clusterState;
            final DiscoveryNode pendingMasterNode = pendingState.nodes().masterNode();
            final DiscoveryNode currentMaster = state.nodes().masterNode();
            if (Objects.equals(currentMaster, pendingMasterNode) == true) {
                assert pendingState.version() <= state.version() :
                        "found a pending state with version [" + pendingState.version() + "] before processed state with version [" + state.version() + "]";
                statesToRemove.add(pendingStates.get(index));
                if (pendingListener != null) {
                    logger.debug("failing committed state uuid[{}]/v[{}] together with state uuid[{}]/v[{}]",
                            pendingState.stateUUID(), pendingState.version(), state.stateUUID(), state.version()
                    );
                    pendingListener.onNewClusterStateFailed(reason);
                } else {
                    logger.trace("failing non-committed pending state uuid[{}]/v[{}] together with state uuid[{}]/v[{}]",
                            pendingState.stateUUID(), pendingState.version(), state.stateUUID(), state.version()
                    );
                }
            }
        }

        // now fail the given state
        statesToRemove.add(pendingStates.get(failedIndex));
        pendingStates.get(failedIndex).listener.onNewClusterStateFailed(reason);
        pendingStates.removeAll(statesToRemove);
    }

    public synchronized void markAsProccessed(ClusterState state) {
        final int processedIndex = findState(state.stateUUID());
        if (processedIndex < 0) {
            throw new IllegalStateException("can't resolve processed cluster state with uuid [" + state.stateUUID() + "], version [" + state.version() + "]");
        }
        assert pendingStates.get(processedIndex).committed() : "processed cluster state is not committed " + state;
        // fail or process all states up to and including the given one
        for (int index = 0; index < processedIndex; index++) {
            final StateProcessedListener pendingListener = pendingStates.get(index).listener;
            final ClusterState pendingState = pendingStates.get(index).clusterState;
            final DiscoveryNode pendingMasterNode = pendingState.nodes().masterNode();
            final DiscoveryNode currentMaster = state.nodes().masterNode();
            if (Objects.equals(currentMaster, pendingMasterNode) == false) {
                if (pendingListener != null) {
                    // this is a committed state , warn
                    logger.warn("received a cluster state (uuid[{}]/v[{}]) from a different master than the current one, rejecting (received {}, current {})",
                            pendingState.stateUUID(), pendingState.version(),
                            pendingMasterNode, currentMaster);
                    pendingListener.onNewClusterStateFailed(
                            new IllegalStateException("cluster state from a different master than the current one, rejecting (received " + pendingMasterNode + ", current " + currentMaster + ")")
                    );
                } else {
                    logger.trace("removing non-committed state with uuid[{}]/v[{}] from [{}] - a state from [{}] was successfully processed",
                            pendingState.stateUUID(), pendingState.version(), pendingMasterNode,
                            currentMaster
                    );
                }
            } else {
                assert pendingState.version() <= state.version() :
                        "found a pending state with version [" + pendingState.version() + "] before processed state with version [" + state.version() + "]";
                logger.trace("processing pending state uuid[{}]/v[{}] together with state uuid[{}]/v[{}]",
                        pendingState.stateUUID(), pendingState.version(), state.stateUUID(), state.version()
                );
                if (pendingListener != null) {
                    pendingListener.onNewClusterStateProcessed();
                }
            }
        }
        // now ack the processed state
        pendingStates.get(processedIndex).listener.onNewClusterStateProcessed();
        pendingStates.subList(0, processedIndex + 1).clear();
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
        for (PendingClusterState pendingState : pendingStates) {
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

        PendingClusterState stateToProcess = null;
        int index = 0;
        for (; index < pendingStates.size(); index++) {
            PendingClusterState potentialState = pendingStates.get(index);
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
            PendingClusterState potentialState = pendingStates.get(index);
            // if its not from the same master, then bail
            if (!Objects.equals(stateToProcess.clusterState.nodes().masterNodeId(), potentialState.clusterState.nodes().masterNodeId())) {
                break;
            }

            if (potentialState.clusterState.version() > stateToProcess.clusterState.version() && potentialState.committed()) {
                // we found a new one
                stateToProcess = potentialState;
            }
        }
        assert stateToProcess.committed() : "should only return committed cluster state. found " + stateToProcess.clusterState;
        return stateToProcess.clusterState;
    }

    static class PendingClusterState {
        final ClusterState clusterState;
        StateProcessedListener listener;

        PendingClusterState(ClusterState clusterState) {
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
