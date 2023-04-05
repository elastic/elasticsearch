/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.gateway.ClusterStateUpdaters;

import java.util.Optional;
import java.util.Set;

class StatelessPersistedState implements CoordinationState.PersistedState {
    private final ClusterStateStore clusterStateStore;
    private ClusterState latestAcceptedState;
    private long currentTerm;

    StatelessPersistedState(ClusterState latestAcceptedState, long currentTerm, ClusterStateStore clusterStateStore) {
        this.latestAcceptedState = latestAcceptedState;
        this.currentTerm = currentTerm;
        this.clusterStateStore = clusterStateStore;
    }

    @Override
    public long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    @Override
    public ClusterState getLastAcceptedState() {
        return latestAcceptedState;
    }

    @Override
    public void setLastAcceptedState(ClusterState clusterState) {
        if (clusterState.nodes().isLocalNodeElectedMaster()) {
            clusterStateStore.write(clusterState);
        }

        latestAcceptedState = clusterState;
    }

    @Override
    public void getLatestStoredState(long term, ActionListener<ClusterState> listener) {
        var getLatestTermAndVersionStep = new StepListener<Optional<PersistedClusterStateMetadata>>();
        var readStateStep = new StepListener<Optional<PersistedClusterState>>();

        getLatestTermAndVersionStep.whenComplete(stateMetadata -> {
            if (stateMetadata.isEmpty() || isLatestAcceptedStateStale(stateMetadata.get()) == false) {
                listener.onResponse(null);
                return;
            }

            clusterStateStore.read(stateMetadata.get().term(), readStateStep);
        }, listener::onFailure);

        readStateStep.whenComplete(persistedClusterStateOpt -> {
            if (persistedClusterStateOpt.isEmpty()) {
                listener.onFailure(new IllegalStateException("Unexpected empty state"));
                return;
            }
            var latestClusterState = persistedClusterStateOpt.get();
            assert latestClusterState.term() < currentTerm;

            final var adaptedClusterState = ClusterStateUpdaters.recoverClusterBlocks(
                ClusterStateUpdaters.addStateNotRecoveredBlock(
                    ClusterState.builder(latestAcceptedState.getClusterName())
                        .metadata(
                            Metadata.builder(latestClusterState.metadata())
                                .coordinationMetadata(
                                    new CoordinationMetadata(
                                        latestClusterState.term(),
                                        // Keep the previous configuration so the assertions don't complain about
                                        // a different committed configuration, we'll change it right away
                                        latestAcceptedState.getLastCommittedConfiguration(),
                                        latestAcceptedState.getLastAcceptedConfiguration(),
                                        Set.of()
                                    )
                                )
                        )
                        .version(latestClusterState.version())
                        .nodes(DiscoveryNodes.builder(latestAcceptedState.nodes()).masterNodeId(null))
                        .build()
                )
            );

            listener.onResponse(adaptedClusterState);
        }, listener::onFailure);

        clusterStateStore.getLatestStoredClusterStateMetadataForTerm(term - 1, getLatestTermAndVersionStep);
    }

    private boolean isLatestAcceptedStateStale(PersistedClusterStateMetadata latestStoredClusterState) {
        return latestStoredClusterState.clusterUUID().equals(latestAcceptedState.metadata().clusterUUID()) == false
            || latestStoredClusterState.term() > latestAcceptedState.term()
            || (latestStoredClusterState.term() == latestAcceptedState.term()
                && latestStoredClusterState.version() > latestAcceptedState.version());
    }
}
