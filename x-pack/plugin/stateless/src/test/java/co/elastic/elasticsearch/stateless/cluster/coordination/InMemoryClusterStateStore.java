/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class InMemoryClusterStateStore implements ClusterStateStore {
    private final Map<Long, PersistedClusterState> clusterStateByTerm = new HashMap<>();

    @Override
    public void write(ClusterState clusterState) {
        clusterStateByTerm.put(
            clusterState.term(),
            new PersistedClusterState(
                clusterState.term(),
                clusterState.version(),
                clusterState.metadata().clusterUUID(),
                clusterState.metadata()
            )
        );
    }

    @Override
    public void read(long targetTerm, ActionListener<Optional<PersistedClusterState>> listener) {
        ActionListener.completeWith(listener, () -> getMostRecentClusterState(targetTerm));
    }

    @Override
    public void getLatestStoredClusterStateMetadataForTerm(
        long targetTerm,
        ActionListener<Optional<PersistedClusterStateMetadata>> listener
    ) {
        ActionListener.completeWith(
            listener,
            () -> getMostRecentClusterState(targetTerm).map(
                persistedClusterState -> new PersistedClusterStateMetadata(
                    persistedClusterState.term(),
                    persistedClusterState.version(),
                    persistedClusterState.metadata().clusterUUID()
                )
            )
        );
    }

    private Optional<PersistedClusterState> getMostRecentClusterState(long targetTerm) {
        for (long term = targetTerm; term > 0; term--) {
            var valueInTerm = clusterStateByTerm.get(term);
            if (valueInTerm != null) {
                return Optional.of(valueInTerm);
            }
        }

        return Optional.empty();
    }
}
