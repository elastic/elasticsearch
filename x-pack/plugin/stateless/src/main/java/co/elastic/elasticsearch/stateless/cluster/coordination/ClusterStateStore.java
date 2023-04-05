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

import java.util.Optional;

public interface ClusterStateStore {
    void write(ClusterState clusterState);

    void read(long term, ActionListener<Optional<PersistedClusterState>> listener);

    void getLatestStoredClusterStateMetadataForTerm(long targetTerm, ActionListener<Optional<PersistedClusterStateMetadata>> listener);
}
