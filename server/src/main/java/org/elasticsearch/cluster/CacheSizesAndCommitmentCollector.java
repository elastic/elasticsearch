/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;

/**
 * Collects node cache commitments, node cache size and individual shard cache requirements for {@link ClusterInfo}.
 */
public interface CacheSizesAndCommitmentCollector {

    /**
     * Used when no cache sizes and commitment collector is available.
     */
    CacheSizesAndCommitmentCollector EMPTY = (clusterState, listener) -> listener.onResponse(CacheSizesAndCommitmentStats.EMPTY);

    /**
     * Collects the shard cache requirements, node cache commitment stats and node cache sizes.
     *
     * @param clusterState The cluster state snapshot for this collection.
     * @param listener The listener which will receive the results.
     */
    void collectCacheSizesAndCommitmentStats(ClusterState clusterState, ActionListener<CacheSizesAndCommitmentStats> listener);
}
