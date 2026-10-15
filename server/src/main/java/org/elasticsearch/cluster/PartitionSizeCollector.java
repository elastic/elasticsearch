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

import java.util.Map;

/**
 * Collects the size in bytes of cluster nodes' hosted-shards partitions for use in {@link ClusterInfo}.
 */
public interface PartitionSizeCollector {

    /**
     * Used when no partition size collector is available.
     */
    PartitionSizeCollector EMPTY = (clusterState, listener) -> listener.onResponse(Map.of());

    /**
     * Collects the "hosted shards" partition size in bytes, keyed by node ID.
     *
     * @param clusterState The current cluster state.
     * @param listener The listener which will receive a map of node ID to hosted-shards partition size in bytes.
     */
    void collectHostedShardsPartitionSizes(ClusterState clusterState, ActionListener<Map<String, Long>> listener);
}
