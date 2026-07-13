/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Set;

public interface AutoscalingDeciderContext {
    /**
     * The cluster state to use when calculation a capacity.
     */
    ClusterState state();

    /**
     * Return current capacity of nodes governed by the policy. Can be null if the capacity of some nodes is unavailable. If a decider
     * relies on this value and gets a null current capacity, it should return a result with a null requiredCapacity (undecided).
     */
    AutoscalingCapacity currentCapacity();

    /**
     * Return the nodes governed by the policy.
     */
    Set<DiscoveryNode> nodes();

    /**
     * Return the set of roles required for nodes governed by the policy.
     */
    Set<DiscoveryNodeRole> roles();

    /**
     * The cluster info to use when calculating a capacity. This represents the storage use on nodes including per shard usage.
     */
    ClusterInfo info();

    /**
     * The snapshot shard size info to use when calculating decider capacity. This represents shard sizes of unallocated restores of
     * shards
     */
    SnapshotShardSizeInfo snapshotShardSizeInfo();

    /**
     * For long running ops, call this from time to time to check if operation has been cancelled.
      */
    void ensureNotCancelled();
}
