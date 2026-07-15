/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.List;

/// A batch of shard mid-recoveries that [DesiredBalanceReconciler] has identified as no longer being allocated to a
/// desired location during one reconciliation round, and that are eligible for direct cancellation, grouped by the
/// data node currently performing each recovery and annotated with the cluster state and desired balance generation
/// from which they were computed.
public record PendingDirectCancellations(
    long clusterStateTerm,
    long clusterStateVersion,
    long desiredBalanceGeneration,
    List<Candidates> candidates
) {

    public static final PendingDirectCancellations EMPTY = new PendingDirectCancellations(-1L, -1L, -1L, List.of());

    public boolean isEmpty() {
        return candidates.isEmpty();
    }

    public boolean isOutOfDate(long expectedTerm, long expectedVersion, long expectedBalanceGeneration) {
        return expectedTerm != clusterStateTerm()
            || expectedVersion != clusterStateVersion()
            || expectedBalanceGeneration != desiredBalanceGeneration();
    }

    public PendingDirectCancellations {
        candidates = List.copyOf(candidates);
    }

    public record Candidates(DiscoveryNode node, List<ShardRecoveryCancellation> cancellations) {
        public Candidates {
            cancellations = List.copyOf(cancellations);
        }
    }
}
