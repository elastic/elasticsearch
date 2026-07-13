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
/// data node currently performing each recovery.
public record DirectCancellationCandidates(List<Candidates> candidates) {

    public static final DirectCancellationCandidates EMPTY = new DirectCancellationCandidates(List.of());

    public DirectCancellationCandidates {
        candidates = List.copyOf(candidates);
    }

    public record Candidates(DiscoveryNode node, List<ShardRecoveryCancellation> cancellations) {
        public Candidates {
            cancellations = List.copyOf(cancellations);
        }
    }
}
