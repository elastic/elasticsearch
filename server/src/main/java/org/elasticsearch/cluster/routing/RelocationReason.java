/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Describes why a shard relocation was started.
 *
 * @param reason           a short description of the kind of move, e.g. {@code move(cannot-remain)}. Used for logging and as a metric
 *                         attribute, so it must be low-cardinality.
 * @param canRemainDecider the label of the {@link org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider} whose non-YES
 *                         {@code canRemain} decision triggered the move, or {@code null} if the move was not triggered by a decider (e.g.
 *                         a rebalance) or the label is not available (see
 *                         {@link org.elasticsearch.cluster.routing.allocation.AllocationService#PRESERVE_DECISION_LABELS_SETTING}).
 */
public record RelocationReason(String reason, @Nullable String canRemainDecider) {

    public RelocationReason {
        Objects.requireNonNull(reason);
    }

    /**
     * A reason that was not triggered by a {@code canRemain} decision.
     */
    public static RelocationReason of(String reason) {
        return new RelocationReason(reason, null);
    }
}
