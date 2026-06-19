/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.action.ActionListener;

/**
 * An optional single-slot hook that gates mutable shard operations. Implementations may delay the
 * listener until the shard is ready to accept writes (e.g. hollow shards in stateless mode).
 * At most one gate may be registered per shard. In the absence of a gate {@link IndexShard#ensureMutable}
 * completes immediately.
 */
@FunctionalInterface
public interface MutableOperationGate {
    /**
     * Called before a mutable operation is executed on the shard. The implementation must eventually
     * complete {@code listener} (on success or failure).
     *
     * @param shard          the shard where the mutable operation will be performed
     * @param permitAcquired whether the operation has already acquired a primary operation permit
     * @param listener       completed when the shard is ready to proceed
     */
    void beforeMutableOperation(IndexShard shard, boolean permitAcquired, ActionListener<Void> listener);
}
