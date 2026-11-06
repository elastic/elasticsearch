/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;

/**
 * Node-wide, thread-safe, created and shut down by the plugin. The store knows nothing of queries beyond their scopes.
 */
public interface SpillStore {

    /**
     * A new scope for one query on this node. Synchronous; throws for a duplicate id and after shutdown.
     *
     * @param queryId for diagnostics and naming; unique on the node while the scope lives
     * @param breaker where the backend charges its own buffers, so that they are visible to the breaker; the bytes it
     *                stores are its own numbers, read through {@link #bytesInUse()}, never the breaker's
     */
    SpillScope newScope(String queryId, CircuitBreaker breaker);

    /**
     * Bytes held across every scope, a snapshot. Admission against the cap is the backend's, at append time.
     */
    long bytesInUse();

    /**
     * The cap the backend admits against, a snapshot; {@link Long#MAX_VALUE} when there is none.
     */
    long bytesLimit();

    /**
     * Releases everything, cancelling what remains; completes when no worker of the store touches anything any more.
     * Idempotent.
     */
    void shutdown(ActionListener<Void> listener);
}
