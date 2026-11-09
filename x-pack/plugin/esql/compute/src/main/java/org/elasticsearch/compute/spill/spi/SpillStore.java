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
     * A new scope for one query on this node. Synchronous; throws after shutdown.
     *
     * @param queryId names the scope and its files, for diagnostics; not a key: a scope that is closing may be
     *                succeeded by one with the same id, since a query's next request on this node does not wait for
     *                the last one's files to be gone, so a backend tells its scopes apart by instance
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
     * Bytes of spilled data the store could not delete yet and still counts against its limit: cleanup debt it retries
     * on its own; the node stat reports it so an operator sees a store that cannot reclaim its space. A store without
     * such a debt reports zero.
     */
    default long cleanupDebtBytes() {
        return 0;
    }

    /**
     * Releases everything, cancelling what remains; completes when no worker of the store touches anything any more.
     * Idempotent.
     */
    void shutdown(ActionListener<Void> listener);
}
