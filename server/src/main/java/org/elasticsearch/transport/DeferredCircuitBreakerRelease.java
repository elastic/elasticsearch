/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.core.Releasable;

/**
 * Interface for transport responses that have circuit breaker bytes that should be released
 * only after the response has been fully written to the network, not when the response is
 * first queued for sending.
 * <p>
 * This is important for preventing memory accumulation when responses are generated faster
 * than they can be transmitted.
 */
public interface DeferredCircuitBreakerRelease {

    /**
     * Takes ownership of the circuit breaker release, returning a releasable that will
     * release the circuit breaker bytes when closed. After this method is called, the
     * response no longer owns the circuit breaker bytes and calling this method again
     * will return null.
     *
     * @return a releasable that releases circuit breaker bytes, or null if already taken
     *         or no bytes were tracked
     */
    Releasable takeCircuitBreakerRelease();
}
