/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

/**
 * Implemented by {@link ActionRequest}s that carry resources (such as circuit-breaker charges from
 * query parsing) that must be released when the request's transport lifecycle ends.
 * <p>
 * {@link org.elasticsearch.action.support.TransportAction} acquires a handle via
 * {@link #acquireReservation()} on every execution path and releases it when the request's listener
 * completes, ensuring charges are held for the full search lifetime without requiring per-site
 * cleanup code.
 */
public interface ReleasableRequest {
    /**
     * Acquires a handle on this request's resource reservation.
     *
     * @return a {@link Releasable} that the caller must close when done, or {@code null} if the
     *         request carries no reservation
     * @throws IllegalStateException if the reservation has already been fully released
     */
    @Nullable
    Releasable acquireReservation();
}
