/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * Tracks circuit-breaker charges accumulated during query parsing and releases them once all holders
 * are done with the parsed query.
 * <p>
 * The reservation starts with an implicit reference count of one, representing the
 * {@link org.elasticsearch.search.builder.SearchSourceBuilder} that owns it. Additional holders call
 * {@link #acquire()} to obtain their own {@link Releasable}; releasing that handle decrements the
 * count. When the count reaches zero, {@link #closeInternal()} releases all accumulated breaker
 * charges.
 */
public final class QueryParsingReservation extends AbstractRefCounted {

    private final List<Releasable> releasables;

    public QueryParsingReservation(List<Releasable> releasables) {
        this.releasables = releasables;
    }

    /**
     * Appends {@code charges} to this reservation's releasable list if the reservation is still live,
     * ensuring they are closed when this reservation is fully released. If the reservation has already
     * been fully released, the charges are closed immediately so they are not leaked.
     */
    public void addCharges(List<Releasable> charges) {
        if (tryIncRef()) {
            try {
                // Synchronized because multiple rewrite threads may call addCharges() concurrently.
                synchronized (releasables) {
                    releasables.addAll(charges);
                }
            } finally {
                decRef();
            }
        } else {
            Releasables.close(charges);
        }
    }

    /**
     * Returns a new handle on this reservation, or {@code null} if the reservation has already been
     * fully released (charges already returned to the breaker). The caller must close the returned
     * {@link Releasable} when the parsed query is no longer needed (typically when the request
     * completes). Returning {@code null} is safe for retried or reused requests: their charges were
     * already released by an earlier close, so there is nothing left to track.
     */
    public Releasable acquire() {
        if (tryIncRef() == false) {
            return null;
        }
        return Releasables.releaseOnce(this::decRef);
    }

    @Override
    protected void closeInternal() {
        Releasables.close(releasables);
    }
}
