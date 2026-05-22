/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.search.SearchContextMissingException;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Tracks the latest instant until which the client expects scroll/PIT search contexts to remain valid,
 * based on keep-alive durations requested with successful searches
 */
public final class SearchContextKeepaliveDeadline {

    /**
     * Sentinel meaning no extension has been recorded yet. Chosen so {@link Math#max(long, long)} with any real
     * {@code now + keepAliveMillis} replaces it on the first successful extension.
     */
    static final long INITIAL_DEADLINE = -1L;

    private final AtomicLong deadlineEpochMillis = new AtomicLong(INITIAL_DEADLINE);
    private final LongSupplier clockMillis;

    public SearchContextKeepaliveDeadline(LongSupplier clockMillis) {
        this.clockMillis = clockMillis;
    }

    /**
     * Updates the deadline after a successful scroll or PIT search response using the effective keep-alive duration for that request.
     */
    public void recordSuccessfulExtension(TimeValue effectiveKeepAlive) {
        long extensionMillis = Math.max(0L, effectiveKeepAlive.millis());
        long now = clockMillis.getAsLong();
        long candidate = now + extensionMillis;
        deadlineEpochMillis.accumulateAndGet(candidate, Math::max);
    }

    /** Returns true when wall-clock time is past {@link #recordSuccessfulExtension(TimeValue)} */
    boolean isPastKeepaliveDeadline() {
        long deadline = deadlineEpochMillis.get();
        if (deadline == INITIAL_DEADLINE) {
            return false;
        }
        return clockMillis.getAsLong() > deadline;
    }

    /**
     * Whether to increment {@link BulkByScrollSearchContextMetrics}. This is true when the search context has returned a
     * {@link SearchContextMissingException} <i>and</i> the keep alive has expired. If the keep alive has not expired,
     * then the search context terminated under different conditions from which we had no control, such as relocation.
     */
    boolean shouldRecordKeepaliveExpiry(@Nullable Throwable catastrophicFailure, @Nullable List<PaginatedSearchFailure> searchFailures) {
        if (isPastKeepaliveDeadline() == false) {
            return false;
        }
        if (catastrophicFailure != null && ExceptionsHelper.unwrap(catastrophicFailure, SearchContextMissingException.class) != null) {
            return true;
        }
        if (searchFailures != null) {
            for (PaginatedSearchFailure f : searchFailures) {
                if (ExceptionsHelper.unwrap(f.getReason(), SearchContextMissingException.class) != null) {
                    return true;
                }
            }
        }
        return false;
    }
}
