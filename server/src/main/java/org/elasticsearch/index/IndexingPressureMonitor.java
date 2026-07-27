/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

/**
 * Monitors indexing pressure events within the system and tracks operation sizes.
 * This interface provides mechanisms to check maximum allowed operation sizes
 * and register listeners for indexing pressure events.
 */
public interface IndexingPressureMonitor {
    /**
     * Returns the maximum allowed size in bytes for any single indexing operation.
     * Operations exceeding this limit may be rejected.
     *
     * @return the maximum allowed operation size in bytes
     */
    long getMaxAllowedOperationSizeInBytes();

    /**
     * Registers a listener to be notified of indexing pressure events.
     * The listener will receive callbacks when operations are tracked or rejected.
     *
     * @param listener the listener to register for indexing pressure events
     */
    void addListener(IndexingPressureListener listener);

    /**
     * Registers a contributor that can reject new indexing operations when an external
     * resource is under pressure. Implementations must be lightweight and
     * thread-safe.
     *
     * @param contributor the contributor to register
     */
    void addContributor(IndexingPressureContributor contributor);

    /**
     * A source of additional write-path back-pressure. If the contributor's internal limit
     * is exceeded, it should throw {@link org.elasticsearch.common.util.concurrent.EsRejectedExecutionException}
     * to reject the operation. The exception message should include enough context (current value,
     * limit) for operators to understand and act on the rejection.
     *
     * <p>Implementations must be thread-safe and must not block or perform significant
     * computation, as {@link #checkAndMaybeReject()} is on the hot write path.
     */
    interface IndexingPressureContributor {
        /**
         * Called before each indexing operation is admitted. Implementations should throw
         * {@link org.elasticsearch.common.util.concurrent.EsRejectedExecutionException} if
         * this contributor's limit is currently exceeded, causing the operation to be rejected
         * with an HTTP 429 response. Does nothing (returns normally) when not over the limit.
         */
        void checkAndMaybeReject();
    }

    /**
     * Listener interface for receiving notifications about indexing pressure events.
     * Implementations can respond to tracking of primary operations and rejections
     * of large indexing operations.
     */
    interface IndexingPressureListener {
        /**
         * Called when a primary indexing operation is tracked.
         * The implementation should be really lightweight as this is called in a hot path.
         *
         * @param largestOperationSizeInBytes the size in bytes of the largest operation tracked
         */
        void onPrimaryOperationTracked(long largestOperationSizeInBytes);

        /**
         * Called when a large indexing operation is rejected due to exceeding size limits.
         * The implementation should be really lightweight as this is called in a hot path.
         *
         * @param largestOperationSizeInBytes the size in bytes of the rejected operation
         */
        void onLargeIndexingOperationRejection(long largestOperationSizeInBytes);
    }
}
