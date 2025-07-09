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
