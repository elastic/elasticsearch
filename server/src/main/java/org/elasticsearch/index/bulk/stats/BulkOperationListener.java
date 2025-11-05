/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.bulk.stats;

/**
 * Listener interface for bulk operation events.
 * Implementations receive callbacks after bulk operations complete, allowing them to
 * track statistics, perform logging, or trigger other side effects.
 */
public interface BulkOperationListener {
    /**
     * Called after a bulk operation completes.
     * The default implementation does nothing, allowing implementers to selectively
     * override this method.
     *
     * @param bulkShardSizeInBytes the total size of the bulk operation in bytes
     * @param tookInNanos the time taken to execute the bulk operation in nanoseconds
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkOperationListener listener = new BulkOperationListener() {
     *     @Override
     *     public void afterBulk(long bulkShardSizeInBytes, long tookInNanos) {
     *         logger.info("Bulk operation completed: {} bytes in {} ns",
     *             bulkShardSizeInBytes, tookInNanos);
     *     }
     * };
     * }</pre>
     */
    default void afterBulk(long bulkShardSizeInBytes, long tookInNanos) {}
}
