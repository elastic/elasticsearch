/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

/**
 * A listener interface for receiving notifications when entries are removed from a {@link Cache}.
 * Removal can occur for various reasons including eviction due to size limits, expiration,
 * or explicit invalidation.
 *
 * <p>The listener is called synchronously during the removal operation, so implementations
 * should be quick and avoid blocking operations.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * RemovalListener<String, Resource> listener = notification -> {
 *     Resource resource = notification.getValue();
 *     if (resource != null) {
 *         resource.cleanup();
 *     }
 *     System.out.println("Removed " + notification.getKey() +
 *                        " due to " + notification.getRemovalReason());
 * };
 *
 * Cache<String, Resource> cache = CacheBuilder.<String, Resource>builder()
 *     .setMaximumWeight(100)
 *     .removalListener(listener)
 *     .build();
 * }</pre>
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
@FunctionalInterface
public interface RemovalListener<K, V> {
    /**
     * Called when an entry is removed from the cache.
     *
     * @param notification contains information about the removed entry including the key,
     *                     value, and reason for removal
     */
    void onRemoval(RemovalNotification<K, V> notification);
}
