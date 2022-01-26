/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.shard.IndexShard;

/**
 * Abstract base class for the an {@link IndexShard} level {@linkplain IndicesRequestCache.CacheEntity}.
 */
abstract class AbstractIndexShardCacheEntity implements IndicesRequestCache.CacheEntity {

    /**
     * Get the {@linkplain ShardRequestCache} used to track cache statistics.
     */
    protected abstract ShardRequestCache stats();

    @Override
    public final void onCached(IndicesRequestCache.Key key, BytesReference value) {
        stats().onCached(key, value);
    }

    @Override
    public final void onHit() {
        stats().onHit();
    }

    @Override
    public final void onMiss() {
        stats().onMiss();
    }

    @Override
    public final void onRemoval(RemovalNotification<IndicesRequestCache.Key, BytesReference> notification) {
        stats().onRemoval(
            notification.getKey(),
            notification.getValue(),
            notification.getRemovalReason() == RemovalNotification.RemovalReason.EVICTED
        );
    }
}
