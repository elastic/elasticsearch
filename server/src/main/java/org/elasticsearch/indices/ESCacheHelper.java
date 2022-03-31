/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.index.IndexReader;

/**
 * Cache helper that allows swapping in implementations that are different to Lucene's
 * IndexReader.CacheHelper which ties its lifecycle to that of the underlying reader.
 *
 * For FrozenEngine, which opens / closes readers on-demand, we don't want caches
 * to be invalidated as soon as a given search terminates, but want to tie the
 * entry in the cache to the lifecycle of the shard.
 */
public interface ESCacheHelper {

    /**
     * Get a key that the resource can be cached on. The given entry can be
     * compared using identity, i.e., {@link Object#equals} is implemented as
     * {@code ==} and {@link Object#hashCode} is implemented as
     * {@link System#identityHashCode}.
     */
    Object getKey();

    /**
     * Adds a listener which will be called when the resource guarded
     * by {@link #getKey()} is closed.
     */
    void addClosedListener(ClosedListener listener);

    @FunctionalInterface
    interface ClosedListener {
        void onClose(Object key);
    }

    /**
     * Implementation of {@link ESCacheHelper} that wraps an {@link IndexReader.CacheHelper}.
     */
    class Wrapper implements ESCacheHelper {

        private final IndexReader.CacheHelper cacheHelper;

        public Wrapper(IndexReader.CacheHelper cacheHelper) {
            this.cacheHelper = cacheHelper;
        }

        @Override
        public Object getKey() {
            return cacheHelper.getKey();
        }

        @Override
        public void addClosedListener(ClosedListener listener) {
            cacheHelper.addClosedListener(listener::onClose);
        }

    }
}
