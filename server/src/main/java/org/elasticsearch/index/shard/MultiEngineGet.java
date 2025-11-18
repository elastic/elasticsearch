/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.engine.Engine;

import java.util.Objects;
import java.util.function.Function;

/**
 * A session that can perform multiple gets without wrapping searchers multiple times.
 * This must be created and used by a single thread.
 */
public abstract class MultiEngineGet {
    private IndexReader.CacheKey lastKey;
    private Engine.Searcher lastWrapped;

    private final Thread creationThread;
    private final Function<Engine.Searcher, Engine.Searcher> wrapper;

    MultiEngineGet(Function<Engine.Searcher, Engine.Searcher> wrapper) {
        this.creationThread = Thread.currentThread();
        this.wrapper = wrapper;
    }

    final boolean assertAccessingThread() {
        assert creationThread == Thread.currentThread()
            : "created by [" + creationThread + "] != current thread [" + Thread.currentThread() + "]";
        return true;
    }

    public abstract Engine.GetResult get(Engine.Get get);

    final Engine.Searcher wrapSearchSearchWithCache(Engine.Searcher searcher) {
        assert assertAccessingThread();
        final IndexReader.CacheHelper cacheHelper = searcher.getIndexReader().getReaderCacheHelper();
        final IndexReader.CacheKey cacheKey = cacheHelper != null ? cacheHelper.getKey() : null;
        // happens with translog reader
        if (cacheKey == null) {
            return wrapper.apply(searcher);
        }
        if (Objects.equals(lastKey, cacheKey) == false) {
            Releasables.close(lastWrapped, () -> lastWrapped = null);
            lastWrapped = wrapper.apply(searcher);
            lastKey = cacheKey;
        }
        return new Engine.Searcher(
            lastWrapped.source(),
            lastWrapped.getIndexReader(),
            lastWrapped.getSimilarity(),
            lastWrapped.getQueryCache(),
            lastWrapped.getQueryCachingPolicy(),
            () -> {}
        );
    }

    final void releaseCachedSearcher() {
        assert assertAccessingThread();
        Releasables.close(lastWrapped);
    }
}
