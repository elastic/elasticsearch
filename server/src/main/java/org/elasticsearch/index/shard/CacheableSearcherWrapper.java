/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.engine.Engine;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A searcher wrapper that avoids wrapping the same searcher multiple times.
 * This wrapper must be created and used by a single thread.
 */
final class CacheableSearcherWrapper implements Function<Engine.Searcher, Engine.Searcher> {
    private final Map<IndexReader.CacheKey, SearcherHolder> caches = new HashMap<>();
    private final Function<Engine.Searcher, Engine.Searcher> delegate;
    private final Thread creationThread;

    CacheableSearcherWrapper(Function<Engine.Searcher, Engine.Searcher> delegate) {
        this.creationThread = Thread.currentThread();
        this.delegate = delegate;
    }

    private boolean assertAccessingThread() {
        assert creationThread == Thread.currentThread()
            : "created by [" + creationThread + "] != current thread [" + Thread.currentThread() + "]";
        return true;
    }

    @Override
    public Engine.Searcher apply(Engine.Searcher in) {
        assert assertAccessingThread();
        final IndexReader.CacheHelper cacheHelper = in.getIndexReader().getReaderCacheHelper();
        final IndexReader.CacheKey cacheKey = cacheHelper != null ? cacheHelper.getKey() : null;
        if (cacheKey == null) {
            return delegate.apply(in);
        }
        final SearcherHolder searcherHolder = caches.compute(cacheKey, (key, curr) -> {
            if (curr != null) {
                curr.incRef();
            } else {
                final Engine.Searcher wrapped = delegate.apply(in);
                curr = new SearcherHolder(wrapped, () -> caches.remove(key));
            }
            return curr;
        });
        return searcherHolder.searcher;
    }

    private static class SearcherHolder extends AbstractRefCounted {
        private final Engine.Searcher searcher;
        private final Releasable onClose;

        SearcherHolder(Engine.Searcher searcher, Releasable onClose) {
            this.onClose = Releasables.wrap(onClose, searcher);
            this.searcher = new Engine.Searcher(
                searcher.source(),
                searcher.getIndexReader(),
                searcher.getSimilarity(),
                searcher.getQueryCache(),
                searcher.getQueryCachingPolicy(),
                this::decRef
            );
        }

        @Override
        protected void closeInternal() {
            onClose.close();
        }
    }

    // for testing
    int cacheSize() {
        return caches.size();
    }
}
