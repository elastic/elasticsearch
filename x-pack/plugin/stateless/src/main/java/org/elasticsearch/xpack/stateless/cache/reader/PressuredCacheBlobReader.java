/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executor;

/**
 * Wrapper around a {@link CacheBlobReader} that acquires {@link FillCacheMemoryPressure} budget for the requested length before
 * delegating a read, and releases it when the returned stream is closed (i.e. once a fill thread has drained it to the cache file)
 * or the read fails. The read may be delayed while waiting for budget; only install this on paths that tolerate waiting (warming,
 * prefetching), never on cache-miss reads serving searches.
 *
 * A read that had to wait resumes on the pool the invoking thread belongs to. The invoking pool is part of the read's contract:
 * fill handlers assert specific pools (see {@code SequentialRangeMissingHandler}), and delegates with a direct fetch executor
 * complete their listener — and thus run the cache-file write — on the invoking thread.
 */
public class PressuredCacheBlobReader implements CacheBlobReader {

    private final CacheBlobReader delegate;
    private final FillCacheMemoryPressure pressure;
    private final ThreadPool threadPool;

    public PressuredCacheBlobReader(CacheBlobReader delegate, FillCacheMemoryPressure pressure, ThreadPool threadPool) {
        this.delegate = delegate;
        this.pressure = pressure;
        this.threadPool = threadPool;
    }

    @Override
    public ByteRange getRange(long position, int length, long remainingFileLength) {
        return delegate.getRange(position, length, remainingFileLength);
    }

    @Override
    public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
        pressure.acquire(length, deferredReadExecutor(), listener.delegateFailureAndWrap((delegatedListener, budget) -> {
            ActionListener<InputStream> readListener = new ActionListener<>() {
                @Override
                public void onResponse(InputStream inputStream) {
                    delegatedListener.onResponse(new ReleasingInputStream(inputStream, budget));
                }

                @Override
                public void onFailure(Exception e) {
                    budget.close();
                    delegatedListener.onFailure(e);
                }
            };
            // routes a synchronously thrown exception to readListener so the budget is still released
            ActionListener.run(readListener, l -> delegate.getRangeInputStream(position, length, l));
        }));
    }

    @Override
    public String executorName() {
        return delegate.executorName();
    }

    /**
     * The executor a deferred grant resumes the read on: the pool of the thread that requested the read. All production reads are
     * issued from registered pool threads (the fill handlers assert as much), so an unresolvable pool fails loudly rather than
     * running the read on the wrong pool.
     */
    private Executor deferredReadExecutor() {
        final String poolName = EsExecutors.executorName(Thread.currentThread());
        if (poolName == null) {
            // not an ES pool thread, which only happens in tests: resume on the thread that released the budget
            return EsExecutors.DIRECT_EXECUTOR_SERVICE;
        }
        return threadPool.executor(poolName);
    }

    private static class ReleasingInputStream extends FilterInputStream {

        private final Releasable budget;
        private boolean closed;

        private ReleasingInputStream(InputStream delegateInputStream, Releasable budget) {
            super(delegateInputStream);
            this.budget = budget;
        }

        @Override
        public void close() throws IOException {
            if (closed == false) {
                closed = true;
                try {
                    super.close();
                } finally {
                    budget.close();
                }
            }
        }
    }
}
