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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executor;

/**
 * {@link CacheBlobReader} wrapper: acquires {@link FillCacheMemoryPressure} budget for the requested length before delegating, and
 * releases it when the returned stream closes (fill wrote it to disk) or the read fails. May delay the read; install only on paths
 * that tolerate waiting (warming, prefetching), never on cache-miss reads.
 *
 * A deferred read resumes on the invoking thread's pool. That pool is part of the read contract: fill handlers assert specific pools
 * (see {@code SequentialRangeMissingHandler}), and direct-executor delegates run the cache-file write on the resuming thread.
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
                    // if delegatedListener.onResponse throws, the wrapper was constructed but the caller never took ownership;
                    // close it here so the budget is released rather than stranded inside an orphaned wrapper stream.
                    final ReleasingInputStream wrapper = new ReleasingInputStream(inputStream, budget);
                    boolean handedOff = false;
                    try {
                        delegatedListener.onResponse(wrapper);
                        handedOff = true;
                    } finally {
                        if (handedOff == false) {
                            IOUtils.closeWhileHandlingException(wrapper);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    budget.close();
                    delegatedListener.onFailure(e);
                }
            };
            // routes sync-thrown exceptions to readListener so the budget still releases
            ActionListener.run(readListener, l -> delegate.getRangeInputStream(position, length, l));
        }));
    }

    @Override
    public String executorName() {
        return delegate.executorName();
    }

    /**
     * Pool a deferred grant resumes on: the invoking thread's pool. Production reads always issue from registered pool threads (fill
     * handlers assert this), so an unresolvable pool fails loudly rather than resuming on the wrong pool.
     */
    private Executor deferredReadExecutor() {
        final String poolName = EsExecutors.executorName(Thread.currentThread());
        if (poolName == null) {
            // not an ES pool thread (tests only): resume on the releaser
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
