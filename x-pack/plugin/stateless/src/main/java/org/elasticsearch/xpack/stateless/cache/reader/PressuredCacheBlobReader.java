/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.core.Releasable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper around a {@link CacheBlobReader} that acquires {@link FillCacheMemoryPressure} budget for the requested length before
 * delegating a read, and releases it when the returned stream is closed (i.e. once a fill thread has drained it to the cache file)
 * or the read fails. The read may be delayed while waiting for budget; only install this on paths that tolerate waiting (warming,
 * prefetching), never on cache-miss reads serving searches.
 */
public class PressuredCacheBlobReader implements CacheBlobReader {

    private final CacheBlobReader delegate;
    private final FillCacheMemoryPressure pressure;

    public PressuredCacheBlobReader(CacheBlobReader delegate, FillCacheMemoryPressure pressure) {
        this.delegate = delegate;
        this.pressure = pressure;
    }

    @Override
    public ByteRange getRange(long position, int length, long remainingFileLength) {
        return delegate.getRange(position, length, remainingFileLength);
    }

    @Override
    public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
        pressure.acquire(length, listener.delegateFailureAndWrap((delegatedListener, budget) -> {
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
