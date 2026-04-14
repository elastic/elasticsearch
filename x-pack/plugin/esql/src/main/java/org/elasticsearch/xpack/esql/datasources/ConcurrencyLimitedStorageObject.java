/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

/**
 * Decorates a {@link StorageObject} with concurrency limiting. Each I/O operation
 * acquires a permit before executing and releases it when the operation completes.
 * For stream-returning methods, the permit is released when the stream is closed.
 */
class ConcurrencyLimitedStorageObject implements StorageObject {

    private final StorageObject delegate;
    private final ConcurrencyLimiter limiter;

    ConcurrencyLimitedStorageObject(StorageObject delegate, ConcurrencyLimiter limiter) {
        this.delegate = delegate;
        this.limiter = limiter;
    }

    @Override
    public InputStream newStream() throws IOException {
        acquirePermit();
        try {
            InputStream stream = delegate.newStream();
            return new PermitReleasingInputStream(stream, limiter);
        } catch (Exception e) {
            limiter.release();
            throw e;
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        acquirePermit();
        try {
            InputStream stream = delegate.newStream(position, length);
            return new PermitReleasingInputStream(stream, limiter);
        } catch (Exception e) {
            limiter.release();
            throw e;
        }
    }

    @Override
    public long length() throws IOException {
        acquirePermit();
        try {
            return delegate.length();
        } finally {
            limiter.release();
        }
    }

    @Override
    public Instant lastModified() throws IOException {
        acquirePermit();
        try {
            return delegate.lastModified();
        } finally {
            limiter.release();
        }
    }

    @Override
    public boolean exists() throws IOException {
        acquirePermit();
        try {
            return delegate.exists();
        } finally {
            limiter.release();
        }
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        acquirePermit();
        try {
            return delegate.readBytes(position, target);
        } finally {
            limiter.release();
        }
    }

    @Override
    public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        try {
            acquirePermit();
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }
        try {
            delegate.readBytesAsync(position, length, executor, ActionListener.wrap(result -> {
                limiter.release();
                listener.onResponse(result);
            }, e -> {
                limiter.release();
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            limiter.release();
            listener.onFailure(e);
        }
    }

    @Override
    public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        try {
            acquirePermit();
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }
        try {
            delegate.readBytesAsync(position, target, executor, ActionListener.wrap(result -> {
                limiter.release();
                listener.onResponse(result);
            }, e -> {
                limiter.release();
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            limiter.release();
            listener.onFailure(e);
        }
    }

    @Override
    public boolean supportsNativeAsync() {
        return delegate.supportsNativeAsync();
    }

    private void acquirePermit() throws IOException {
        try {
            limiter.acquire();
        } catch (TimeoutException e) {
            throw new IOException("Failed to acquire concurrency permit for cloud API call", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for concurrency permit", e);
        }
    }

    /**
     * InputStream wrapper that releases the concurrency permit when closed.
     */
    private static class PermitReleasingInputStream extends FilterInputStream {
        private final ConcurrencyLimiter limiter;
        private boolean released;

        PermitReleasingInputStream(InputStream in, ConcurrencyLimiter limiter) {
            super(in);
            this.limiter = limiter;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                if (released == false) {
                    released = true;
                    limiter.release();
                }
            }
        }
    }
}
