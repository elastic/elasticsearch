/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
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

    // Metadata ops (length/lastModified/exists) are deliberately exempt from permit acquisition.
    // The permit is pure throughput control with no correctness role; these are cheap, short-lived
    // HEAD/stat calls whose fan-out is already bounded by the caller thread pool and
    // ExternalSourceResolver.MAX_PARALLEL_METADATA_READS. Parking them behind long-lived stream
    // permits (streams hold their permit for their whole minutes-long lifetime) starved SEARCH
    // threads — see #1151. Byte-transfer ops (newStream/readBytes) stay permit-governed.
    @Override
    public long length() throws IOException {
        return delegate.length();
    }

    @Override
    public Instant lastModified() throws IOException {
        return delegate.lastModified();
    }

    @Override
    public boolean exists() throws IOException {
        return delegate.exists();
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public void abortStream(InputStream stream) throws IOException {
        if (stream instanceof PermitReleasingInputStream wrapper) {
            // Route the abort through to the wrapped inner stream so the delegate (and
            // eventually the storage provider) can do a non-draining abort. Calling
            // wrapper.close() instead would cascade super.close() → in.close() and trigger
            // exactly the close-time drain we are trying to avoid on providers like S3.
            try {
                delegate.abortStream(wrapper.inner());
            } finally {
                wrapper.markReleased();
            }
            return;
        }
        // Not a stream we produced — should be unreachable since the SPI contract requires
        // the exact instance returned from newStream(). Fall back to the SPI default.
        stream.close();
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
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        try {
            acquirePermit();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try {
            // We intentionally use a raw ActionListener instead of ActionListener.wrap so a
            // throw from listener.onResponse(result) does NOT get auto-routed to our onFailure
            // lambda — that would double-release the permit and double-fire the downstream
            // listener (onResponse + onFailure for the same I/O).
            delegate.readBytesAsync(position, length, factory, executor, new ActionListener<>() {
                @Override
                public void onResponse(DirectReadBuffer result) {
                    limiter.release();
                    try {
                        listener.onResponse(result);
                    } catch (Exception e) {
                        // listener.onResponse was already invoked; routing via listener.onFailure
                        // here would violate the single-completion contract. Close the buffer to
                        // free the breaker reservation and propagate so the caller observes the
                        // failure instead of a silent swallow.
                        try {
                            result.close();
                        } catch (Exception closeFailure) {
                            e.addSuppressed(closeFailure);
                        }
                        throw ExceptionsHelper.convertToRuntime(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    limiter.release();
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            limiter.release();
            listener.onFailure(e);
        }
    }

    @Override
    public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        try {
            acquirePermit();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try {
            // Raw ActionListener (see overload above) so a throw from listener.onResponse does
            // not get auto-routed and double-release the permit / double-fire the listener.
            delegate.readBytesAsync(position, target, executor, new ActionListener<>() {
                @Override
                public void onResponse(Integer result) {
                    limiter.release();
                    try {
                        listener.onResponse(result);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    limiter.release();
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            limiter.release();
            listener.onFailure(e);
        }
    }

    @Override
    public boolean supportsNativeAsync() {
        return delegate.supportsNativeAsync();
    }

    @Override
    public StorageObjectMetrics metrics() {
        return delegate.metrics();
    }

    private void acquirePermit() {
        try {
            limiter.acquire();
        } catch (TimeoutException e) {
            throw new EsRejectedExecutionException("Failed to acquire concurrency permit for cloud API call: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EsRejectedExecutionException("Interrupted while waiting for concurrency permit: " + e);
        }
    }

    /**
     * InputStream wrapper that releases the concurrency permit when closed.
     */
    private static class PermitReleasingInputStream extends FilterInputStream {
        private final ConcurrencyLimiter limiter;
        private volatile boolean released;

        PermitReleasingInputStream(InputStream in, ConcurrencyLimiter limiter) {
            super(in);
            this.limiter = limiter;
        }

        InputStream inner() {
            return in;
        }

        /**
         * Releases the permit without closing the wrapped stream. Used by
         * {@link ConcurrencyLimitedStorageObject#abortStream(InputStream)} after the inner
         * stream has been aborted directly via the delegate, so we don't double-close.
         */
        void markReleased() {
            if (released == false) {
                released = true;
                limiter.release();
            }
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
