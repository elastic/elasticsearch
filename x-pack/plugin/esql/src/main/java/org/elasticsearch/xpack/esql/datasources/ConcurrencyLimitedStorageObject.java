/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIoAffinity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Decorates a {@link StorageObject} with concurrency limiting. Each I/O operation
 * acquires a permit before executing and releases it when the operation completes.
 * For stream-returning methods, the permit is released when the stream is closed.
 */
class ConcurrencyLimitedStorageObject implements StorageObject, ResumeBypassingStorageObject {

    private final StorageObject delegate;
    private final ConcurrencyLimiter limiter;

    ConcurrencyLimitedStorageObject(StorageObject delegate, ConcurrencyLimiter limiter) {
        this.delegate = delegate;
        this.limiter = limiter;
    }

    @Override
    public InputStream newStream() throws IOException {
        acquireForStream();
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
        acquireForStream();
        try {
            InputStream stream = delegate.newStream(position, length);
            return new PermitReleasingInputStream(stream, limiter);
        } catch (Exception e) {
            limiter.release();
            throw e;
        }
    }

    /**
     * First GET parks on {@link ConcurrencyLimiter#acquireChecked()}. A text resume
     * ({@link StoragePermitBarge}) barges ({@link ConcurrencyLimiter#tryAcquire()}) so the
     * segmentator never joins the fair semaphore queue. A miss is
     * {@link ConcurrencyLimiter.PermitMissException}; {@link RetryableStorageObject} polls
     * on the reader rather than {@code acquireAsync().join()}, which would deadlock
     * {@code esql_external_io}.
     */
    private void acquireForStream() {
        if (StoragePermitBarge.active()) {
            limiter.acquireBargeChecked();
        } else {
            limiter.acquireChecked();
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
    public long lengthForFooterCacheKey() throws IOException {
        return delegate.lengthForFooterCacheKey();
    }

    @Override
    public long knownLength() {
        return delegate.knownLength();
    }

    @Override
    public String contentGeneration() {
        return delegate.contentGeneration();
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
    public StorageIdentity storageIdentity() {
        return delegate.storageIdentity();
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
    public InputStream withoutResume(InputStream stream) {
        return ResumeBypassingStorageObject.withoutResumeThrough(
            delegate,
            stream,
            PermitReleasingInputStream.class,
            PermitReleasingInputStream::inner
        );
    }

    @Override
    public long admissionWaitTimeoutMs() {
        return limiter.acquireTimeoutMs();
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        limiter.acquireChecked();
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
        startReadBytesAsync(position, length, factory, executor, listener);
    }

    @Override
    public Releasable startReadBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        return startReadBytesAsync(position, length, factory, executor, listener, false);
    }

    /**
     * {@code barge}: untimed {@link ConcurrencyLimiter#tryAcquire()} so a retry continuation never
     * parks. A miss is {@link ConcurrencyLimiter.PermitMissException}; the retry layer waits on
     * {@link #admissionWaitTimeoutMs()} without burning a storage attempt. Permit is not held
     * across attempts; the next hop acquires again. First-attempt async uses a permit ticket.
     */
    @Override
    public Releasable startReadBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener,
        boolean barge
    ) {
        if (barge) {
            return startReadBytesAsyncBarge(position, length, factory, executor, listener);
        }
        StorageIoAffinity.Scope scope = StorageIoAffinity.current();
        RowGroupIo lease = scope == null ? null : scope.lease();
        boolean countGets = scope != null && scope.countGets;
        BooleanSupplier cancel = StorageRetryCancellation.current() == null ? () -> false : StorageRetryCancellation.current();
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicBoolean permitReleased = new AtomicBoolean();
        AtomicReference<Releasable> getHandle = new AtomicReference<>();
        SubscribableListener<Void> ticket = limiter.acquireAsync(() -> cancelled.get() || cancel.getAsBoolean(), executor);
        ticket.addListener(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                if (cancelled.get()) {
                    releaseLimiterOnce(permitReleased);
                    listener.onFailure(new TaskCancelledException("Cancelled while waiting for a concurrency permit"));
                    return;
                }
                try {
                    StorageRetryCancellation.runWithCancellation(cancel, () -> {
                        if (scope != null) {
                            try (StorageIoAffinity.Scope ignored = StorageIoAffinity.open(lease, countGets)) {
                                startDelegate(position, length, factory, executor, listener, permitReleased, getHandle, cancelled);
                            }
                        } else {
                            startDelegate(position, length, factory, executor, listener, permitReleased, getHandle, cancelled);
                        }
                    });
                } catch (Exception e) {
                    releaseLimiterOnce(permitReleased);
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
        return () -> {
            cancelled.set(true);
            limiter.wakeAsyncWaiters();
            Releasable handle = getHandle.get();
            if (handle != null) {
                handle.close();
            }
        };
    }

    private Releasable startReadBytesAsyncBarge(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        try {
            limiter.acquireBargeChecked();
        } catch (Exception e) {
            listener.onFailure(e);
            return () -> {};
        }
        try {
            return delegate.startReadBytesAsync(position, length, factory, executor, new ActionListener<>() {
                @Override
                public void onResponse(DirectReadBuffer result) {
                    limiter.release();
                    try {
                        listener.onResponse(result);
                    } catch (Exception e) {
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
            return () -> {};
        }
    }

    private void startDelegate(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener,
        AtomicBoolean permitReleased,
        AtomicReference<Releasable> getHandle,
        AtomicBoolean cancelled
    ) {
        try {
            Releasable handle = delegate.startReadBytesAsync(position, length, factory, executor, new ActionListener<>() {
                @Override
                public void onResponse(DirectReadBuffer result) {
                    releaseLimiterOnce(permitReleased);
                    try {
                        listener.onResponse(result);
                    } catch (Exception e) {
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
                    releaseLimiterOnce(permitReleased);
                    listener.onFailure(e);
                }
            });
            getHandle.set(handle);
            if (cancelled.get()) {
                handle.close();
            }
        } catch (Exception e) {
            releaseLimiterOnce(permitReleased);
            listener.onFailure(e);
        }
    }

    private void releaseLimiterOnce(AtomicBoolean permitReleased) {
        if (permitReleased.compareAndSet(false, true)) {
            limiter.release();
        }
    }

    @Override
    public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        try {
            limiter.acquireChecked();
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
    public boolean readBytesAsyncReleasesExecutor() {
        return delegate.readBytesAsyncReleasesExecutor();
    }

    @Override
    public StorageObjectMetrics metrics() {
        return delegate.metrics();
    }

    /**
     * InputStream wrapper that releases the concurrency permit when closed.
     */
    private static class PermitReleasingInputStream extends FilterInputStream {
        private final ConcurrencyLimiter limiter;
        private final AtomicBoolean released = new AtomicBoolean();

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
            if (released.getAndSet(true) == false) {
                limiter.release();
            }
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                if (released.getAndSet(true) == false) {
                    limiter.release();
                }
            }
        }
    }
}
