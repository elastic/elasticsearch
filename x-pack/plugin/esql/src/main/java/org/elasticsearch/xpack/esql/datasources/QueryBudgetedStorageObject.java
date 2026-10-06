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
import org.elasticsearch.core.Releasable;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Decorates a {@link StorageObject} with per-query concurrency budget enforcement. Each I/O
 * operation acquires a permit from the query's {@link QueryConcurrencyBudget} before delegating
 * and releases it when the operation completes. For stream-returning methods, the permit is
 * held until the stream is closed.
 * <p>
 * This wrapper is applied on top of the global {@link ConcurrencyLimitedStorageObject} to provide
 * two-level concurrency control: per-query fairness (this layer) + global hard cap (inner layer).
 */
class QueryBudgetedStorageObject implements StorageObject, ResumeBypassingStorageObject {

    private final StorageObject delegate;
    private final QueryConcurrencyBudget budget;

    QueryBudgetedStorageObject(StorageObject delegate, QueryConcurrencyBudget budget) {
        this.delegate = delegate;
        this.budget = budget;
    }

    @Override
    public InputStream newStream() throws IOException {
        PermitToken token = acquirePermit();
        try {
            InputStream stream = delegate.newStream();
            return new PermitReleasingInputStream(stream, budget, token);
        } catch (Exception e) {
            releasePermit(token);
            throw e;
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        PermitToken token = acquirePermit();
        try {
            InputStream stream = delegate.newStream(position, length);
            return new PermitReleasingInputStream(stream, budget, token);
        } catch (Exception e) {
            releasePermit(token);
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
            // close-time drain we are trying to avoid on providers like S3.
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
    public void bindRowGroup(RowGroupIo io) {
        budget.bind(io);
    }

    @Override
    public long admissionWaitTimeoutMs() {
        return budget.acquireTimeoutMs();
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        PermitToken token = acquirePermit();
        try {
            return delegate.readBytes(position, target);
        } finally {
            releasePermit(token);
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
        final PermitToken token;
        try {
            token = acquirePermit();
        } catch (Exception e) {
            listener.onFailure(e);
            return () -> {};
        }
        try {
            // We intentionally use a raw ActionListener instead of ActionListener.wrap so a
            // throw from listener.onResponse(result) does NOT get auto-routed to our onFailure
            // lambda — that would double-release the budget and double-fire the downstream
            // listener (onResponse + onFailure for the same I/O). The token captures the lease
            // from the calling thread; SDK callbacks must not read StorageIoAffinity.current().
            return delegate.startReadBytesAsync(position, length, factory, executor, new ActionListener<>() {
                @Override
                public void onResponse(DirectReadBuffer result) {
                    releasePermit(token);
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
                    releasePermit(token);
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            releasePermit(token);
            listener.onFailure(e);
            return () -> {};
        }
    }

    @Override
    public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        final PermitToken token;
        try {
            token = acquirePermit();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try {
            // Raw ActionListener (see overload above) so a throw from listener.onResponse does
            // not get auto-routed and double-release the budget / double-fire the listener.
            delegate.readBytesAsync(position, target, executor, new ActionListener<>() {
                @Override
                public void onResponse(Integer result) {
                    releasePermit(token);
                    try {
                        listener.onResponse(result);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    releasePermit(token);
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            releasePermit(token);
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
     * Reads {@link StorageIoAffinity#current()} on the calling thread. SDK completion callbacks
     * must use the returned token instead of the ThreadLocal.
     */
    private PermitToken acquirePermit() {
        StorageIoAffinity.Scope scope = StorageIoAffinity.current();
        RowGroupIo lease = scope == null ? null : scope.lease();
        boolean countGets = scope != null && scope.countGets;
        try {
            budget.acquire(lease, countGets);
        } catch (TimeoutException e) {
            throw new EsRejectedExecutionException("Failed to acquire query concurrency budget permit: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EsRejectedExecutionException("Interrupted while waiting for query concurrency budget permit: " + e);
        }
        return new PermitToken(lease, countGets);
    }

    private void releasePermit(PermitToken token) {
        budget.release(token.lease, token.countGets);
    }

    private record PermitToken(RowGroupIo lease, boolean countGets) {}

    private static class PermitReleasingInputStream extends FilterInputStream {
        private final QueryConcurrencyBudget budget;
        private final PermitToken token;
        private final AtomicBoolean released = new AtomicBoolean();

        PermitReleasingInputStream(InputStream in, QueryConcurrencyBudget budget, PermitToken token) {
            super(in);
            this.budget = budget;
            this.token = token;
        }

        InputStream inner() {
            return in;
        }

        /**
         * Releases the permit without closing the wrapped stream. Used by
         * {@link QueryBudgetedStorageObject#abortStream(InputStream)} after the inner stream
         * has been aborted directly via the delegate, so we don't double-close.
         */
        void markReleased() {
            if (released.getAndSet(true) == false) {
                budget.release(token.lease, token.countGets);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                if (released.getAndSet(true) == false) {
                    budget.release(token.lease, token.countGets);
                }
            }
        }
    }
}
