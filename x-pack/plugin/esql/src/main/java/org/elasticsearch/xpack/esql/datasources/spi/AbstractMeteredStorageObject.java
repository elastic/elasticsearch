/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Base for the leaf {@link StorageObject} providers (S3, GCS, Azure, HTTP, local): owns the
 * {@link #counters} field and the {@link #metrics()} accessor that each used to re-declare
 * identically, plus the shared native-async read completion handling ({@link #onReadComplete} /
 * {@link #deliverRead}). Subclasses keep their provider-specific orchestration (failure mapping,
 * status checks, buffer allocation) inline. Decorators do not extend this; they forward
 * {@link #metrics()} to their delegate.
 */
public abstract class AbstractMeteredStorageObject implements StorageObject {

    protected final StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();

    @Override
    public final StorageObjectMetrics metrics() {
        return counters.snapshot();
    }

    /**
     * Attaches {@code handler} to a native-async read {@code future} and returns the future. The
     * underlying client (the AWS SDK, {@code HttpClient}, the Azure reactive client) hands us a
     * {@link CompletableFuture}, which captures any {@link Error} a completion callback throws into
     * the future it returns rather than letting it propagate. The handler is wrapped so a fatal
     * error is rethrown to the uncaught-exception handler via
     * {@link ExceptionsHelper#maybeDieOnAnotherThread} instead of being swallowed.
     */
    protected static <T> CompletableFuture<T> onReadComplete(CompletableFuture<T> future, BiConsumer<T, Throwable> handler) {
        return future.whenComplete((result, throwable) -> {
            try {
                handler.accept(result, throwable);
            } catch (Throwable t) {
                ExceptionsHelper.maybeDieOnAnotherThread(t);
                throw t;
            }
        });
    }

    /**
     * Hands a successfully-read {@code buffer} to {@code listener}, recording the byte count and
     * elapsed time. If delivery throws, closes the buffer so its breaker charge does not outlive the
     * failed hand-off, then propagates.
     */
    protected final void deliverRead(ActionListener<DirectReadBuffer> listener, DirectReadBuffer buffer, long startNanos) {
        counters.addRequest(System.nanoTime() - startNanos, buffer.buffer().remaining());
        try {
            listener.onResponse(buffer);
        } catch (Exception e) {
            try {
                buffer.close();
            } catch (Exception closeEx) {
                e.addSuppressed(closeEx);
            }
            throw e;
        }
    }
}
