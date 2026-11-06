/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * A {@link LogRecordExporter} whose delegate can be replaced, so TLS material can be reloaded without rebuilding the
 * {@link BatchLogRecordProcessor} in front of it. The processor registers its queue gauges as callbacks that are never
 * unregistered and are keyed by a static component name, so a second processor on the same meter provider would be
 * summed into the same series and leak its queue.
 *
 * <p>Each operation holds a reference to its delegate from before entering the SDK until asynchronous completion.
 * Replacing a delegate releases its ownership reference, so shutdown waits for both batch preparation and network
 * activity to finish. This is necessary because shutting down the OkHttp sender cancels its in-flight calls.
 */
final class SwappableLogRecordExporter implements LogRecordExporter {

    // Guarded by this. Keep holders until shutdown completes, including those already shutting down after retirement.
    private final Set<ExporterHolder> exporters = new HashSet<>();
    @Nullable
    private ExporterHolder current;
    private final CompletableResultCode shutdownResult = new CompletableResultCode();

    SwappableLogRecordExporter(LogRecordExporter delegate) {
        current = new ExporterHolder(delegate);
        exporters.add(current);
    }

    @Override
    public CompletableResultCode export(Collection<LogRecordData> logs) {
        return withDelegate(delegate -> delegate.export(logs));
    }

    @Override
    public CompletableResultCode flush() {
        return withDelegate(LogRecordExporter::flush);
    }

    private CompletableResultCode withDelegate(Function<LogRecordExporter, CompletableResultCode> operation) {
        final ExporterHolder holder = acquireCurrent();
        if (holder == null) {
            return CompletableResultCode.ofExceptionalFailure(new IllegalStateException("log exporter is shut down"));
        }

        CompletableResultCode result = null;
        try {
            result = operation.apply(holder.delegate);
        } finally {
            if (result == null) {
                holder.decRef();
            }
        }
        return result.whenComplete(holder::decRef);
    }

    /**
     * Prevents new operations and shuts down all delegates, including retired ones. In-flight operations may be cancelled
     * during this explicit teardown. Repeated calls return the same result, which waits for every delegate's shutdown.
     */
    @Override
    public CompletableResultCode shutdown() {
        final List<ExporterHolder> toShutdown = beginShutdown();
        if (toShutdown != null) {
            completeWith(CompletableResultCode.ofAll(toShutdown.stream().map(ExporterHolder::shutdown).toList()), shutdownResult);
        }
        return shutdownResult;
    }

    /**
     * Takes ownership of {@code newDelegate} and routes subsequent operations to it. The previous delegate shuts down
     * after its active operations finish. If teardown has begun, closes the new delegate and rejects the swap.
     */
    void swap(LogRecordExporter newDelegate) {
        final ExporterHolder replacement = new ExporterHolder(newDelegate);
        final ExporterHolder previous = replaceCurrent(replacement);
        if (previous == null) {
            replacement.decRef();
            throw new IllegalStateException("log exporter is shut down");
        }
        previous.decRef();
    }

    /** Returns a retained current holder, or {@code null} once shutdown has begun. */
    @Nullable
    private synchronized ExporterHolder acquireCurrent() {
        if (current != null) {
            current.mustIncRef();
        }
        return current;
    }

    /** Transfers the previous holder's ownership reference to the caller, or rejects replacement after shutdown. */
    @Nullable
    private synchronized ExporterHolder replaceCurrent(ExporterHolder replacement) {
        final ExporterHolder previous = current;
        if (previous != null) {
            current = replacement;
            exporters.add(replacement);
        }
        return previous;
    }

    /** Prevents new acquisitions and captures the holders to shut down outside the monitor. */
    @Nullable
    private synchronized List<ExporterHolder> beginShutdown() {
        if (current == null) {
            return null;
        }
        current = null;
        return List.copyOf(exporters);
    }

    private synchronized void removeExporter(ExporterHolder exporter) {
        exporters.remove(exporter);
    }

    private static void completeWith(CompletableResultCode result, CompletableResultCode target) {
        result.whenComplete(() -> {
            if (result.isSuccess()) {
                target.succeed();
            } else {
                target.failExceptionally(result.getFailureThrowable());
            }
        });
    }

    /**
     * Owns one reference while current and one per active operation. Forced shutdown and reference exhaustion share
     * a single shutdown result so that explicit teardown can cancel work without racing normal retirement.
     */
    private final class ExporterHolder extends AbstractRefCounted {
        private final LogRecordExporter delegate;
        private final AtomicBoolean shutdownStarted = new AtomicBoolean();
        private final CompletableResultCode shutdownResult = new CompletableResultCode();

        private ExporterHolder(LogRecordExporter delegate) {
            this.delegate = Objects.requireNonNull(delegate);
            shutdownResult.whenComplete(() -> removeExporter(this));
        }

        @Override
        protected void closeInternal() {
            shutdown();
        }

        private CompletableResultCode shutdown() {
            if (shutdownStarted.compareAndSet(false, true)) {
                try {
                    completeWith(delegate.shutdown(), shutdownResult);
                } catch (Exception e) {
                    shutdownResult.failExceptionally(e);
                }
            }
            return shutdownResult;
        }
    }
}
