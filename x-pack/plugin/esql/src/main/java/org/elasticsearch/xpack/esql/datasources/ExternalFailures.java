/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

/**
 * Classifies a failure raised while reading an external data source into the exception the
 * {@code AsyncExternalSourceOperator} should surface, so that it maps to the right HTTP status. The
 * companion {@link #surface} helper is used at the worker rethrow sites inside parallel coordinators and
 * page iterators to pre-type the failure: it wraps a raw {@link IOException} in an already-classified
 * {@link ExternalClientException} (400) so the read boundary's {@link #classify} sees a status-typed
 * exception. {@code surface} cannot rescue a status signal already buried under a status-neutral
 * {@link RuntimeException} wrapper; callers must therefore pass the <em>raw</em> stored throwable, not a
 * pre-wrapped one.
 * <p>
 * {@link #classify} is the single boundary where external-source reads turn into a user-visible error,
 * and it is reached only for external-source queries (the operator exists only for them), so index queries
 * are unaffected. It runs co-located with the throw, on the node that reads the external source, before
 * the failure is serialized back to the coordinator — so classification relies on the concrete
 * exception type while it is still available, and only the resulting {@code status()} needs to cross
 * the wire (see {@link org.elasticsearch.xpack.esql.datasources.spi.ExternalException}). The policy:
 * <ul>
 *     <li>{@link Error} (assertion failures, OOM, …) is rethrown — a JVM/programming fault must stay
 *     fatal, never be downgraded to a request error.</li>
 *     <li>An {@link ElasticsearchException} already carries its own status and is returned unchanged:
 *     this covers the {@link ExternalException} family (400/500/503) raised at the reader/storage
 *     boundary, as well as {@code CircuitBreakingException} (429) and {@code TaskCancelledException}
 *     (400).</li>
 *     <li>An {@link EsRejectedExecutionException} — a thread pool refusing the task (e.g. the node shutting
 *     down) — is client-actionable backpressure, not a server fault. It already maps to 429 (TOO_MANY_REQUESTS)
 *     via {@code ExceptionsHelper.status}, so it is returned unchanged rather than mistaken for a broken
 *     invariant and reported as 500.</li>
 *     <li>An {@link IllegalArgumentException} already maps to 400; it is returned as-is.</li>
 *     <li>An {@link IOException}/{@link UncheckedIOException}, or one of the specific third-party
 *     decoding exceptions in {@link #MALFORMED_DATA_EXCEPTIONS}, means we could not read or interpret
 *     the resource — a client-class {@link ExternalClientException} (400). Retryable transport failures
 *     never reach here as plain I/O errors: the storage layer raises them as {@link ExternalException}
 *     (503) first.</li>
 *     <li>Anything else ({@link IllegalStateException}, {@link NullPointerException}, an unrecognized
 *     {@link RuntimeException}) indicates a broken invariant in our own code rather than bad input,
 *     so it surfaces as an {@link ExternalServerException} (500) to keep the bug visible.</li>
 * </ul>
 * Cancellation is not special-cased here: it arrives as a {@code TaskCancelledException} (handled by the
 * {@link ElasticsearchException} branch, 400), and a read interrupted while blocking surfaces as an
 * {@link IOException} subclass (so, 400). A bare {@link InterruptedException} would fall through to 500;
 * the interrupt flag is intentionally left untouched, since this runs on the thread surfacing the stored
 * failure, not the worker thread that was interrupted.
 */
public final class ExternalFailures {

    private ExternalFailures() {}

    /**
     * Third-party decoding exceptions that are, by contract, malformed-input signals rather than bugs —
     * currently Parquet's {@code ParquetDecodingException} ("could not read page ..."). They are
     * unchecked {@link RuntimeException}s (not {@link IOException}s), so without this they would be
     * mistaken for a bug and reported as 500. Matched by exact class name (the types are not on this
     * module's compile classpath), and deliberately <em>not</em> by package prefix: the
     * {@code org.apache.parquet} package also holds bug-class types (e.g. {@code ShouldNeverHappenException},
     * {@code BadConfigurationException}, {@code ParquetEncodingException}) that must stay 500. Likewise a
     * {@link NullPointerException}/{@link ArrayIndexOutOfBoundsException} thrown from inside a library is a
     * bug, not bad input, and is not listed here. This is only a backstop; the reader modules, which have
     * these types on their classpath, remain the primary place that translates them.
     */
    private static final Set<String> MALFORMED_DATA_EXCEPTIONS = Set.of("org.apache.parquet.io.ParquetDecodingException");

    /**
     * Returns the {@link RuntimeException} to throw for the given read failure. May instead throw if
     * {@code t} is an {@link Error}, which must propagate unchanged.
     */
    public static RuntimeException classify(Throwable t) {
        if (t instanceof Error error) {
            throw error;
        }
        if (t instanceof ElasticsearchException ese) {
            return ese;
        }
        if (t instanceof EsRejectedExecutionException rejected) {
            // A thread pool refusing the task (e.g. the node shutting down) is client-actionable backpressure,
            // not a server fault. The type already maps to 429 (TOO_MANY_REQUESTS) via ExceptionsHelper.status,
            // so it is returned unchanged rather than falling through to the 500 ExternalServerException below.
            return rejected;
        }
        if (t instanceof IllegalArgumentException iae) {
            return iae;
        }
        if (t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t)) {
            return new ExternalClientException(t, "Failed to read external source: {}", detail(t));
        }
        // Use detail() rather than the raw getMessage() so a null-message fault (e.g. a bare NPE) surfaces
        // its class name instead of a useless "null", while the original cause stays chained for the stack.
        return new ExternalServerException(t, "Unexpected failure reading external source: {}", detail(t));
    }

    /**
     * Surfaces a worker-side stored failure as a typed ES|QL exception that already carries the right HTTP
     * status, instead of a status-neutral {@link RuntimeException} wrapper. Called at the throw site inside
     * parallel parsing coordinators and page iterators (the boundary between the worker that stored the
     * failure and the consumer pulling from the iterator). Companion to {@link #classify}: where
     * {@code classify} runs at the read boundary and maps a freshly raised failure to a status-typed
     * exception, {@code surface} runs at the worker rethrow site and pins the status carried by the
     * underlying type while preserving a context prefix ({@code fallbackMessage}, e.g.
     * {@code "Streaming parallel parsing failed"}) so the coordinator/iterator origin stays visible in logs
     * and the user-facing message:
     * <ul>
     *     <li>An {@link Error} is rethrown unchanged — a JVM/programming fault must stay fatal.</li>
     *     <li>A {@link RuntimeException} is returned as-is. This covers status carriers
     *     ({@link ElasticsearchException} family, {@link IllegalArgumentException}) which already pin
     *     their own status, and any other unchecked cause raised by the worker. <strong>Note:</strong> a
     *     bare {@link RuntimeException} that buries an {@link IOException} cause is <em>not</em> rescued
     *     here — the wrapper has already destroyed the type signal. Callers must therefore pass the raw
     *     stored throwable, not a pre-wrapped one.</li>
     *     <li>An {@link IOException} or {@link UncheckedIOException} becomes an
     *     {@link ExternalClientException} (400) — undecodable input is a client-class error, not a server
     *     fault. The {@code fallbackMessage} prefix is kept either way, so the context survives whether
     *     the worker raised checked or unchecked I/O.</li>
     *     <li>Anything else (a checked, non-IO exception — typically {@link InterruptedException} stored
     *     after a worker thread was interrupted) becomes an {@link ExternalServerException} (500): we have
     *     no evidence it is the caller's fault, so we keep the bug visible.</li>
     * </ul>
     * Calling {@code surface} at the worker rethrow site lets {@code AsyncExternalSourceOperator}'s
     * {@link #classify} compose cleanly on the result: an {@link ExternalException} returned here passes
     * straight through {@code classify} unchanged; a non-classified {@link RuntimeException} is the only
     * shape {@code classify} still actively re-wraps (into {@link ExternalServerException}, the same status
     * {@code surface} would have produced for an unrecognized worker fault).
     *
     * @param failure the raw stored worker-side throwable; <em>not</em> a status-neutral wrapper around it
     * @param fallbackMessage non-null context prefix (e.g. the coordinator/iterator name); included in
     *                        every wrapped result so the throw-site origin survives classification
     */
    public static RuntimeException surface(Throwable failure, String fallbackMessage) {
        if (failure instanceof Error error) {
            throw error;
        }
        // UncheckedIOException is checked before the generic RuntimeException branch so its IO origin is
        // typed as 400 with the coordinator's context prefix, instead of falling through unchanged and
        // letting classify() re-wrap it with the boundary's generic message.
        if (failure instanceof IOException || failure instanceof UncheckedIOException) {
            return new ExternalClientException(failure, "{}: {}", fallbackMessage, detail(failure));
        }
        if (failure instanceof RuntimeException re) {
            return re;
        }
        return new ExternalServerException(failure, "{}: {}", fallbackMessage, detail(failure));
    }

    private static boolean isMalformedDataException(Throwable t) {
        return MALFORMED_DATA_EXCEPTIONS.contains(t.getClass().getName());
    }

    /**
     * Best-effort short description of a failure for inclusion in a typed exception's message: the
     * {@code getMessage()} if set, otherwise the class's simple name (so a null-message
     * {@link InterruptedException} reads as {@code "InterruptedException"} rather than {@code "null"}).
     */
    private static String detail(Throwable failure) {
        return failure.getMessage() != null ? failure.getMessage() : failure.getClass().getSimpleName();
    }
}
