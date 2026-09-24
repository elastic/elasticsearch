/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

/**
 * Classifies a failure raised while reading an external data source into the exception an external-read
 * operator should surface, so that it maps to the right HTTP status. The
 * companion {@link #surface} helper is used at the worker rethrow sites inside parallel coordinators and
 * page iterators to pre-type the failure: it wraps a raw {@link IOException} in an already-classified
 * {@link ExternalClientException} (400) so the read boundary's {@link #classify} sees a status-typed
 * exception. {@code surface} cannot rescue a status signal already buried under a status-neutral
 * {@link RuntimeException} wrapper; callers must therefore pass the <em>raw</em> stored throwable, not a
 * pre-wrapped one.
 * <p>
 * {@link #classify} is the shared policy boundary where external-source reads turn into a user-visible
 * error. Both eager reads in {@code AsyncExternalSourceOperator} and deferred reads in
 * {@code ExternalFieldExtractOperator} invoke it at their local read boundary; neither operator exists
 * for index queries, so those are unaffected. Classification runs co-located with the throw, on the node
 * that reads the external source, before the failure is serialized back to the coordinator — so it relies
 * on the concrete exception type while it is still available, and only the resulting public exception
 * name and {@code status()} need to cross the wire (see {@link ExternalException}). The policy:
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
 *     <li>An {@link IllegalArgumentException} from a format reader may embed a full storage URI; it is wrapped
 *     in an {@link ExternalClientException} (400) with a path-free message and no cause chain, so the IAE
 *     message never appears in {@code caused_by}. The original is logged at {@code WARN} on this node.</li>
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

    private static final Logger logger = LogManager.getLogger(ExternalFailures.class);

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

    /** Depth bound for cause-chain walks. Real chains are 2-4 deep; this only stops a pathological one. */
    private static final int MAX_CAUSE_DEPTH = 12;

    /**
     * Storage-URI scheme prefixes that must never appear in an {@link ExternalException} message
     * handed to a caller. Used by the {@code assert} guard in {@link #classify}.
     */
    private static final String[] STORAGE_URI_SCHEMES = {
        "s3://",
        "s3a://",
        "s3n://",
        "gs://",
        "wasb://",
        "wasbs://",
        // Azure Blob Storage HTTPS endpoint (https://account.blob.core.windows.net/container/blob)
        ".blob.core.windows.net/",
        // GCS HTTPS endpoint (https://storage.googleapis.com/bucket/object)
        "storage.googleapis.com/" };

    /**
     * Returns {@code true} when no message in {@code e}'s full cause chain contains a known
     * storage-URI scheme. A {@code false} result means a full object-store path leaked into a
     * user-facing exception message.
     */
    static boolean noStoragePathLeaked(RuntimeException e) {
        Throwable current = e;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH; depth++) {
            if (containsStoragePath(current.getMessage())) {
                return false;
            }
            Throwable cause = current.getCause();
            if (cause == null || cause == current) {
                break;
            }
            current = cause;
        }
        return true;
    }

    private static boolean containsStoragePath(String msg) {
        if (msg == null) {
            return false;
        }
        for (String scheme : STORAGE_URI_SCHEMES) {
            if (msg.contains(scheme)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the {@link RuntimeException} to throw for the given read failure. May instead throw if
     * {@code t} is an {@link Error}, which must propagate unchanged.
     * <p>
     * Under {@code -ea} (assertions enabled), verifies that no message in the result's full cause chain
     * contains a known storage-URI scheme — a debug guard that fires immediately if a new throw site
     * embeds a full path instead of using the structured constructors on {@link ExternalException}.
     * See {@link #noStoragePathLeaked}.
     */
    public static RuntimeException classify(Throwable t) {
        if (t instanceof Error error) {
            throw error;
        }
        if (t instanceof ElasticsearchException ese) {
            assert noStoragePathLeaked(ese) : "storage path leaked in ExternalException: " + ese.getMessage();
            return ese;
        }
        if (t instanceof EsRejectedExecutionException rejected) {
            return rejected;
        }
        if (t instanceof IllegalArgumentException iae) {
            // IAE from format readers may embed storage URIs in the message. Log at WARN on this node for
            // debugging; do not chain it into the exception so its message never crosses the wire.
            logger.warn("External read failed with IllegalArgumentException (cause logged, not forwarded)", iae);
            ExternalClientException iaeResult = new ExternalClientException("Malformed external data ({})", iae.getClass().getSimpleName());
            // Include the IAE detail only when it is free of storage-URI schemes; a Parquet reader may surface
            // a column name or file basename that is useful for diagnosis without leaking the full object path.
            if (iae.getMessage() != null && containsStoragePath(iae.getMessage()) == false) {
                iaeResult.setDetail(iae.getMessage());
            }
            return iaeResult;
        }
        RuntimeException result;
        if (t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t)) {
            result = new ExternalClientException(t, "Failed to read external source: {}", detail(t));
        } else {
            result = new ExternalServerException(t, "Unexpected failure reading external source: {}", detail(t));
        }
        assert noStoragePathLeaked(result) : "storage path leaked in classified exception: " + result.getMessage();
        return result;
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
     *
     * @param failure the raw stored worker-side throwable; <em>not</em> a status-neutral wrapper around it
     * @param fallbackMessage non-null context prefix included in every wrapped result
     */
    public static RuntimeException surface(Throwable failure, String fallbackMessage) {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof IOException || failure instanceof UncheckedIOException) {
            return new ExternalClientException(failure, "{}: {}", fallbackMessage, detail(failure));
        }
        if (failure instanceof RuntimeException re) {
            return re;
        }
        return new ExternalServerException(failure, "{}: {}", fallbackMessage, detail(failure));
    }

    private static boolean isMalformedDataException(Throwable t) {
        Throwable current = t;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH; depth++) {
            if (MALFORMED_DATA_EXCEPTIONS.contains(current.getClass().getName())) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause == null || cause == current) {
                break;
            }
            current = cause;
        }
        return false;
    }

    /**
     * Best-effort short description of a failure for inclusion in a typed exception's message.
     */
    private static String detail(Throwable failure) {
        return failure.getMessage() != null ? failure.getMessage() : failure.getClass().getSimpleName();
    }

    /**
     * {@link #detail} of the first exception in {@code failure}'s chain that carries a message someone wrote, rather
     * than one a wrapper derived from {@link Throwable#toString()}.
     */
    public static String rootDetail(Throwable failure) {
        return detail(rootCause(failure));
    }

    /**
     * The throwable {@link #rootDetail} takes its message from.
     */
    public static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH; depth++) {
            Throwable cause = current.getCause();
            if (cause == null || cause == current || derivesMessageFrom(current, cause) == false) {
                return current;
            }
            current = cause;
        }
        return current;
    }

    private static boolean derivesMessageFrom(Throwable wrapper, Throwable cause) {
        String message = wrapper.getMessage();
        return message == null || message.equals(cause.toString());
    }
}
