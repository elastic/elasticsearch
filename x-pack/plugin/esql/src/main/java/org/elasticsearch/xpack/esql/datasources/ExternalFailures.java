/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * The single authority on exception <em>shape</em> for external-source reads. One rule underpins all of it:
 * a failure to read or decode the resource is the caller's bad input (HTTP 400), and no intermediate wrapper
 * may hide that and let it surface as a server fault (500). Three operations enforce it, each used at a
 * different layer:
 * <ul>
 *     <li>{@link #classify} — at the read boundary, map a failure to the {@link ExternalException} carrying
 *     the right HTTP status.</li>
 *     <li>{@link #asUnchecked} — at a worker throw site, rethrow a stored failure as an unchecked exception
 *     while preserving an {@link IOException} so {@link #classify} still sees a 400.</li>
 *     <li>{@link #unwrapAsync} — strip {@code java.util.concurrent} wrappers to the real cause before a
 *     caller dispatches on (or classifies) it.</li>
 * </ul>
 * <p>
 * {@link #classify} is the single boundary where external-source reads turn into a user-visible error, and it
 * is reached only for external-source queries (the operator exists only for them), so index queries are
 * unaffected. It runs co-located with the throw, on the node that reads the external source, before
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
 *     <li>An {@link IllegalArgumentException} already maps to 400; it is returned as-is.</li>
 *     <li>An {@link IOException}/{@link UncheckedIOException}, or one of the specific third-party
 *     decoding exceptions in {@link #MALFORMED_DATA_EXCEPTIONS}, means we could not read or interpret
 *     the resource — a client-class {@link ExternalClientException} (400). Retryable transport failures
 *     never reach here as plain I/O errors: the storage layer raises them as {@link ExternalException}
 *     (503) first.</li>
 *     <li>Failing the above, the cause chain is walked as a backstop: a data-class cause (an
 *     {@link IOException}/{@link UncheckedIOException} or a known decoding exception) buried under a non-IO
 *     wrapper still means "could not read or decode", so it classifies 400. The throw sites preserve the
 *     {@link IOException} directly; this covers any path that does not.</li>
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
        if (t instanceof IllegalArgumentException iae) {
            return iae;
        }
        if (isDataException(t)) {
            return new ExternalClientException(t, "Failed to read external source: {}", t.getMessage());
        }
        // Backstop for a data-class failure buried under a non-IO wrapper: classify on it, not the wrapper.
        Throwable dataCause = findDataCause(t);
        if (dataCause != null) {
            return new ExternalClientException(t, "Failed to read external source: {}", dataCause.getMessage());
        }
        return new ExternalServerException(t, "Unexpected failure reading external source: {}", t.getMessage());
    }

    /**
     * Rethrows a stored worker failure as an unchecked exception, preserving an {@link IOException} as an
     * {@link UncheckedIOException} so {@link #classify} still keys it to 400 rather than a bare
     * {@link RuntimeException} that would be a 500. A {@link RuntimeException} is returned unchanged; anything
     * else is wrapped in a {@link RuntimeException} with {@code fallbackMessage}. The parallel parsing
     * coordinators call this from their {@code checkError} throw site.
     */
    static RuntimeException asUnchecked(Throwable failure, String fallbackMessage) {
        if (failure instanceof RuntimeException re) {
            return re;
        }
        if (failure instanceof IOException ioe) {
            return new UncheckedIOException(ioe);
        }
        return new RuntimeException(fallbackMessage, failure);
    }

    /**
     * Strips the {@code java.util.concurrent} wrappers that carry a failed computation's real cause —
     * {@link ExecutionException} and {@link CompletionException} — and returns the first non-wrapper
     * throwable. Cycle-safe. Lets a caller dispatch on (or {@link #classify}) the underlying exception type
     * rather than the async plumbing that buried it.
     */
    static Throwable unwrapAsync(Throwable t) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = t;
        while ((current instanceof ExecutionException || current instanceof CompletionException)
            && current.getCause() != null
            && seen.add(current)) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * A failure that, by contract, means we could not read or decode the resource (bad input, 400) rather
     * than a bug in our own code: an {@link IOException}/{@link UncheckedIOException} or one of the known
     * third-party decoding exceptions in {@link #MALFORMED_DATA_EXCEPTIONS}.
     */
    private static boolean isDataException(Throwable t) {
        return t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t);
    }

    /**
     * Walks the cause chain (cycle-safe) and returns the first {@link #isDataException data-class} cause, or
     * {@code null} if none is found. The top-level throwable is excluded — callers test it directly first.
     */
    private static Throwable findDataCause(Throwable t) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        seen.add(t);
        for (Throwable c = t.getCause(); c != null && seen.add(c); c = c.getCause()) {
            if (isDataException(c)) {
                return c;
            }
        }
        return null;
    }

    private static boolean isMalformedDataException(Throwable t) {
        return MALFORMED_DATA_EXCEPTIONS.contains(t.getClass().getName());
    }
}
