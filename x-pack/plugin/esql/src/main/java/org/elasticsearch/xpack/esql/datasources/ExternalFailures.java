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
 *     <li>Otherwise the cause chain is walked and the first throwable carrying a definitive status wins,
 *     so a status-neutral wrapper (a bare {@link RuntimeException} from a coordinator/mapper, an
 *     {@code ExecutionException} from a cache) cannot bury it. Per node, in priority order: an
 *     {@link ElasticsearchException} already pins its own status and is returned unchanged — the
 *     {@link ExternalException} family (400/500/503), {@code CircuitBreakingException} (429),
 *     {@code TaskCancelledException} (400); an {@link IllegalArgumentException} is 400; an
 *     {@link IOException}/{@link UncheckedIOException} or a known third-party decoding exception in
 *     {@link #MALFORMED_DATA_EXCEPTIONS} means we could not read or decode the resource — a client-class
 *     {@link ExternalClientException} (400). Retryable transport failures never reach here as plain I/O
 *     errors: the storage layer raises them as {@link ExternalException} (503) first.</li>
 *     <li>Only if nothing in the chain is classifiable ({@link IllegalStateException},
 *     {@link NullPointerException}, an unrecognized {@link RuntimeException}, a bare
 *     {@link InterruptedException}) is it a broken invariant in our own code — an
 *     {@link ExternalServerException} (500) that keeps the bug visible.</li>
 * </ul>
 * Cancellation is not special-cased: it arrives as a {@code TaskCancelledException} (the
 * {@link ElasticsearchException} branch, 400), and a read interrupted while blocking surfaces as an
 * {@link IOException} subclass (so, 400). The interrupt flag is intentionally left untouched, since this
 * runs on the thread surfacing the stored failure, not the worker thread that was interrupted.
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
        // Walk the cause chain (cycle-safe) and classify on the first throwable that carries a definitive
        // status, so a status-neutral wrapper — a bare RuntimeException from a coordinator/mapper, an
        // ExecutionException from a cache — cannot bury it and force a 500. The per-node order mirrors the
        // priority: an ElasticsearchException already pins its own status (400/429/503/…); an
        // IllegalArgumentException is 400; a read/decode failure is a client-class 400. Only if nothing in
        // the chain is classifiable is it a 500 — a genuine broken invariant in our own code.
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable c = t; c != null && seen.add(c); c = c.getCause()) {
            if (c instanceof ElasticsearchException ese) {
                return ese;
            }
            if (c instanceof IllegalArgumentException iae) {
                return iae;
            }
            if (isDataException(c)) {
                return new ExternalClientException(t, "Failed to read external source: {}", c.getMessage());
            }
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
     * Walks the cause chain (cycle-safe) and returns the first {@link ElasticsearchException} — which carries
     * its own HTTP status (400/429/503/…) — or {@code null} if none is present. Lets a caller on a path with
     * its own error-wrapping contract (e.g. split discovery, which annotates failures with the source path)
     * preserve a buried 429/503 that a status-neutral wrapper would otherwise flatten to a 500, <em>without</em>
     * reclassifying other exception types the way {@link #classify} does for the read boundary.
     */
    static ElasticsearchException statusCarryingCause(Throwable t) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable c = t; c != null && seen.add(c); c = c.getCause()) {
            if (c instanceof ElasticsearchException ese) {
                return ese;
            }
        }
        return null;
    }

    /**
     * A failure that, by contract, means we could not read or decode the resource (bad input, 400) rather
     * than a bug in our own code: an {@link IOException}/{@link UncheckedIOException} or one of the known
     * third-party decoding exceptions in {@link #MALFORMED_DATA_EXCEPTIONS}.
     */
    private static boolean isDataException(Throwable t) {
        return t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t);
    }

    private static boolean isMalformedDataException(Throwable t) {
        return MALFORMED_DATA_EXCEPTIONS.contains(t.getClass().getName());
    }
}
