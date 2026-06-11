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
 * a failure's definitive HTTP status — a read/decode failure is the caller's 400, an
 * {@link ElasticsearchException} pins its own 400/429/503 — must survive any intermediate status-neutral
 * wrapper (a bare {@link RuntimeException} from a coordinator/mapper, an {@code ExecutionException} from a
 * cache) instead of surfacing as a generic 500. Four operations enforce it, each used at a different layer:
 * <ul>
 *     <li>{@link #classify} — at the read boundary, map a failure to the exception carrying the right
 *     HTTP status.</li>
 *     <li>{@link #asUnchecked} — at a worker throw site, rethrow a stored failure as an unchecked exception
 *     while keeping an {@link IOException}'s type visible to any consumer.</li>
 *     <li>{@link #unwrapAsync} — strip {@code java.util.concurrent} wrappers to the real cause before a
 *     caller dispatches on (or classifies) it.</li>
 *     <li>{@link #statusCarryingCause} — find a buried status-carrying {@link ElasticsearchException} for
 *     callers with their own error-wrapping contract (e.g. split discovery).</li>
 * </ul>
 * <p>
 * {@link #classify} is the single boundary where external-source reads turn into a user-visible error, and it
 * is reached only for external-source queries (the operator exists only for them), so index queries are
 * unaffected. It runs co-located with the throw, on the node that reads the external source, before
 * the failure is serialized back to the coordinator — so classification relies on the concrete
 * exception type while it is still available, and only the resulting {@code status()} needs to cross
 * the wire (see {@link org.elasticsearch.xpack.esql.datasources.spi.ExternalException}). The ranking,
 * checked against the whole cause chain, strongest signal first:
 * <ul>
 *     <li>{@link Error} (assertion failures, OOM, …) is rethrown — a JVM/programming fault must stay
 *     fatal, never be downgraded to a request error.</li>
 *     <li>An {@link ElasticsearchException} <em>anywhere</em> in the chain wins and is returned unchanged —
 *     it pins its own status: the {@link ExternalException} family (400/500/503),
 *     {@code CircuitBreakingException} (429), {@code TaskCancelledException} (400). It outranks the generic
 *     data-error 400 below: a circuit-breaker trip wrapped in a read {@link IOException} is a retryable 429,
 *     not "your data is bad".</li>
 *     <li>Failing that, the first {@link IllegalArgumentException} (400) or read/decode failure — an
 *     {@link IOException}/{@link UncheckedIOException} or a known third-party decoding exception in
 *     {@link #MALFORMED_DATA_EXCEPTIONS} — becomes a client-class {@link ExternalClientException} (400).
 *     Retryable transport failures never reach here as plain I/O errors: the storage layer raises them as
 *     {@link ExternalException} (503) first.</li>
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
        // Two passes over the cause chain, strongest signal first — see the class Javadoc for the ranking.
        ElasticsearchException carrier = statusCarryingCause(t);
        if (carrier != null) {
            return carrier;
        }
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable c = t; c != null && seen.add(c); c = c.getCause()) {
            if (c instanceof IllegalArgumentException iae) {
                return iae;
            }
            if (isDataException(c)) {
                String detail = c.getMessage() != null ? c.getMessage() : c.getClass().getSimpleName();
                return new ExternalClientException(t, "Failed to read external source: {}", detail);
            }
        }
        return new ExternalServerException(t, "Unexpected failure reading external source: {}", t.getMessage());
    }

    /**
     * Rethrows a stored worker failure as an unchecked exception. An {@link Error} is rethrown unchanged —
     * never downgraded to a request error. An {@link IOException} is preserved as an
     * {@link UncheckedIOException} so its type stays visible to any consumer without relying on
     * {@link #classify}'s cause-chain backstop. A {@link RuntimeException} is returned unchanged; anything
     * else is wrapped in a {@link RuntimeException} with {@code fallbackMessage}. The parallel parsing
     * coordinators call this from their {@code checkError} throw site.
     */
    static RuntimeException asUnchecked(Throwable failure, String fallbackMessage) {
        if (failure instanceof Error error) {
            throw error;
        }
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
