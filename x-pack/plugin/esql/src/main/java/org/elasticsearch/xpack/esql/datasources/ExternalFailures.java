/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.regex.Pattern;

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

    private static final Logger logger = LogManager.getLogger(ExternalFailures.class);

    private ExternalFailures() {}

    /**
     * Storage-URI scheme prefixes (object stores, HTTP, Arrow Flight / gRPC, local files) that must never appear in a
     * message handed to a caller who may only read the dataset.
     */
    private static final String[] STORAGE_URI_SCHEMES = {
        "s3://",
        "s3a://",
        "s3n://",
        "gs://",
        "wasb://",
        "wasbs://",
        "http://",
        "https://",
        "flight://",
        "grpc://",
        "grpcs://",
        "file:/" };

    /**
     * Domains of the cloud object-store endpoints. SDK and DNS failures name the endpoint host without a scheme
     * ({@code bucket.s3.us-east-1.amazonaws.com: Name or service not known}), and the host embeds the bucket or account.
     */
    private static final String[] STORAGE_HOST_DOMAINS = {
        "amazonaws.com",
        "googleapis.com",
        "core.windows.net",
        "core.usgovcloudapi.net",
        "core.chinacloudapi.cn" };

    /** A bare filesystem location: an absolute POSIX path or a Windows drive path, at the start or after a delimiter. */
    private static final Pattern ABSOLUTE_FILESYSTEM_PATH = Pattern.compile(
        "(?:^|[\\s\\[(<'\"=,])/[^\\s/\\[\\]()<>'\",]+|(?:^|[\\s\\[(<'\"=,])[A-Za-z]:[\\\\/]"
    );

    /**
     * Returns {@code true} when {@code message} contains no known storage-URI scheme, cloud storage host or absolute
     * filesystem path. Hosts of custom endpoints and relative paths are not recognised. Used to decide whether a
     * diagnostic message from a third-party library is safe to forward to the user.
     */
    public static boolean safeForUserMessage(String message) {
        if (message == null) {
            return true;
        }
        for (String scheme : STORAGE_URI_SCHEMES) {
            if (message.contains(scheme)) {
                return false;
            }
        }
        for (String domain : STORAGE_HOST_DOMAINS) {
            if (message.contains(domain)) {
                return false;
            }
        }
        return ABSOLUTE_FILESYSTEM_PATH.matcher(message).find() == false;
    }

    /**
     * Returns a fresh copy of {@code e} without its cause chain, after logging the chain on this node. Suppressed
     * failures that are themselves external failures are detached and kept; any other suppressed failure is dropped,
     * since suppressed failures are serialised into the response too.
     */
    public static ExternalException detach(ExternalException e) {
        if (e.getCause() != null) {
            logger.debug("External failure detached from its cause (cause logged, not forwarded)", e);
        }
        ExternalException detached = e.withoutCause();
        for (Throwable suppressed : e.getSuppressed()) {
            if (suppressed instanceof ExternalException external) {
                detached.addSuppressed(detach(external));
            }
        }
        return detached;
    }

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
        if (t instanceof ExternalException ee) {
            return detach(ee);
        }
        if (t instanceof ElasticsearchException ese) {
            return ese;
        }
        RuntimeException control = controlFlowFailure(t);
        if (control != null) {
            return control;
        }
        if (t instanceof EsRejectedExecutionException rejected) {
            // A thread pool refusing the task (e.g. the node shutting down) is client-actionable backpressure,
            // not a server fault. The type already maps to 429 (TOO_MANY_REQUESTS) via ExceptionsHelper.status,
            // so it is returned unchanged rather than falling through to the 500 ExternalServerException below.
            return rejected;
        }
        if (t instanceof IllegalArgumentException iae) {
            // A format reader's IAE may embed a storage URI. Log it here and never chain it, so its message cannot
            // surface in caused_by; forward the message only when it names no location.
            logger.debug("External read failed with a client error (cause logged, not forwarded)", iae);
            return new ExternalClientException("Malformed external data: {}", safeDetail(iae));
        }
        if (t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t)) {
            // Storage clients embed full URIs in their messages and causes: log on this node, do not chain.
            logger.debug("External read failed with a client error (cause logged, not forwarded)", t);
            return new ExternalClientException("Failed to read external source: {}", safeDetail(t));
        }
        // Unchecked SDK failures (e.g. AWS SdkClientException) land here and may name the bucket host. The only
        // diagnosis of a server-side failure is this node's log, so it is logged at WARN rather than chained.
        logger.warn("Unexpected failure reading external source (cause logged, not forwarded)", t);
        return new ExternalServerException("Unexpected failure reading external source: {}", safeDetail(t));
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
        RuntimeException control = controlFlowFailure(failure);
        if (control != null) {
            return control;
        }
        // UncheckedIOException is checked before the generic RuntimeException branch so its IO origin is
        // typed as 400 with the coordinator's context prefix, instead of falling through unchanged and
        // letting classify() re-wrap it with the boundary's generic message.
        if (failure instanceof IOException || failure instanceof UncheckedIOException) {
            logger.debug("External read failed (cause logged, not forwarded)", failure);
            return new ExternalClientException("{}: {}", fallbackMessage, safeDetail(failure));
        }
        if (failure instanceof RuntimeException re) {
            return re;
        }
        logger.warn("External read failed (cause logged, not forwarded)", failure);
        return new ExternalServerException("{}: {}", fallbackMessage, safeDetail(failure));
    }

    /**
     * A cancellation or circuit-breaker trip buried under an I/O or wrapper exception. Callers up the stack look for
     * these two by type in the cause chain (to stop retrying, or to report 429), so they must not be flattened into a
     * generic 400/500 when the cause is dropped. Neither names a storage location.
     */
    private static RuntimeException controlFlowFailure(Throwable t) {
        Throwable control = ExceptionsHelper.unwrap(t, TaskCancelledException.class, CircuitBreakingException.class);
        return control instanceof RuntimeException re ? re : null;
    }

    /**
     * Wraps a row-level read failure in an {@link ExternalClientException} that keeps {@code cause} chained, so a caller
     * further up can still recognise it by type (the CSV reader walks the chain for an over-{@code max_record_size}
     * drop). Unlike {@link #surface}, the cause is <em>not</em> dropped: use it only for a cause whose message and chain
     * name no storage location.
     *
     * @param safeMessage a caller-controlled message naming only the object (never the full path)
     */
    public static ExternalClientException rowError(Throwable cause, String safeMessage) {
        return new ExternalClientException(cause, "{}", safeMessage);
    }

    /**
     * Wraps a failure in an {@link ElasticsearchException} whose message is {@code context} plus the failure's detail
     * when that names no storage location. The failure itself is logged on this node and not chained, since the
     * REST layer renders the whole cause chain and storage-client causes name the bucket and key.
     */
    public static ElasticsearchException wrapWithoutCause(String context, Throwable failure) {
        if (controlFlowFailure(failure) instanceof ElasticsearchException control) {
            return control;
        }
        logger.warn(() -> context, failure);
        return new ElasticsearchException("{}: {}", context, safeDetail(failure));
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

    /** Like {@link #detail} but falls back to the class name when the message names a storage location. */
    public static String safeDetail(Throwable failure) {
        return safeForUserMessage(failure.getMessage()) && namesNoHost(failure) ? detail(failure) : failure.getClass().getSimpleName();
    }

    /** A URI anywhere in a message: a scheme, {@code ://}, then everything up to a delimiter. */
    private static final Pattern URI_IN_MESSAGE = Pattern.compile("[A-Za-z][A-Za-z0-9+.-]*://[^\\s\\]\\[,;)]*");

    /**
     * The reason a validator rejected a dataset or data source: the message of the deepest {@link IllegalArgumentException}
     * in {@code failure}'s chain (the failure itself included, since a validator's exception is often the one thrown) that
     * is safe once any URI in it is replaced by {@code <location>} (e.g. {@code endpoint [<location>] must use https}).
     * Only an {@code IllegalArgumentException} qualifies: validators raise those, whereas storage-client failures, which
     * name the bucket and host, never are. Returns {@code null} if the chain has none.
     */
    public static String validationReason(Throwable failure) {
        String reason = null;
        Throwable t = failure;
        for (int depth = 0; t != null && depth < 12; depth++, t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof IllegalArgumentException && t.getMessage() != null && namesNoHost(t)) {
                String redacted = URI_IN_MESSAGE.matcher(t.getMessage()).replaceAll("<location>");
                if (safeForUserMessage(redacted)) {
                    reason = redacted;
                }
            }
        }
        return reason;
    }

    /**
     * Network failures name the endpoint host ({@code Unable to resolve host minio.internal}, {@code Connect to
     * minio.internal:9000 failed}) without a scheme or a cloud domain, so {@link #safeForUserMessage} cannot recognise
     * them; the endpoint of a custom object store, or a Flight server, is part of the dataset location.
     */
    private static boolean namesNoHost(Throwable failure) {
        if (ExceptionsHelper.unwrap(failure, UnknownHostException.class, ConnectException.class, NoRouteToHostException.class) != null) {
            return false;
        }
        Throwable t = failure;
        for (int depth = 0; t != null && depth < 12; depth++, t = t.getCause() == t ? null : t.getCause()) {
            String name = t.getClass().getName();
            if (name.startsWith("io.grpc.") || name.startsWith("org.apache.arrow.flight.") || name.endsWith("HttpHostConnectException")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Drops the query string, fragment and user info from an {@code http}/{@code https} location, where a pre-signed
     * URL carries its signature and a {@code user:pass@} its credentials. Other schemes are returned unchanged: their
     * user info is not a secret (for {@code wasb}/{@code wasbs} it is the container name).
     */
    public static String redactHttpUrl(String location) {
        int schemeEnd = location.indexOf("://");
        if (schemeEnd < 0) {
            return location;
        }
        String scheme = location.substring(0, schemeEnd);
        if (scheme.equalsIgnoreCase("http") == false && scheme.equalsIgnoreCase("https") == false) {
            return location;
        }
        String rest = location.substring(schemeEnd + 3);
        int query = rest.indexOf('?');
        int fragment = rest.indexOf('#');
        int end = Math.min(query < 0 ? rest.length() : query, fragment < 0 ? rest.length() : fragment);
        rest = rest.substring(0, end);
        int pathStart = rest.indexOf('/');
        int at = rest.lastIndexOf('@', pathStart < 0 ? rest.length() - 1 : pathStart - 1);
        return location.substring(0, schemeEnd + 3) + (at < 0 ? rest : rest.substring(at + 1));
    }
}
