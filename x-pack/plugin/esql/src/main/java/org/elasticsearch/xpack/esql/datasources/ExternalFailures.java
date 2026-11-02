/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.LogThrottle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.List;
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
 *     <li>An {@link ElasticsearchException} already carries its own status and keeps it: this covers the
 *     {@link ExternalException} family (400/500/503) raised at the reader/storage boundary and
 *     {@code CircuitBreakingException} (429). It is returned unchanged when it has no cause, else
 *     {@link #detach(ElasticsearchException) detached}.</li>
 *     <li>An {@link EsRejectedExecutionException} — a thread pool refusing the task (e.g. the node shutting
 *     down) — is client-actionable backpressure, not a server fault. It already maps to 429 (TOO_MANY_REQUESTS)
 *     via {@code ExceptionsHelper.status}, so it is returned (detached from any cause) rather than mistaken for a
 *     broken invariant and reported as 500.</li>
 *     <li>An {@link IllegalArgumentException} from a format reader may embed a full storage URI; it is wrapped
 *     in an {@link ExternalClientException} (400) with a path-free message and no cause chain, so the IAE
 *     message never appears in {@code caused_by}. The original is logged on this node.</li>
 *     <li>An {@link IOException}/{@link UncheckedIOException}, or one of the specific third-party
 *     decoding exceptions in {@link #MALFORMED_DATA_EXCEPTIONS}, means we could not read or interpret
 *     the resource — a client-class {@link ExternalClientException} (400). Retryable transport failures
 *     never reach here as plain I/O errors: the storage layer raises them as {@link ExternalException}
 *     (503) first.</li>
 *     <li>Anything else ({@link IllegalStateException}, {@link NullPointerException}, an unrecognized
 *     {@link RuntimeException}) indicates a broken invariant in our own code rather than bad input,
 *     so it surfaces as an {@link ExternalServerException} (500) to keep the bug visible.</li>
 * </ul>
 * A {@code TaskCancelledException} anywhere in the chain is returned as the cancellation, whatever wraps it: the
 * result carries no cause, so query failure ranking could not find it there. A read interrupted while blocking
 * surfaces as an {@link IOException} subclass (so, 400). A bare {@link InterruptedException} would fall through to
 * 500; the interrupt flag is intentionally left untouched, since this runs on the thread surfacing the stored
 * failure, not the worker thread that was interrupted.
 * <p>
 * Wherever a failure's own text is forwarded, it goes through {@link #forwardableDetail}: text naming no location,
 * and not composed by a storage client (see {@link #composedByStorageClient}). A storage client's message relays
 * what the remote said (an IAM denial names the principal and resource ARNs), so its class name stands in for it.
 * A format library's or the JDK's message describes the bytes we read (a bad magic number, a truncated footer) and
 * is forwarded.
 */
public final class ExternalFailures {

    private static final Logger logger = LogManager.getLogger(ExternalFailures.class);

    /**
     * Bounds the WARN lines for client failures whose message the response withholds: a corrupt file or a library
     * failing every split would otherwise write one per read. Shared by every dataset and user on the node, so one
     * noisy dataset can push another's first such failure down to DEBUG for the interval.
     */
    static final LogThrottle WITHHELD_MESSAGE_WARN = new LogThrottle(TimeValue.timeValueMinutes(1));

    /** Depth bound for cause-chain walks. Real chains are 2-4 deep; this only stops a pathological one. */
    private static final int MAX_CAUSE_DEPTH = 12;

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
     * {@code e} without its cause chain, after logging the cause on this node: the REST layer renders both, and a cause
     * below an {@link ElasticsearchException} that is not an {@link ExternalException} can still be a storage SDK's
     * exception. Returns {@code e} itself when there is nothing to drop. Otherwise the copy keeps the status, message,
     * metadata and headers, and the type where the state callers act on can be carried over (breaker byte counts,
     * cancellation); any other type becomes an {@link ExternalClientException} (400), an
     * {@link ExternalServerException} (500) or an {@link ElasticsearchStatusException} with the same status.
     * Suppressed {@link ElasticsearchException}s are detached and kept (same as {@link #detach(ExternalException)}), so
     * cycle detection that walks suppressed links still sees them; other suppressed failures are dropped.
     */
    public static ElasticsearchException detach(ElasticsearchException e) {
        return detach(e, Level.WARN);
    }

    /**
     * {@code e} without its cause chain and suppressed failures, after logging them on this node. Keeps the message
     * and {@link EsRejectedExecutionException#isExecutorShutdown()}; returns {@code e} when there is nothing to drop.
     * Not an {@link ElasticsearchException}, so it cannot go through {@link #detach(ElasticsearchException)}.
     */
    public static EsRejectedExecutionException detach(EsRejectedExecutionException e) {
        if (e.getCause() == null && e.getSuppressed().length == 0) {
            return e;
        }
        logger.log(Level.DEBUG, () -> "Failure detached from its cause (cause logged, not forwarded)", e);
        EsRejectedExecutionException copy = new EsRejectedExecutionException(e.getMessage(), e.isExecutorShutdown());
        copy.setStackTrace(e.getStackTrace());
        for (Throwable suppressed : e.getSuppressed()) {
            if (suppressed instanceof ElasticsearchException ese) {
                copy.addSuppressed(detach(ese, Level.DEBUG));
            }
        }
        return copy;
    }

    private static ElasticsearchException detach(ElasticsearchException e, Level serverFailureLevel) {
        if (e instanceof ExternalException ee) {
            return detach(ee);
        }
        if (e.getCause() == null && e.getSuppressed().length == 0) {
            return e;
        }
        Level level = e.status().getStatus() >= 500 ? serverFailureLevel : Level.DEBUG;
        logger.log(level, () -> "Failure detached from its cause (cause logged, not forwarded)", e);
        String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        ElasticsearchException copy;
        if (e instanceof CircuitBreakingException cbe) {
            copy = new CircuitBreakingException(cbe.getMessage(), cbe.getBytesWanted(), cbe.getByteLimit(), cbe.getDurability());
        } else if (e instanceof TaskCancelledException) {
            copy = new TaskCancelledException(e.getMessage());
        } else if (e.status() == RestStatus.BAD_REQUEST) {
            copy = new ExternalClientException("{}", message);
        } else if (e.status() == RestStatus.INTERNAL_SERVER_ERROR) {
            copy = new ExternalServerException("{}", message);
        } else {
            copy = new ElasticsearchStatusException("{}", e.status(), message);
        }
        for (String key : e.getMetadataKeys()) {
            copy.addMetadata(key, e.getMetadata(key));
        }
        for (String key : e.getBodyHeaderKeys()) {
            copy.addBodyHeader(key, e.getBodyHeader(key));
        }
        for (String key : e.getHttpHeaderKeys()) {
            copy.addHttpHeader(key, e.getHttpHeader(key));
        }
        copy.setStackTrace(e.getStackTrace());
        for (Throwable suppressed : e.getSuppressed()) {
            if (suppressed instanceof ElasticsearchException ese) {
                copy.addSuppressed(detach(ese, Level.DEBUG));
            }
        }
        return copy;
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
     * <p>
     * An {@link IllegalArgumentException} or I/O failure whose message is withheld (see {@link #forwardableDetail}) is
     * logged as one WARN line naming the failure, at most once a minute per node: this node's log is the only place
     * it survives. One whose message is forwarded is logged at DEBUG.
     */
    public static RuntimeException classify(Throwable t) {
        if (t instanceof Error error) {
            throw error;
        }
        if (ExceptionsHelper.unwrap(t, TaskCancelledException.class) instanceof TaskCancelledException cancelled) {
            return detach(cancelled);
        }
        if (t instanceof ExternalException ee) {
            return detach(ee);
        }
        if (t instanceof ElasticsearchException ese) {
            return detach(ese);
        }
        if (controlFlowFailure(t) instanceof ElasticsearchException control) {
            return detach(control);
        }
        if (t instanceof EsRejectedExecutionException rejected) {
            // A thread pool refusing the task (e.g. the node shutting down) is client-actionable backpressure,
            // not a server fault. The type already maps to 429 (TOO_MANY_REQUESTS) via ExceptionsHelper.status,
            // so it is returned (detached from any cause) rather than falling through to the 500 below.
            return detach(rejected);
        }
        if (t instanceof IllegalArgumentException iae) {
            // A format reader's IAE may embed a storage URI or library text. Log it here and never chain it, so its
            // message cannot surface in caused_by; forward the message only when forwardableDetail allows it.
            logClientFailure(Level.WARN, iae, forwardableDetail(iae) == null);
            return new ExternalClientException("Malformed external data: {}", safeDetail(iae));
        }
        if (t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t)) {
            // Storage clients embed full URIs in their messages and causes: log on this node, do not chain.
            logClientFailure(Level.WARN, t, forwardableDetail(t) == null);
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
     *     the worker raised checked or unchecked I/O. The failure is not chained; its message is kept only if
     *     {@link #forwardableDetail} allows it. It is logged at DEBUG (lenient error modes surface one per
     *     malformed row), or at WARN at most once a minute when its message is withheld.</li>
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
            logClientFailure(Level.WARN, failure, forwardableDetail(failure) == null);
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
            return detach(control);
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

    /** Like {@link #detail} but falls back to the class name when the message may not be forwarded. */
    public static String safeDetail(Throwable failure) {
        String forwardable = forwardableDetail(failure);
        return forwardable != null ? forwardable : rootCause(failure).getClass().getSimpleName();
    }

    /**
     * Logs a client failure for the admin, location included. A withheld message survives only here, so it reaches
     * WARN as one line, at most once a minute per node ({@link #WITHHELD_MESSAGE_WARN}): a corrupt file fails every
     * split that reads it, and the stack trace says nothing the message does not. A forwarded one reaches the caller
     * whole, so DEBUG is enough.
     */
    public static void logClientFailure(Throwable t) {
        logClientFailure(Level.WARN, t, forwardableDetail(t) == null);
    }

    private static void logClientFailure(Level level, Throwable t, boolean withheld) {
        if (level == Level.WARN && withheld && WITHHELD_MESSAGE_WARN.tryAcquire()) {
            logger.warn("External read failed with a client error (not forwarded): {}", t.toString());
        }
        logger.debug("External read failed with a client error (cause logged, not forwarded)", t);
    }

    /**
     * {@link #detail} of the first exception in {@code failure}'s chain that carries a message someone wrote, rather
     * than one a wrapper derived from {@link Throwable#toString()}. When a storage client composed that exception (see
     * {@link #composedByStorageClient}) its class name stands in for the message. It may still name a location:
     * callers that forward it check {@link #safeForUserMessage}, or use {@link #forwardableDetail}.
     */
    public static String rootDetail(Throwable failure) {
        Throwable root = rootCause(failure);
        return composedByStorageClient(root) ? root.getClass().getSimpleName() : detail(root);
    }

    /**
     * The message of the first exception in {@code failure}'s chain that someone wrote (see {@link #rootCause}), when it
     * may be shown to whoever runs the query: no storage client composed it (see {@link #composedByStorageClient}), it
     * names no location (see {@link #safeForUserMessage}) or endpoint host, and it does not contain a storage client's
     * own message from further down the chain. {@code null} otherwise.
     * <p>
     * The last check catches Elasticsearch code that pastes a client's message into its own
     * ({@code new IOException("listing failed: " + sdk.getMessage(), sdk)}): {@link #rootCause} stops at that wrapper
     * because the message is not {@code cause.toString()}, so {@link #composedByStorageClient} would otherwise see only
     * the wrapper.
     */
    @Nullable
    public static String forwardableDetail(Throwable failure) {
        Throwable root = rootCause(failure);
        String message = root.getMessage();
        if (message == null || composedByStorageClient(root) || safeForUserMessage(message) == false || namesNoHost(failure) == false) {
            return null;
        }
        Throwable current = root;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH; depth++) {
            Throwable cause = current.getCause();
            if (cause == null || cause == current) {
                break;
            }
            if (composedByStorageClient(cause)) {
                String causeMessage = cause.getMessage();
                if (causeMessage != null && causeMessage.isEmpty() == false && message.contains(causeMessage)) {
                    return null;
                }
            }
            current = cause;
        }
        return message;
    }

    /**
     * Packages of the storage and catalog clients the data sources talk to. Their exceptions relay what the remote
     * answered, and a remote's refusal names the identity it was refused: an S3 or KMS denial carries the principal's
     * and the resource's ARNs, a GCS or Azure one the service account or tenant.
     * <p>
     * A client missing here has its text forwarded. A data source that adds a storage or catalog client must add its
     * packages, and pin them with a {@code testClientExceptionsAreStorageClientText} in its module, as the S3, GCS,
     * Azure and Flight modules do.
     */
    private static final List<String> STORAGE_CLIENT_PACKAGES = List.of(
        "software.amazon.awssdk.",
        "com.google.cloud.",
        "com.google.auth.",
        "com.google.api.",
        "com.azure.",
        "com.microsoft.aad.msal4j.",
        "com.nimbusds.",
        "org.apache.arrow.flight.",
        "io.grpc.",
        "org.apache.iceberg.rest.",
        "org.apache.iceberg.aws.",
        "org.apache.iceberg.gcp.",
        "org.apache.iceberg.azure."
    );

    /**
     * Whether a storage client composed {@code t}'s own message: its class, or the frame that constructed it, is in one
     * of {@link #STORAGE_CLIENT_PACKAGES}. The frame catches a JDK exception a client built (an {@code IOException}
     * carrying the response). Elasticsearch code that pastes a client's message into its own is caught by
     * {@link #forwardableDetail}, which withholds a wrapper message that contains a storage-client cause's text.
     */
    public static boolean composedByStorageClient(Throwable t) {
        if (isStorageClientClass(t.getClass().getName())) {
            return true;
        }
        StackTraceElement[] trace = t.getStackTrace();
        return trace.length > 0 && isStorageClientClass(trace[0].getClassName());
    }

    private static boolean isStorageClientClass(String className) {
        for (String prefix : STORAGE_CLIENT_PACKAGES) {
            if (className.startsWith(prefix)) {
                return true;
            }
        }
        return false;
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
            if (t instanceof IllegalArgumentException && t.getMessage() != null && namesNoHost(t) && composedByStorageClient(t) == false) {
                String redacted = URI_IN_MESSAGE.matcher(t.getMessage()).replaceAll("<location>");
                if (safeForUserMessage(redacted)) {
                    reason = redacted;
                }
            }
        }
        return reason;
    }

    /**
     * Why a resolve-time failure happened, for the message that replaces its dropped cause chain: the message of the first I/O
     * failure in the chain when it names no storage location (the policy {@link #classify} applies at read time, e.g.
     * {@code read denied: credential mismatch}), else a validator's {@link #validationReason}. Returns
     * {@code null} when neither is available.
     */
    public static String failureReason(Throwable failure) {
        Throwable t = failure;
        for (int depth = 0; t != null && depth < 12; depth++, t = t.getCause() == t ? null : t.getCause()) {
            if ((t instanceof IOException || t instanceof UncheckedIOException) && t.getMessage() != null) {
                String forwardable = forwardableDetail(t);
                if (forwardable != null) {
                    return forwardable;
                }
                break;
            }
        }
        return validationReason(failure);
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
