/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Classifies a failure raised while reading an external data source into the exception an external-read
 * operator should surface, so that it maps to the right HTTP status. The
 * companion {@link #surface} helper is used at the worker rethrow sites inside parallel coordinators and
 * page iterators to pre-type the failure: it turns a raw {@link IOException} into an already-classified
 * {@link ExternalClientException} (400), without chaining it, so the read boundary's {@link #classify} sees a
 * status-typed exception. {@code surface} cannot rescue a status signal already buried under a status-neutral
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
 *     <li>An {@link ExternalException} (400/500/503) raised at the reader/storage boundary keeps its type and
 *     status but is returned as a {@link #detach detached copy}: storage-client causes name the bucket and key,
 *     and the instance may be shared (e.g. by a cache's concurrent waiters) while callers annotate the result.</li>
 *     <li>Any other {@link ElasticsearchException} already carries its own status and keeps it: this covers
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
 *     so it surfaces as an {@link ExternalServerException} (500) to keep the bug visible. It is not chained:
 *     unchecked storage-client failures (e.g. AWS {@code SdkClientException}) land here too.</li>
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

    /**
     * Bounds the WARN lines for access-denied reads, whose reason (the provider's refusal, naming the principal it
     * read as) survives only in this log. Shared across datasets like {@link #WITHHELD_MESSAGE_WARN}.
     */
    static final LogThrottle ACCESS_DENIED_WARN = new LogThrottle(TimeValue.timeValueMinutes(1));

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
     * <p>
     * Covers object-store schemes (S3, GCS, Azure Blob), generic HTTP/HTTPS endpoints, Arrow Flight / gRPC
     * endpoints and local files. {@code file:/} also matches the authority-less {@code file:/path} form.
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

    /**
     * Returns {@code true} when no message in {@code e}'s full cause chain, or among its suppressed failures, names a
     * location (see {@link #safeForUserMessage}). A {@code false} result means a location leaked into a
     * user-facing exception message.
     */
    static boolean noStoragePathLeaked(RuntimeException e) {
        return chainNamesNoLocation(e);
    }

    /**
     * Walks {@code start} and its cause chain, checking each message and its suppressed failures for a location.
     * {@code null} is accepted and trivially passes: used by {@link #rowError}, where only the cause (not the
     * caller-supplied top message, which is row data) must be checked.
     */
    private static boolean chainNamesNoLocation(Throwable start) {
        Throwable current = start;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (containsStoragePath(current.getMessage())) {
                return false;
            }
            // suppressed[] is serialized into the response via innerToXContent; guard it too.
            for (Throwable suppressed : current.getSuppressed()) {
                if (containsStoragePath(suppressed.getMessage())) {
                    return false;
                }
            }
            Throwable cause = current.getCause();
            if (cause == null || cause == current) {
                break;
            }
            current = cause;
        }
        return true;
    }

    /**
     * A bare filesystem location carries no scheme: an absolute POSIX path ({@code /data}, {@code /data/x.csv}) or a
     * Windows drive path ({@code C:\data}), at the start of the message or after a delimiter.
     */
    private static final Pattern ABSOLUTE_FILESYSTEM_PATH = Pattern.compile(
        "(?:^|[\\s\\[(<'\"=,])/[^\\s/\\[\\]()<>'\",]+|(?:^|[\\s\\[(<'\"=,])[A-Za-z]:[\\\\/]"
    );

    private static boolean containsStoragePath(String msg) {
        if (msg == null) {
            return false;
        }
        for (String scheme : STORAGE_URI_SCHEMES) {
            if (msg.contains(scheme)) {
                return true;
            }
        }
        for (String domain : STORAGE_HOST_DOMAINS) {
            if (msg.contains(domain)) {
                return true;
            }
        }
        return ABSOLUTE_FILESYSTEM_PATH.matcher(msg).find();
    }

    /**
     * Returns {@code true} when {@code message} contains no known storage-URI scheme, cloud storage host or absolute
     * filesystem path. Hosts of custom endpoints and relative paths are not recognised. Callers
     * outside the {@code spi} package use this to decide whether a diagnostic message from a
     * third-party library is safe to forward to the user.
     */
    public static boolean safeForUserMessage(String message) {
        return containsStoragePath(message) == false;
    }

    /**
     * Wraps a row-level parse failure in an {@link ExternalClientException} using the
     * caller-supplied {@code safeMessage} verbatim as the exception message. Use this when the
     * message is already self-descriptive (e.g. {@code "Row [N] of [file.csv]: ..."}) and the
     * structured {@link ExternalException.Condition#MALFORMED_DATA} prefix would be redundant.
     *
     * @param cause the parse exception that triggered the row failure; {@code null} is accepted. Its chain is
     *     verified by assertion (see {@link #chainNamesNoLocation}); {@code safeMessage} is not, since it is
     *     caller-supplied row content, not a location, and can legitimately look like one (e.g. a row holding a URL
     *     or an absolute path). Asserting on it would make a malformed-row failure throw an {@link AssertionError}
     *     under {@code -ea} instead of returning its intended HTTP 400.
     * @param safeMessage a caller-controlled message naming only the object (never the full path), plus row content
     */
    public static ExternalClientException rowError(Throwable cause, String safeMessage) {
        Exception e = cause instanceof Exception ex ? ex : null;
        ExternalClientException result = new ExternalClientException(e, "{}", safeMessage);
        assert chainNamesNoLocation(cause) : "storage path leaked via row error cause chain: " + result.getMessage();
        return result;
    }

    /**
     * Returns the {@link RuntimeException} to throw for the given read failure. May instead throw if
     * {@code t} is an {@link Error}, which must propagate unchanged.
     * <p>
     * Under {@code -ea} (assertions enabled), verifies that no message in the result's full cause chain
     * contains a known storage-URI scheme — a debug guard that fires immediately if a new throw site
     * embeds a full path instead of using the structured constructors on {@link ExternalException}.
     * See {@link #noStoragePathLeaked}.
     * <p>
     * An {@link IllegalArgumentException} or I/O failure whose message is withheld (see {@link #forwardableDetail}) is
     * logged as one WARN line naming the failure, at most once a minute per node: this node's log is the only place
     * it survives. One whose message is forwarded is logged at DEBUG. A typed failure's dropped cause is logged at WARN
     * when it is server-side or an access denial, else at DEBUG, since the condition already says what is wrong.
     */
    public static RuntimeException classify(Throwable t) {
        return classify(t, Level.WARN);
    }

    /**
     * {@link #classify} for a failure that only gets attached as suppressed to one already classified for the
     * same read. Logs client failures and typed server failures at DEBUG, since the first failure was already logged at
     * WARN and parallel readers often fail the same way (e.g. all throttled). An untyped failure is still logged at
     * WARN: it is a bug, not a condition.
     */
    public static RuntimeException classifySuppressed(Throwable t) {
        return classify(t, Level.DEBUG);
    }

    private static RuntimeException classify(Throwable t, Level failureLevel) {
        if (t instanceof Error error) {
            throw error;
        }
        if (ExceptionsHelper.unwrap(t, TaskCancelledException.class) instanceof TaskCancelledException cancelled) {
            return detach(cancelled, failureLevel);
        }
        if (t instanceof ExternalException ee) {
            ExternalException detached = detach(ee, failureLevel);
            assert noStoragePathLeaked(detached) : "storage path leaked in ExternalException: " + detached.getMessage();
            return detached;
        }
        if (t instanceof ElasticsearchException ese) {
            ElasticsearchException detached = detach(ese, failureLevel);
            assert noStoragePathLeaked(detached) : "storage path leaked in ElasticsearchException: " + detached.getMessage();
            return detached;
        }
        if (t instanceof EsRejectedExecutionException rejected) {
            return detach(rejected);
        }
        if (t instanceof IllegalArgumentException iae) {
            // IAE from format readers may embed storage URIs or library text in the message. Log on this node for
            // debugging; do not chain it into the exception so its message never crosses the wire.
            String forwardable = forwardableDetail(iae);
            logClientFailure(failureLevel, iae, forwardable == null);
            ExternalClientException iaeResult = new ExternalClientException("Malformed external data ({})", iae.getClass().getSimpleName());
            // A Parquet reader may surface a column name or file basename that is useful for diagnosis without
            // leaking the full object path.
            if (forwardable != null) {
                iaeResult.setDetail(forwardable);
            }
            return iaeResult;
        }
        RuntimeException result;
        if (t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t)) {
            // IOException messages from storage clients may embed full storage URIs. Log on this node
            // for debugging; do not chain t into the exception so its message and cause chain never
            // cross the wire.
            String forwardable = forwardableDetail(t);
            logClientFailure(failureLevel, t, forwardable == null);
            ExternalClientException ioResult = new ExternalClientException(
                "Failed to read external source: {}",
                t.getClass().getSimpleName()
            );
            if (forwardable != null) {
                ioResult.setDetail(forwardable);
            }
            result = ioResult;
        } else {
            // Unchecked SDK failures (e.g. AWS SdkClientException) land here and may name the bucket host.
            logger.warn("Unexpected failure reading external source (cause logged, not forwarded)", t);
            result = new ExternalServerException("Unexpected failure reading external source: {}", safeDetail(t));
        }
        assert noStoragePathLeaked(result) : "storage path leaked in classified exception: " + result.getMessage();
        return result;
    }

    /**
     * Returns a fresh copy of {@code e} {@link ExternalException#withoutCause() detached} from its cause chain,
     * after logging the chain on this node. Always a new instance, so the caller may annotate it even when
     * {@code e} is shared (e.g. rethrown to every waiter of a cache load). Suppressed failures that are themselves
     * external failures are detached and kept; any other suppressed failure is dropped.
     */
    public static ExternalException detach(ExternalException e) {
        return detach(e, Level.WARN);
    }

    /**
     * @param serverFailureLevel the level a server-side failure's cause is logged at: it is its only diagnosis. A client
     *     failure's condition already says what is wrong, so its cause is logged at DEBUG, except for an access denial:
     *     the provider's reason for refusing is its only diagnosis too, so it reaches WARN at most once a minute.
     */
    private static ExternalException detach(ExternalException e, Level serverFailureLevel) {
        if (e.getCause() != null) {
            Level level;
            if (e.status().getStatus() >= 500) {
                level = serverFailureLevel;
            } else if (serverFailureLevel == Level.WARN
                && e.condition() == ExternalException.Condition.ACCESS_DENIED
                && ACCESS_DENIED_WARN.tryAcquire()) {
                    level = Level.WARN;
                } else {
                    level = Level.DEBUG;
                }
            logger.log(level, () -> "External failure detached from its cause (cause logged, not forwarded)", e);
        }
        ExternalException detached = e.withoutCause();
        for (Throwable suppressed : e.getSuppressed()) {
            if (suppressed instanceof ExternalException external) {
                detached.addSuppressed(detach(external, Level.DEBUG));
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
            return detach(ee, serverFailureLevel);
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
     *     their own status, and any other unchecked cause raised by the worker. It is <em>not</em> detached
     *     from its cause, so a caller not followed by {@link #classify} must {@link #detach} it itself. <strong>Note:</strong> a
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
     *     no evidence it is the caller's fault, so we keep the bug visible. Logged at WARN, not chained.</li>
     * </ul>
     *
     * @param failure the raw stored worker-side throwable; <em>not</em> a status-neutral wrapper around it
     * @param fallbackMessage non-null context prefix included in every wrapped result
     */
    public static RuntimeException surface(Throwable failure, String fallbackMessage) {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException re && failure instanceof UncheckedIOException == false) {
            return re;
        }
        // Storage clients embed full URIs in their messages and causes, so the failure is logged here and
        // never chained; its message is forwarded only when forwardableDetail allows it.
        String detail = safeDetail(failure);
        ElasticsearchException result;
        if (failure instanceof IOException || failure instanceof UncheckedIOException) {
            logClientFailure(Level.WARN, failure, forwardableDetail(failure) == null);
            result = new ExternalClientException("{}: {}", fallbackMessage, detail);
        } else {
            logger.warn("External read failed (cause logged, not forwarded)", failure);
            result = new ExternalServerException("{}: {}", fallbackMessage, detail);
        }
        assert noStoragePathLeaked(result) : "storage path leaked in surfaced exception: " + result.getMessage();
        return result;
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
     * The deepest cause of {@code e}, for a WARN line that reports {@code detail} to the admin, when it says something
     * {@code detail} does not (an access denial's reason, which the response never carries) and the shared
     * {@link #WITHHELD_MESSAGE_WARN} allows one more such line this interval. {@code null} otherwise: anyone who can
     * query a failing dataset can repeat the failure, and the remote's sentence must not reach WARN once per query.
     */
    @Nullable
    public static Throwable withheldCauseToLog(@Nullable String detail, Throwable e) {
        Throwable deepest = e;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH && deepest.getCause() != null && deepest.getCause() != deepest; depth++) {
            deepest = deepest.getCause();
        }
        if (deepest == e || detail != null && detail.contains(String.valueOf(deepest.getMessage()))) {
            return null;
        }
        return WITHHELD_MESSAGE_WARN.tryAcquire() ? deepest : null;
    }

    private static String safeDetail(Throwable failure) {
        String forwardable = forwardableDetail(failure);
        return forwardable != null ? forwardable : rootCause(failure).getClass().getSimpleName();
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
     * than one a wrapper derived from {@link Throwable#toString()}. When a storage client composed that exception (see
     * {@link #composedByStorageClient}) its class name stands in for the message, so a site that copies this into its
     * own exception's message cannot launder the remote's text into one Elasticsearch appears to have written.
     * It may still name a location: callers that forward it check {@link #safeForUserMessage}, or use
     * {@link #forwardableDetail}.
     */
    public static String rootDetail(Throwable failure) {
        Throwable root = rootCause(failure);
        return composedByStorageClient(root) ? root.getClass().getSimpleName() : detail(root);
    }

    /**
     * The message of the first exception in {@code failure}'s chain that someone wrote (see {@link #rootCause}), when it
     * may be shown to whoever runs the query: no storage client composed it (see {@link #composedByStorageClient}), it
     * names no location (see {@link #safeForUserMessage}), and it does not contain a storage client's own message from
     * further down the chain. {@code null} otherwise.
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
        if (message == null || composedByStorageClient(root) || safeForUserMessage(message) == false) {
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
