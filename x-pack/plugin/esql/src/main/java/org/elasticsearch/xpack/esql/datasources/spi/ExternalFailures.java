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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;

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
 *     <li>An {@link ElasticsearchException} already carries its own status and keeps it, but is returned
 *     {@link #withoutCause without its cause chain}: this covers the {@link ExternalException} family
 *     (400/500/503) raised at the reader/storage boundary, as well as {@code CircuitBreakingException} (429)
 *     and {@code TaskCancelledException} (400).</li>
 *     <li>An {@link EsRejectedExecutionException} — a thread pool refusing the task (e.g. the node shutting
 *     down) — is client-actionable backpressure, not a server fault. It already maps to 429 (TOO_MANY_REQUESTS)
 *     via {@code ExceptionsHelper.status}, so it keeps its type (without its cause chain) rather than being
 *     mistaken for a broken invariant and reported as 500.</li>
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
 * A {@code TaskCancelledException} anywhere in the chain is returned as the cancellation (400), since the result
 * carries no chain for callers to find it in. A read interrupted while blocking surfaces as an
 * {@link IOException} subclass (so, 400). A bare {@link InterruptedException} would fall through to 500;
 * the interrupt flag is intentionally left untouched, since this runs on the thread surfacing the stored
 * failure, not the worker thread that was interrupted.
 */
public final class ExternalFailures {

    private static final Logger logger = LogManager.getLogger(ExternalFailures.class);

    /**
     * Bounds the WARN lines {@link #logReadFailure} writes for cause-less failures whose message {@link #classify}
     * withholds. Shared by every dataset and user on the node, so one noisy dataset can push another's first such
     * failure down to DEBUG for the interval.
     */
    public static final LogThrottle WITHHELD_MESSAGE_WARN = new LogThrottle(TimeValue.timeValueMinutes(1));

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
     * Covers object-store schemes (S3, GCS, Azure Blob) and generic HTTP/HTTPS endpoints.
     * Flight/gRPC ({@code esql-datasource-grpc}) uses non-HTTP schemes ({@code grpc://},
     * {@code grpcs://}) and is hardened separately; add those schemes here when that module
     * migrates to structured exceptions.
     */
    private static final String[] STORAGE_URI_SCHEMES = {
        "s3://",
        "s3a://",
        "s3n://",
        "gs://",
        "wasb://",
        "wasbs://",
        "http://",
        "https://" };

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
     * Returns {@code true} when {@code message} contains no known storage-URI scheme. Callers
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
     * @param cause the parse exception that triggered the row failure; {@code null} is accepted.
     *     Verified by assertion via {@link #noStoragePathLeaked} (which walks the full cause chain).
     * @param safeMessage a caller-controlled message verified to be free of storage-URI schemes
     */
    public static ExternalClientException rowError(Throwable cause, String safeMessage) {
        Exception e = cause instanceof Exception ex ? ex : null;
        ExternalClientException result = new ExternalClientException(e, "{}", safeMessage);
        assert containsStoragePath(safeMessage) == false : "storage path in row error message: " + safeMessage;
        assert noStoragePathLeaked(result) : "storage path leaked via row error cause chain: " + result.getMessage();
        return result;
    }

    /**
     * Returns the {@link RuntimeException} to throw for the given read failure. May instead throw if
     * {@code t} is an {@link Error}, which must propagate unchanged.
     * <p>
     * The result carries no cause chain (see {@link #withoutCause}). A {@link TaskCancelledException} anywhere in
     * {@code t}'s chain is returned as the cancellation itself, so the query still reports it as one.
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
        // Dropping the chain would hide a cancellation from FailureCollector, which finds it by walking the chain.
        if (ExceptionsHelper.unwrap(t, TaskCancelledException.class) instanceof TaskCancelledException cancelled) {
            return withoutCause(cancelled);
        }
        if (t instanceof ElasticsearchException ese) {
            assert noStoragePathLeaked(ese) : "storage path leaked in ExternalException: " + ese.getMessage();
            return withoutCause(ese);
        }
        if (t instanceof EsRejectedExecutionException rejected) {
            return withoutCause(rejected);
        }
        if (t instanceof IllegalArgumentException iae) {
            // IAE from format readers may embed storage URIs in the message. Log at WARN on this node for
            // debugging; do not chain it into the exception so its message never crosses the wire.
            logger.warn("External read failed with IllegalArgumentException (cause logged, not forwarded)", iae);
            ExternalClientException iaeResult = new ExternalClientException("Malformed external data ({})", iae.getClass().getSimpleName());
            // A Parquet reader may surface a column name or file basename that is useful for diagnosis without
            // leaking the full object path; forwardableDetail keeps only such text.
            String iaeDetail = forwardableDetail(iae);
            if (iaeDetail != null) {
                iaeResult.setDetail(iaeDetail);
            }
            return iaeResult;
        }
        RuntimeException result;
        if (t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t)) {
            result = new ExternalClientException("Failed to read external source: {}", rootDetail(t));
        } else {
            result = new ExternalServerException("Unexpected failure reading external source: {}", rootDetail(t));
        }
        result.setStackTrace(t.getStackTrace());
        assert noStoragePathLeaked(result) : "storage path leaked in classified exception: " + result.getMessage();
        return result;
    }

    /**
     * Logs a read failure at an operator's read boundary, before {@link #classify} drops its cause chain from what the
     * caller sees. The response keeps only text Elasticsearch composed, so when {@code t} carries a cause or suppressed
     * exceptions this log is where an operator finds them. Call it once per surfaced failure, not from status
     * reporting, which reclassifies on every poll.
     * <p>
     * Logged at {@code WARN} when the response loses something or the failure is a server-side one. A cause-less client
     * failure (missing object, malformed file, breaker trip) whose message Elasticsearch composed reaches the caller
     * whole and is logged at {@code DEBUG}, as are cancellations: neither is something an operator has to act on. When
     * the response withholds the message instead (a JDK or library exception, such as the inflater's
     * {@code EOFException} for a truncated gzip file, shows only its class name) this log is the only record of it, so
     * it reaches {@code WARN}, at most once a minute per node ({@link #WITHHELD_MESSAGE_WARN}). Storage layers that
     * drop a provider cause themselves log it where they drop it.
     */
    public static void logReadFailure(Throwable t) {
        if (t instanceof Error) {
            return;
        }
        if (ExceptionsHelper.unwrap(t, TaskCancelledException.class) != null) {
            logger.debug("External source read cancelled", t);
            return;
        }
        boolean dropsSomething = t.getCause() != null || t.getSuppressed().length > 0;
        // classify reports these as client failures whatever ExceptionsHelper.status says about the raw type.
        boolean clientRead = t instanceof IOException || t instanceof UncheckedIOException || isMalformedDataException(t);
        if (dropsSomething || (clientRead == false && ExceptionsHelper.status(t).getStatus() >= 500)) {
            logger.warn("External source read failed", t);
        } else if (rootCause(t).getMessage() != null && forwardableDetail(t) == null && WITHHELD_MESSAGE_WARN.tryAcquire()) {
            logger.warn("External source read failed; its message is withheld from the response", t);
        } else {
            logger.debug("External source read failed", t);
        }
    }

    /**
     * {@code e} without its cause chain and suppressed exceptions, keeping its type, status, message and the state
     * callers act on (retry hints, breaker byte counts, executor-shutdown flag, dataset context). Returns {@code e}
     * itself when there is nothing to drop.
     * <p>
     * Every level of a failure's cause chain is rendered into the error response under {@code caused_by}, and the
     * deepest levels are often a storage SDK's own exception, carrying text the provider wrote: for an IAM denial,
     * the principal and KMS key ARNs Elasticsearch authenticated with. The boundaries where an external-source
     * failure becomes a user-facing one ({@link #classify}, the resolver, split discovery) pass their result through
     * here, so only text Elasticsearch composed reaches the caller whichever throw site raised it. Callers log the
     * original first: this is the one place the provider's reason survives.
     * <p>
     * A foreign {@link ElasticsearchException} that carries a cause is rebuilt with the same status and message:
     * as {@link ExternalClientException} for 400, {@link ExternalServerException} for 500, and
     * {@link ElasticsearchStatusException} otherwise. Any other {@link RuntimeException} is returned unchanged; the
     * boundaries type those before they get here.
     */
    public static RuntimeException withoutCause(RuntimeException e) {
        if (e.getCause() == null && e.getSuppressed().length == 0) {
            return e;
        }
        RuntimeException copy;
        if (e instanceof ExternalException ee) {
            return ee.withoutCause();
        } else if (e instanceof CircuitBreakingException cbe) {
            copy = new CircuitBreakingException(cbe.getMessage(), cbe.getBytesWanted(), cbe.getByteLimit(), cbe.getDurability());
        } else if (e instanceof TaskCancelledException) {
            copy = new TaskCancelledException(e.getMessage());
        } else if (e instanceof EsRejectedExecutionException rejected) {
            copy = new EsRejectedExecutionException(rejected.getMessage(), rejected.isExecutorShutdown());
        } else if (e instanceof ElasticsearchException ese) {
            RestStatus status = ese.status();
            if (status == RestStatus.BAD_REQUEST) {
                copy = new ExternalClientException("{}", ese.getMessage());
            } else if (status == RestStatus.INTERNAL_SERVER_ERROR) {
                copy = new ExternalServerException("{}", ese.getMessage());
            } else {
                copy = new ElasticsearchStatusException("{}", status, ese.getMessage());
            }
        } else {
            return e;
        }
        copy.setStackTrace(e.getStackTrace());
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
        RuntimeException result;
        if (failure instanceof IOException || failure instanceof UncheckedIOException) {
            result = new ExternalClientException(failure, "{}: {}", fallbackMessage, rootDetail(failure));
        } else if (failure instanceof RuntimeException re) {
            return re;
        } else {
            result = new ExternalServerException(failure, "{}: {}", fallbackMessage, rootDetail(failure));
        }
        assert noStoragePathLeaked(result) : "storage path leaked in surfaced exception: " + result.getMessage();
        return result;
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
     * The message for a wrapper that types a metadata-resolution failure as client-caused — {@code FileSourceFactory},
     * {@code TableCatalog}. Such a wrapper exists to fix the HTTP status, not to say anything new, so it keeps the
     * cause's own diagnosis: "Object not found: &lt;path&gt;", "CSV file has no schema line", "Could not read
     * [&lt;path&gt;] as a Parquet file: ...". A wrapper that replaces the diagnosis with a constant naming only the
     * path reports every distinct condition — a missing object, a wrong format, a truncated footer, an empty file —
     * with one identical sentence, which is what makes an external-source failure unactionable.
     * <p>
     * The location is prepended only when the cause does not already name it. Storage and reader messages usually do
     * (they are built from the path), and this method is reached through
     * {@code ExternalSourceResolver#mapResolveFailure}, which passes a client-caused failure straight to the user
     * without adding context of its own — so the location has to be here when the cause omits it, and must not be
     * here twice when the cause includes it.
     */
    public static String resolutionFailureMessage(String location, Throwable cause) {
        return locate("Failed to resolve metadata for", location, detail(cause));
    }

    /**
     * Applies the same rule for any wrapper prefix: a detail that already names the location is returned as-is,
     * so the path is not printed twice. Callers that have already resolved their own detail string use this
     * directly rather than re-deriving it from the cause.
     */
    public static String locate(String prefix, String location, @Nullable String detail) {
        String shown = redactHttpUrl(location);
        if (detail == null) {
            // A message-less throwable reaches here from the arms that pass getMessage() straight in --
            // EsRejectedExecutionException has a no-argument constructor. Name the location and stop, rather
            // than appending the word "null".
            return prefix + " [" + shown + "]";
        }
        // Redact every occurrence of the raw location before deciding: a pre-signed URL's redacted form is a prefix of
        // the raw one, so a detail naming the raw URL also "contains" the redacted form and would pass the signature
        // through. A detail built from the redacted form (the HTTP store's own messages) already names the location.
        String safeDetail = detail.replace(location, shown);
        return safeDetail.contains(shown) ? safeDetail : prefix + " [" + shown + "]: " + safeDetail;
    }

    /**
     * {@link #detail} of the first exception in {@code failure}'s chain that carries a message someone wrote, rather
     * than one a wrapper derived from {@link Throwable#toString()}. When that exception was not built by Elasticsearch
     * code (see {@link #composedByElasticsearch}) its class name stands in for the message.
     */
    public static String rootDetail(Throwable failure) {
        Throwable root = rootCause(failure);
        return composedByElasticsearch(root) ? detail(root) : root.getClass().getSimpleName();
    }

    /**
     * The message of the first exception in {@code failure}'s chain that someone wrote (see {@link #rootCause}), when it
     * may be shown to whoever runs the query: Elasticsearch composed it (see {@link #composedByElasticsearch}) and it
     * names no storage location. {@code null} otherwise. Every boundary that forwards a failure's own text goes through
     * this or {@link #rootDetail}, so the rule is the same whichever route the failure took.
     */
    @Nullable
    public static String forwardableDetail(Throwable failure) {
        Throwable root = rootCause(failure);
        String message = root.getMessage();
        return message != null && composedByElasticsearch(root) && safeForUserMessage(message) ? message : null;
    }

    /**
     * Whether {@code t}'s own message is text Elasticsearch composed, and so may be shown to whoever runs the query.
     * A throwable records where it was constructed as its top stack frame: one built by a storage SDK, a format
     * library or the JDK may carry a sentence the remote wrote (a denied read names the principal and resource ARNs),
     * and is recognised here without a list of what such sentences look like. A throwable with no stack trace is not
     * trusted.
     */
    public static boolean composedByElasticsearch(Throwable t) {
        StackTraceElement[] trace = t.getStackTrace();
        return trace.length > 0 && trace[0].getClassName().startsWith("org.elasticsearch.");
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
