/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class ExternalFailuresTests extends ESTestCase {

    public void testErrorIsRethrown() {
        AssertionError error = new AssertionError("boom");
        AssertionError thrown = expectThrows(AssertionError.class, () -> ExternalFailures.classify(error));
        assertSame(error, thrown);
    }

    public void testExternalExceptionsKeepTheirTypeAndStatusButNotTheirCause() {
        var client = new ExternalClientException("bad file", new IOException("truncated"));
        RuntimeException classifiedClient = ExternalFailures.classify(client);
        assertThat(classifiedClient, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertEquals("bad file", classifiedClient.getMessage());
        assertNull(classifiedClient.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classifiedClient));

        var server = new ExternalServerException("invariant violated", new IllegalStateException());
        RuntimeException classifiedServer = ExternalFailures.classify(server);
        assertThat(classifiedServer, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertEquals("invariant violated", classifiedServer.getMessage());
        assertNull(classifiedServer.getCause());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classifiedServer));

        var unavailable = new ExternalUnavailableException("store 503", new IOException());
        RuntimeException classifiedUnavailable = ExternalFailures.classify(unavailable);
        assertThat(classifiedUnavailable, org.hamcrest.Matchers.instanceOf(ExternalUnavailableException.class));
        assertNull(classifiedUnavailable.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(classifiedUnavailable));
    }

    public void testCircuitBreakingAndCancellationKeepTheirStatus() {
        var breaking = new CircuitBreakingException("over", 10, 5, CircuitBreaker.Durability.TRANSIENT);
        assertSame(breaking, ExternalFailures.classify(breaking));
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(ExternalFailures.classify(breaking)));

        var cancelled = new TaskCancelledException("cancelled");
        assertSame(cancelled, ExternalFailures.classify(cancelled));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(cancelled)));
    }

    /**
     * Real breaker messages end with a reference-docs link ({@code https://www.elastic.co/docs/...}). That link must not
     * be mistaken for a leaked storage URI: the leak assertion in {@code classify} is fatal to the node under {@code -ea}.
     */
    public void testCircuitBreakingWithReferenceDocsLinkIsNotALeak() {
        var breaking = new CircuitBreakingException(
            "[request] Data too large, data for [<esql_block_factory>] would be [181195099/172.8mb], which is larger than "
                + "the limit of [181193932/172.7mb]; for more information, see "
                + ReferenceDocs.CIRCUIT_BREAKER_ERRORS,
            181195099,
            181193932,
            CircuitBreaker.Durability.TRANSIENT
        );
        assertSame(breaking, ExternalFailures.classify(breaking));
        assertSame(breaking, ExternalFailures.classifySuppressed(breaking));
    }

    /**
     * A breaker that carries a cause goes through the detached-copy path of {@code classify}: the copy keeps the message
     * (with its docs link), drops the cause, and must not trip the leak assertion either.
     */
    public void testCircuitBreakingWithCauseAndReferenceDocsLinkIsDetachedNotALeak() {
        var breaking = new CircuitBreakingException(
            "[request] Data too large; for more information, see " + ReferenceDocs.CIRCUIT_BREAKER_ERRORS,
            10,
            5,
            CircuitBreaker.Durability.TRANSIENT
        );
        breaking.initCause(new IOException("failed reading s3://secret-bucket/k"));
        RuntimeException classified = ExternalFailures.classify(breaking);
        assertThat(classified, instanceOf(CircuitBreakingException.class));
        assertNull(classified.getCause());
        assertEquals(breaking.getMessage(), classified.getMessage());
    }

    /**
     * The exemption is limited to core exceptions that are not raised at the storage boundary: any other
     * {@link ElasticsearchException} that embeds a location still trips the assertion.
     */
    public void testGenericElasticsearchExceptionStillAssertsOnALeakedPath() {
        var leaking = new ElasticsearchException("failed reading s3://secret-bucket/k");
        expectThrows(AssertionError.class, () -> ExternalFailures.classify(leaking));
    }

    public void testRejectedExecutionIsBackpressureNotServerError() {
        // A saturated thread pool (or the node shutting down) can reject work as an EsRejectedExecutionException.
        // That is load-shed backpressure (429), not a broken invariant in our reading code (500): classify must
        // return it (detached) so its self-carried 429 survives, rather than wrapping it as an ExternalServerException.
        // Storage concurrency permit exhaustion is a separate case: it is raised as a 503-class
        // ExternalUnavailableException at the concurrency-limiter boundary so the storage retry layer engages, so it
        // does not reach classify() as an EsRejectedExecutionException.
        var rejected = new EsRejectedExecutionException("rejected execution while reading external source", true);
        RuntimeException classified = ExternalFailures.classify(rejected);
        assertSame(rejected, classified);
        assertEquals(
            "permit/queue exhaustion must surface as 429 backpressure, not 500",
            RestStatus.TOO_MANY_REQUESTS,
            ExceptionsHelper.status(classified)
        );

        rejected.initCause(new RuntimeException("User: arn:aws:sts::123456789012:assumed-role/reader/session"));
        RuntimeException detached = ExternalFailures.classify(rejected);
        assertThat(detached, instanceOf(EsRejectedExecutionException.class));
        assertNotSame(rejected, detached);
        assertNull(detached.getCause());
        assertTrue(((EsRejectedExecutionException) detached).isExecutorShutdown());
        assertArrayEquals(
            "the original stack is kept so DEBUG and origin checks still see it",
            rejected.getStackTrace(),
            detached.getStackTrace()
        );
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(detached));
    }

    public void testGenericElasticsearchExceptionPassesThrough() {
        var ese = new ElasticsearchException("generic");
        assertSame(ese, ExternalFailures.classify(ese));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(ExternalFailures.classify(ese)));
    }

    public void testIllegalArgumentExceptionWrappedAs400() {
        // IAE may embed storage paths; classify() wraps it so the message is path-free and logs the original at WARN.
        var iae = new IllegalArgumentException("bad arg containing s3://bucket/prefix/file.parquet");
        RuntimeException classified = ExternalFailures.classify(iae);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertNotSame(iae, classified);
        // IAE is intentionally not chained: its message may embed storage URIs which would cross the wire via caused_by.
        assertNull(classified.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        assertThat(classified.getMessage(), org.hamcrest.Matchers.containsString("IllegalArgumentException"));
        assertThat(classified.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("s3://")));
    }

    public void testIoErrorsBecomeClientException() {
        for (Throwable io : new Throwable[] {
            new IOException("read failed"),
            new EOFException("truncated"),
            new UncheckedIOException(new IOException("wrapped")) }) {
            RuntimeException classified = ExternalFailures.classify(io);
            assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
            // IOException is not chained: its message may embed storage URIs which would leak via caused_by.
            assertNull("IOException must not be chained to prevent caused_by leaks", classified.getCause());
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
            assertThat(classified.getMessage(), org.hamcrest.Matchers.containsString(io.getClass().getSimpleName()));
        }
    }

    public void testIoMessageWithStorageUriIsStripped() {
        IOException io = new IOException("s3://my-bucket/path/file.parquet: read failed");
        RuntimeException classified = ExternalFailures.classify(io);
        assertNull("IOException with URI must not be chained", classified.getCause());
        assertThat(classified.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("s3://")));
    }

    public void testInflaterPrematureEofIsMalformedInput() {
        EOFException inflater = new EOFException("Unexpected end of ZLIB input stream");
        RuntimeException classified = ExternalFailures.classify(inflater);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        // IOException is not chained: its message may embed storage URIs which would leak via caused_by.
        assertNull("EOFException must not be chained to prevent caused_by leaks", classified.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));

        RuntimeException surfaced = ExternalFailures.surface(inflater, "Streaming parallel parsing failed");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(surfaced));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("Streaming parallel parsing failed"));

        UncheckedIOException wrapped = new UncheckedIOException(inflater);
        RuntimeException classifiedWrapped = ExternalFailures.classify(wrapped);
        assertThat(classifiedWrapped, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classifiedWrapped));
    }

    public void testCredentialsExpiredPassesThroughAs400() {
        var expired = new ExternalCredentialsExpiredException("Session credentials expired reading [k]");
        RuntimeException classified = ExternalFailures.classify(expired);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalCredentialsExpiredException.class));
        assertEquals(expired.getMessage(), classified.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        assertSame(expired, ExternalFailures.surface(expired, "ctx"));
        assertNotNull(ExceptionsHelper.unwrap(new ExecutionException(expired), ExternalCredentialsExpiredException.class));
        assertNull(ExceptionsHelper.unwrap(new IOException("HTTP 400 ExpiredToken"), ExternalCredentialsExpiredException.class));
    }

    public void testObjectChangedPassesThroughAs503() {
        var changed = new ExternalObjectChangedException("Object changed during read of [k]");
        RuntimeException classified = ExternalFailures.classify(changed);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalObjectChangedException.class));
        assertEquals(changed.getMessage(), classified.getMessage());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(classified));
        assertSame(changed, ExternalFailures.surface(changed, "ctx"));
    }

    public void testRetryableStatusPolicy() {
        // Shared by every storage backend (S3/GCS/Azure/HTTP) to decide 503-vs-client: server-side and
        // throttling statuses are retryable; 2xx/4xx (including not-found/forbidden) are not.
        for (int retryable : new int[] { 429, 500, 502, 503, 504 }) {
            assertTrue("HTTP " + retryable + " should be retryable", ExternalUnavailableException.isRetryableStatus(retryable));
        }
        // 501 (Not Implemented) and 505 (HTTP Version Not Supported) are permanent 5xx outcomes, not transient.
        for (int permanent : new int[] { 200, 206, 400, 403, 404, 416, 501, 505 }) {
            assertFalse("HTTP " + permanent + " should not be retryable", ExternalUnavailableException.isRetryableStatus(permanent));
        }
    }

    public void testBugLikeExceptionsBecomeServerException() {
        for (Throwable bug : new Throwable[] {
            new IllegalStateException("broken invariant"),
            new NullPointerException(),
            new RuntimeException("unexpected"),
            new Exception("checked, non-IO") }) {
            RuntimeException classified = ExternalFailures.classify(bug);
            assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
            assertNull("unchecked SDK failures land here, so the cause must not be chained", classified.getCause());
            assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classified));
        }
        RuntimeException sdk = ExternalFailures.classify(new RuntimeException("Unable to execute HTTP request to s3://secret-bucket/k"));
        assertThat(sdk.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("secret-bucket")));
    }

    public void testClassifyDetachesTypedFailuresFromTheirCause() {
        ExternalClientException loser = new ExternalClientException(
            ExternalException.Condition.OBJECT_NOT_FOUND,
            StoragePath.of("s3://secret-bucket/private/b.csv"),
            "",
            "",
            new IOException("NoSuchKey: secret-bucket/private/b.csv")
        );
        ExternalCredentialsExpiredException typed = new ExternalCredentialsExpiredException(
            StoragePath.of("s3://secret-bucket/private/a.csv"),
            "ExpiredToken",
            "",
            new IOException("The provided token has expired for secret-bucket.s3.amazonaws.com")
        );
        typed.addSuppressed(loser);
        typed.addSuppressed(new IOException("raw loser for secret-bucket"));

        RuntimeException classified = ExternalFailures.classify(typed);

        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalCredentialsExpiredException.class));
        assertNotSame(typed, classified);
        assertNull(classified.getCause());
        assertEquals(typed.getMessage(), classified.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        assertEquals("only the typed suppressed failure is kept", 1, classified.getSuppressed().length);
        assertThat(classified.getSuppressed()[0], org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertNull(classified.getSuppressed()[0].getCause());
        assertNotSame("even a detached failure is copied, since it may be shared", classified, ExternalFailures.classify(classified));
    }

    /**
     * A cache rethrows one failure instance to every concurrent waiter, and each waiter's caller annotates what it
     * gets with its own dataset. Detaching must hand out a copy, or the annotations pile up on the shared instance.
     */
    public void testDetachCopiesSoASharedFailureIsNeverAnnotated() {
        var shared = new ExternalClientException("Unreadable footer in [a.parquet]");
        shared.setDetail("bad magic");

        ExternalException first = ExternalFailures.detach(shared);
        first.setDatasetContext("tmax", "noaa", "s3");
        ExternalException second = ExternalFailures.detach(shared);
        second.setDatasetContext("tmin", "noaa", "s3");

        assertEquals("Unreadable footer in [a.parquet]: bad magic", shared.getMessage());
        assertThat(first.getMessage(), org.hamcrest.Matchers.containsString("bad magic"));
        assertThat(first.getMessage(), org.hamcrest.Matchers.containsString("in dataset [tmax]"));
        assertThat(first.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("tmin")));
        assertThat(second.getMessage(), org.hamcrest.Matchers.containsString("in dataset [tmin]"));
        assertThat(second.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("tmax")));
    }

    public void testDetailAndDatasetContextSurviveDetach() {
        var annotated = new ExternalClientException("Access denied reading [file.parquet]", new IOException("s3://bucket/k"));
        annotated.setDetail("some reader detail");
        annotated.setDatasetContext("tmax", "noaa", "s3");

        ExternalException detached = ExternalFailures.detach(annotated);

        assertEquals(annotated.getMessage(), detached.getMessage());
        assertNull(detached.getCause());
    }

    /**
     * The user never sees a client failure's detail, so the first one of a read is logged at WARN. The failures
     * that only get suppressed under it are logged at DEBUG, or a read over many objects floods the log.
     */
    /**
     * A row excerpt is user data, not a location, and can legitimately look like one (a URL or an absolute
     * path in a column). {@code rowError} must not assert it's free of storage-URI schemes or paths: under
     * {@code -ea} that assert is an {@link AssertionError}, which a row-processing loop does not catch and
     * {@code ElasticsearchUncaughtExceptionHandler} treats as fatal, halting the node instead of returning 400.
     */
    public void testRowErrorAcceptsRowDataThatLooksLikeALocation() {
        for (String row : new String[] {
            "Row [3] of [x.csv]: expected 2 columns, got 3; row: 1,/api/users,extra",
            "Row [3] of [x.csv]: expected 2 columns, got 3; row: 1,https://example.com/a",
            "Row [3] of [x.csv]: expected 2 columns, got 3; row: 1,s3://not-a-real-bucket/k" }) {
            ExternalClientException result = ExternalFailures.rowError(null, row);
            assertEquals(row, result.getMessage());
        }
    }

    /**
     * {@code rowError}'s cause may be any throwable a caller passes; a location leaked through its cause
     * chain must still trip the assert, even though the message itself is never checked.
     */
    public void testRowErrorStillAssertsOnALeakedCause() {
        IOException leaking = new IOException("failed reading s3://secret-bucket/k");
        expectThrows(AssertionError.class, () -> ExternalFailures.rowError(leaking, "Row [3] of [x.csv]: malformed"));
    }

    public void testOnlyTheFirstClientFailureIsLoggedAtWarn() {
        for (Throwable clientFailure : new Throwable[] {
            builtBySdk(new IOException("truncated")),
            builtBySdk(new IllegalArgumentException("bad page")) }) {
            ExternalFailures.WITHHELD_MESSAGE_WARN.reset();
            MockLog.assertThatLogger(
                () -> ExternalFailures.classify(clientFailure),
                ExternalFailures.class,
                new MockLog.SeenEventExpectation("first", ExternalFailures.class.getCanonicalName(), Level.WARN, "External read failed*")
            );
            MockLog.assertThatLogger(
                () -> ExternalFailures.classifySuppressed(clientFailure),
                ExternalFailures.class,
                new MockLog.UnseenEventExpectation("suppressed", ExternalFailures.class.getCanonicalName(), Level.WARN, "*")
            );
        }
    }

    public void testSuppressedServerFailureIsStillLoggedAtWarn() {
        MockLog.assertThatLogger(
            () -> ExternalFailures.classifySuppressed(new IllegalStateException("broken invariant")),
            ExternalFailures.class,
            new MockLog.SeenEventExpectation("server", ExternalFailures.class.getCanonicalName(), Level.WARN, "Unexpected failure*")
        );
    }

    public void testClassifyFallsBackToClassNameWhenMessageIsNull() {
        // The actual bug this hunk fixes: classify() must use detail() so a null-message fault surfaces its
        // class name instead of a useless "null" in the user-facing message. Covers both the server (bare
        // NPE) and client (bare IOException) branches.
        RuntimeException server = ExternalFailures.classify(new NullPointerException());
        assertThat(server, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertThat(server.getMessage(), org.hamcrest.Matchers.containsString("NullPointerException"));
        assertThat(server.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("null")));

        RuntimeException client = ExternalFailures.classify(new IOException());
        assertThat(client, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertThat(client.getMessage(), org.hamcrest.Matchers.containsString("IOException"));
        assertThat(client.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("null")));
    }

    public void testSurfaceRethrowsErrorUnchanged() {
        AssertionError error = new AssertionError("boom");
        AssertionError thrown = expectThrows(AssertionError.class, () -> ExternalFailures.surface(error, "ctx"));
        assertSame(error, thrown);
    }

    public void testSurfaceReturnsRuntimeExceptionAsIs() {
        // Plain RuntimeException, status carriers, and IllegalArgumentException share the same passthrough
        // branch — they self-describe their status (or lack of one) and the worker context is not relevant.
        RuntimeException plain = new RuntimeException("oops");
        assertSame(plain, ExternalFailures.surface(plain, "ctx"));

        var carrier = new ExternalClientException("already typed", new IOException("io"));
        assertSame(carrier, ExternalFailures.surface(carrier, "ctx"));

        var iae = new IllegalArgumentException("bad arg");
        assertSame(iae, ExternalFailures.surface(iae, "ctx"));
    }

    public void testSurfaceWrapsIoExceptionAsExternalClient() {
        IOException ioe = new IOException("record exceeded external_max_record_size");
        RuntimeException surfaced = ExternalFailures.surface(ioe, "Streaming parallel parsing failed");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertNull("IOException must not be chained to prevent caused_by leaks", surfaced.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(surfaced));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("Streaming parallel parsing failed"));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("record exceeded external_max_record_size"));
    }

    public void testSurfaceStripsIoMessageWithStorageUri() {
        IOException ioe = new IOException("Failed to read s3://secret-bucket/private/prefix/data.csv", new IOException("sdk detail"));
        RuntimeException surfaced = ExternalFailures.surface(ioe, "Failed to read CSV batch");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertNull(surfaced.getCause());
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("Failed to read CSV batch"));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("IOException"));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("secret-bucket")));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("private/prefix")));
    }

    public void testSurfaceWrapsUncheckedIoExceptionAsExternalClient() {
        // UncheckedIOException is a RuntimeException — but it represents an underlying IO failure, so it must
        // route through the IO branch (400 + context prefix), not the generic RuntimeException passthrough.
        IOException cause = new IOException("upstream");
        UncheckedIOException uioe = new UncheckedIOException("wrapped", cause);
        RuntimeException surfaced = ExternalFailures.surface(uioe, "Failed to read CSV batch");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertNull("UncheckedIOException must not be chained to prevent caused_by leaks", surfaced.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(surfaced));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("Failed to read CSV batch"));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("wrapped"));
    }

    public void testSurfaceDoesNotRescueIoBuriedUnderRuntimeException() {
        // surface() cannot recover a status signal already destroyed by an intermediate RuntimeException
        // wrapper — the documented contract is that callers pass the raw stored throwable, not a pre-wrap.
        RuntimeException wrapper = new RuntimeException("wrapper", new IOException("inner"));
        assertSame(wrapper, ExternalFailures.surface(wrapper, "ctx"));
    }

    public void testSurfaceComposesIdempotentlyForCheckedNonIo() {
        // The other main coordinator path: a stored InterruptedException produces an ExternalServerException
        // at surface(), which classify() then keeps (as a detached copy) at the read boundary.
        InterruptedException interrupted = new InterruptedException("worker interrupted");
        RuntimeException surfaced = ExternalFailures.surface(interrupted, "Parallel parsing failed");
        RuntimeException classified = ExternalFailures.classify(surfaced);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertEquals(surfaced.getMessage(), classified.getMessage());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classified));
    }

    public void testSurfaceWrapsCheckedNonIoAsExternalServer() {
        // Bare InterruptedException stored after a worker thread was interrupted is the canonical case here:
        // we have no evidence of bad input, so the bug stays visible as a 500 with the context prefix.
        InterruptedException interrupted = new InterruptedException("worker interrupted");
        RuntimeException surfaced = ExternalFailures.surface(interrupted, "Parallel parsing failed");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertNull("the stored failure must not be chained to prevent caused_by leaks", surfaced.getCause());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(surfaced));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("Parallel parsing failed"));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("worker interrupted"));
    }

    public void testSurfaceFallsBackToClassNameWhenMessageIsNull() {
        // detail() returns the class's simple name when getMessage() is null, so the formatted message reads
        // "<prefix>: InterruptedException" rather than the unhelpful "<prefix>: null".
        InterruptedException interrupted = new InterruptedException();
        RuntimeException surfaced = ExternalFailures.surface(interrupted, "Parallel parsing failed");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("InterruptedException"));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("null")));
    }

    public void testSurfaceComposesIdempotentlyWithClassify() {
        // The expected composition at production sites: surface() runs at the worker rethrow, classify() runs
        // at the read boundary. surface()'s typed output must pass through classify() as the same type
        // with the same message, so the prefix and status the worker site set are preserved end-to-end.
        IOException ioe = new IOException("truncated");
        RuntimeException surfaced = ExternalFailures.surface(ioe, "Failed to read NDJSON page");
        RuntimeException classified = ExternalFailures.classify(surfaced);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertEquals("classify must keep an already-typed surface() result", surfaced.getMessage(), classified.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
    }

    public void testRedactHttpUrl() {
        assertEquals("https://h.example.com/p/x.csv", ExternalFailures.redactHttpUrl("https://h.example.com/p/x.csv?sig=1"));
        assertEquals("http://h.example.com/p/x.csv", ExternalFailures.redactHttpUrl("http://u:p@h.example.com/p/x.csv"));
        // An '@' after the first '/' is part of the path, not user info.
        assertEquals("https://h.example.com/a@b/x.csv", ExternalFailures.redactHttpUrl("https://h.example.com/a@b/x.csv?sig=1"));
        assertEquals("https://h.example.com", ExternalFailures.redactHttpUrl("https://u:p@h.example.com"));
        assertEquals("https://h.example.com", ExternalFailures.redactHttpUrl("https://h.example.com?sig=1"));
        assertEquals("h.example.com/x.csv?sig=1", ExternalFailures.redactHttpUrl("h.example.com/x.csv?sig=1"));
        // Whichever of '#' and '?' comes first ends the path.
        assertEquals("https://h.example.com/x.csv", ExternalFailures.redactHttpUrl("https://h.example.com/x.csv#frag?sig=1"));
        assertEquals("https://h.example.com/x.csv", ExternalFailures.redactHttpUrl("https://h.example.com/x.csv?sig=1#frag"));
        // For wasbs the user info is the container name, not a secret.
        String wasbs = "wasbs://container@account.blob.core.windows.net/x.csv";
        assertEquals(wasbs, ExternalFailures.redactHttpUrl(wasbs));
    }

    public void testRootCauseStepsThroughAToStringDerivedWrapper() {
        IOException real = new IOException("Object not found: s3://bucket/x.csv");
        // The shape the resolver sees: a JDK ExecutionException whose message is the cause's toString(). It is not
        // an ElasticsearchWrapperException, so ExceptionsHelper.unwrapCause would return it unchanged.
        ExecutionException wrapper = new ExecutionException(real);
        assertSame(real, ExternalFailures.rootCause(wrapper));
        assertSame(wrapper, ExceptionsHelper.unwrapCause(wrapper));
    }

    public void testRootCauseKeepsAWrapperThatCarriesItsOwnMessage() {
        IOException real = new IOException("Object not found: s3://bucket/x.csv");
        IOException described = new IOException("Failed to list bucket [b]", real);
        assertSame(described, ExternalFailures.rootCause(described));
    }

    public void testNoStoragePathLeakedGuard() {
        // Clean messages pass.
        assertTrue(ExternalFailures.noStoragePathLeaked(new ExternalClientException("Access denied reading [file.parquet]")));
        assertTrue(ExternalFailures.noStoragePathLeaked(new ExternalClientException("External store unavailable (HTTP 503)")));
        // Top-level message containing a storage URI fails.
        assertFalse(ExternalFailures.noStoragePathLeaked(new ExternalClientException("Access denied [s3://bucket/prefix/file.parquet]")));
        assertFalse(ExternalFailures.noStoragePathLeaked(new ExternalClientException("Read error [gs://bucket/file.parquet]")));
        // Cause message containing a storage URI also fails (the cause appears in the API response as caused_by).
        var withCause = new ExternalClientException(
            new RuntimeException("s3://bucket/prefix/file.parquet: connection reset"),
            "Failed to read external source: {}",
            "connection reset"
        );
        assertFalse(ExternalFailures.noStoragePathLeaked(withCause));
        // Any http:// or https:// URL in an exception message fails — covers HTTP-native and HTTPS storage endpoints.
        assertFalse(
            ExternalFailures.noStoragePathLeaked(
                new ExternalClientException("GET https://account.blob.core.windows.net/container/blob → 403")
            )
        );
        assertFalse(
            ExternalFailures.noStoragePathLeaked(new ExternalClientException("GET https://storage.googleapis.com/bucket/object → 404"))
        );
        assertFalse(
            ExternalFailures.noStoragePathLeaked(new ExternalClientException("HEAD http://storage.example.com/bucket/file.parquet → 404"))
        );
    }

    public void testDatasetContextAppendsToMessage() {
        var ex = new ExternalClientException("Access denied reading [file.parquet]");
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("dataset")));

        ex.setDatasetContext("tmax", "noaa", "s3");
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("in dataset [tmax]"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("from data source [noaa]"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("(s3)"));

        // setDatasetContext with only dataset name (no datasource)
        var ex2 = new ExternalClientException("Access denied reading [file.parquet]");
        ex2.setDatasetContext("tmax", null, null);
        assertThat(ex2.getMessage(), org.hamcrest.Matchers.containsString("in dataset [tmax]"));
        assertThat(ex2.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("from data source")));

        // setDatasetLabel with a pre-formatted string
        var ex3 = new ExternalClientException("Access denied reading [file.parquet]");
        ex3.setDatasetLabel("in dataset [tmax] from data source [noaa] (s3)");
        assertThat(ex3.getMessage(), org.hamcrest.Matchers.containsString("in dataset [tmax] from data source [noaa] (s3)"));
    }

    public void testDatasetContextAppendsAfterDetail() {
        var ex = new ExternalClientException("Access denied reading [file.parquet]");
        ex.setDetail("some reader detail");
        ex.setDatasetContext("tmax", "noaa", "s3");
        // Order: base message, then detail, then dataset context
        String msg = ex.getMessage();
        int detailPos = msg.indexOf("some reader detail");
        int ctxPos = msg.indexOf("in dataset [tmax]");
        assertTrue("detail must appear before dataset context", detailPos < ctxPos);
    }

    /** A local location has no scheme to spot, so an absolute filesystem path is unsafe on its own. */
    public void testAbsoluteFilesystemPathIsNotSafe() {
        for (String unsafe : new String[] {
            "/data/private/x.csv",
            "Path is not a regular file: /data/private/x.csv",
            "cannot open [/srv/esql/in.parquet]",
            "Directory does not exist: C:\\data\\private",
            "failed (/var/lib/es/x.orc)",
            "/tmp",
            "Directory does not exist: /secrets" }) {
            assertFalse(unsafe, ExternalFailures.safeForUserMessage(unsafe));
        }
        for (String safe : new String[] {
            "[day=2/part-0.parquet] has [1] columns, [day=1/part-0.parquet] has [2]",
            "Row [3] of [x.csv]: [3] columns, the schema has [2]",
            "content type application/json is not supported",
            "losing precision above 2^53",
            "read and/or write",
            "ratio 3/4",
            "delimiter [/]" }) {
            assertTrue(safe, ExternalFailures.safeForUserMessage(safe));
        }
    }

    public void testAuthorityLessFileUriIsAStorageUri() {
        assertFalse(ExternalFailures.safeForUserMessage("cannot read file:/data/private/x.csv"));
    }

    /**
     * SDK and DNS failures name the endpoint host without a scheme, and the host embeds the bucket or storage account.
     */
    public void testCloudStorageHostIsNotSafe() {
        for (String unsafe : new String[] {
            "secret-bucket.s3.us-east-1.amazonaws.com: Name or service not known",
            "Unable to execute HTTP request: Connect to secret-bucket.s3.amazonaws.com:443 failed",
            "storage.googleapis.com: nodename nor servname provided",
            "secretaccount.blob.core.windows.net: Temporary failure in name resolution",
            "secretaccount.blob.core.usgovcloudapi.net: Temporary failure in name resolution",
            "secretaccount.blob.core.chinacloudapi.cn: Temporary failure in name resolution" }) {
            assertFalse(unsafe, ExternalFailures.safeForUserMessage(unsafe));
        }
    }

    /**
     * A server-side failure's cause is its only diagnosis, so it is logged at WARN when detached. A client failure's
     * condition already says what is wrong, so its cause stays at DEBUG.
     */
    public void testDetachLogsTheCauseOfAServerFailureAtWarn() {
        MockLog.assertThatLogger(
            () -> ExternalFailures.detach(new ExternalServerException("invariant violated", new IllegalStateException("root"))),
            ExternalFailures.class,
            new MockLog.SeenEventExpectation("server", ExternalFailures.class.getCanonicalName(), Level.WARN, "External failure detached*")
        );
        MockLog.assertThatLogger(
            () -> ExternalFailures.detach(new ExternalUnavailableException("store 503", new IOException("root"))),
            ExternalFailures.class,
            new MockLog.SeenEventExpectation(
                "unavailable",
                ExternalFailures.class.getCanonicalName(),
                Level.WARN,
                "External failure detached*"
            )
        );
        MockLog.assertThatLogger(
            () -> ExternalFailures.detach(new ExternalClientException("bad file", new IOException("root"))),
            ExternalFailures.class,
            new MockLog.UnseenEventExpectation("client", ExternalFailures.class.getCanonicalName(), Level.WARN, "*")
        );
    }

    /**
     * Parallel readers of one source often fail the same way (e.g. all throttled), so only the first typed server
     * failure's cause is logged at WARN; those suppressed under it, or attached to it as suppressed, are at DEBUG.
     */
    @TestLogging(value = "org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures:DEBUG", reason = "asserts DEBUG events")
    public void testSuppressedTypedServerFailureIsLoggedAtDebug() {
        String logger = ExternalFailures.class.getCanonicalName();
        MockLog.assertThatLogger(
            () -> ExternalFailures.classifySuppressed(new ExternalUnavailableException("store 503", new IOException("throttled"))),
            ExternalFailures.class,
            new MockLog.UnseenEventExpectation("no warn", logger, Level.WARN, "*"),
            new MockLog.SeenEventExpectation("debug", logger, Level.DEBUG, "External failure detached*")
        );

        var first = new ExternalUnavailableException("store 503", new IOException("throttled"));
        first.addSuppressed(new ExternalUnavailableException("store 503", new IOException("throttled")));
        try (MockLog mockLog = MockLog.capture(ExternalFailures.class)) {
            List<LogEvent> warns = new ArrayList<>();
            mockLog.addExpectation(new MockLog.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    if (event.getLevel().equals(Level.WARN)) {
                        warns.add(event);
                    }
                }

                @Override
                public void assertMatched() {
                    assertEquals("only the first failure is logged at WARN", 1, warns.size());
                }
            });
            ExternalFailures.classify(first);
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * A client failure's WARN is one line naming the failure; the stack trace is only at DEBUG.
     */
    @TestLogging(value = "org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures:DEBUG", reason = "asserts the DEBUG trace")
    public void testClientFailureWarnCarriesNoStackTrace() {
        ExternalFailures.WITHHELD_MESSAGE_WARN.reset();
        IOException failure = new IOException("truncated reading s3://bucket/x.csv");
        MockLog.assertThatLogger(
            () -> ExternalFailures.classify(failure),
            ExternalFailures.class,
            new MockLog.SeenEventExpectation(
                "one line",
                ExternalFailures.class.getCanonicalName(),
                Level.WARN,
                "*java.io.IOException: truncated reading s3://bucket/x.csv"
            ),
            new MockLog.SeenEventExpectation(
                "trace",
                ExternalFailures.class.getCanonicalName(),
                Level.DEBUG,
                "External read failed with a client error (cause logged, not forwarded)"
            ),
            new MockLog.LoggingExpectation() {
                private boolean traceAtWarn;

                @Override
                public void match(LogEvent event) {
                    if (event.getLevel().equals(Level.WARN) && event.getThrown() != null) {
                        traceAtWarn = true;
                    }
                }

                @Override
                public void assertMatched() {
                    assertFalse("the WARN must not carry the stack trace", traceAtWarn);
                }
            }
        );
    }

    /**
     * Asserts that every storage-provider URI scheme is covered by {@link ExternalFailures#safeForUserMessage}.
     * S3: s3/s3a/s3n, GCS: gs, Azure: wasb/wasbs, HTTP: http/https, Flight: flight/grpc/grpcs, local: file.
     * If a new provider adds a new scheme, this test will catch it.
     */
    public void testStorageUriSchemesCoversAllProviderSchemes() {
        for (String scheme : java.util.List.of(
            "s3",
            "s3a",
            "s3n",
            "gs",
            "wasb",
            "wasbs",
            "http",
            "https",
            "flight",
            "grpc",
            "grpcs",
            "file"
        )) {
            assertFalse(
                "scheme [" + scheme + "://] must be treated as a storage URI",
                ExternalFailures.safeForUserMessage(scheme + "://bucket/object")
            );
        }
    }

    private static final String IAM_DENIAL = "User: arn:aws:sts::123456789012:assumed-role/reader/session is not authorized to "
        + "perform: s3:GetObject on resource: \"arn:aws:s3:::bucket/key\" with an explicit deny in an identity-based policy";

    /**
     * Stands in for construction inside a storage SDK: the esql test classpath has no SDK, and built here a
     * throwable's top frame would be this test class.
     */
    private static <T extends Throwable> T builtBySdk(T t) {
        return builtBy("software.amazon.awssdk.services.s3.model.S3Exception$BuilderImpl", t);
    }

    private static <T extends Throwable> T builtBy(String className, T t) {
        t.setStackTrace(new StackTraceElement[] { new StackTraceElement(className, "build", null, 1) });
        return t;
    }

    /**
     * A storage client's message relays what the remote said, and naming no URI or host does not make it safe:
     * every arm that forwards a failure's own text shows its class name instead.
     */
    public void testStorageClientTextIsNotForwarded() {
        RuntimeException io = ExternalFailures.classify(builtBySdk(new IOException(IAM_DENIAL)));
        assertThat(io.getMessage(), not(containsString("arn:aws")));
        assertThat(io.getMessage(), containsString("IOException"));
        RuntimeException iae = ExternalFailures.classify(builtBySdk(new IllegalArgumentException(IAM_DENIAL)));
        assertThat(iae.getMessage(), not(containsString("arn:aws")));
        RuntimeException server = ExternalFailures.classify(builtBySdk(new RuntimeException(IAM_DENIAL)));
        assertThat(server.getMessage(), not(containsString("arn:aws")));
        assertThat(server.getMessage(), containsString("RuntimeException"));
        RuntimeException surfaced = ExternalFailures.surface(builtBySdk(new IOException(IAM_DENIAL)), "Split discovery failed");
        assertThat(surfaced.getMessage(), not(containsString("arn:aws")));
        // A site that copies rootDetail into its own exception cannot launder the text either.
        IllegalArgumentException copied = new IllegalArgumentException(
            ExternalFailures.rootDetail(builtBySdk(new RuntimeException(IAM_DENIAL)))
        );
        assertThat(ExternalFailures.classify(copied).getMessage(), not(containsString("arn:aws")));
        // Elasticsearch wrapping the SDK with a new message that includes the remote's text: rootCause stops at the
        // wrapper, so composedByStorageClient would miss it; forwardableDetail walks the chain.
        IOException sdk = builtBySdk(new IOException(IAM_DENIAL));
        IOException laundered = new IOException("listing failed: " + sdk.getMessage(), sdk);
        assertNull(ExternalFailures.forwardableDetail(laundered));
        assertThat(ExternalFailures.classify(laundered).getMessage(), not(containsString("arn:aws")));
    }

    /**
     * A format library's or the JDK's message describes the bytes we read, not who we are, and is what the user needs
     * to fix the file: it reaches the response, through every arm and through sites that copy {@link
     * ExternalFailures#rootDetail}.
     */
    public void testFormatLibraryAndJdkTextIsForwarded() {
        String magic = "file is not a Parquet file. Expected magic number at tail, but found [1, 2, 3, 4]";
        IOException parquet = builtBy("org.apache.parquet.hadoop.ParquetFileReader", new IOException(magic));
        assertThat(ExternalFailures.classify(parquet).getMessage(), containsString(magic));
        assertThat(ExternalFailures.surface(parquet, "Streaming parallel parsing failed").getMessage(), containsString(magic));

        String jackson = "Unexpected character ('}' (code 125)): was expecting double-quote to start field name";
        IllegalArgumentException json = builtBy("com.fasterxml.jackson.core.JsonParser", new IllegalArgumentException(jackson));
        assertThat(ExternalFailures.classify(json).getMessage(), containsString(jackson));

        EOFException truncated = new EOFException("Unexpected end of ZLIB input stream");
        truncated.setStackTrace(new StackTraceElement[] { new StackTraceElement("java.util.zip.InflaterInputStream", "fill", null, 1) });
        assertThat(ExternalFailures.classify(new UncheckedIOException(truncated)).getMessage(), containsString("Unexpected end of ZLIB"));

        // FileSourceFactory and TableCatalog copy rootDetail into an IAE of their own; the diagnosis survives.
        IllegalArgumentException copied = new IllegalArgumentException(ExternalFailures.rootDetail(parquet), parquet);
        assertThat(ExternalFailures.classify(copied).getMessage(), containsString(magic));

        assertThat(ExternalFailures.classify(new IOException("Object not found: x.csv")).getMessage(), containsString("x.csv"));
        assertThat(
            ExternalFailures.classify(new IllegalArgumentException("column [a] has unsupported type")).getMessage(),
            containsString("column [a] has unsupported type")
        );
    }

    public void testComposedByStorageClient() {
        assertTrue(ExternalFailures.composedByStorageClient(builtBySdk(new RuntimeException(IAM_DENIAL))));
        for (String client : List.of(
            "com.google.cloud.storage.StorageException",
            "com.google.api.client.http.HttpResponseException$Builder",
            "com.azure.storage.blob.models.BlobStorageException",
            "org.apache.arrow.flight.CallStatus",
            "io.grpc.Status",
            "org.apache.iceberg.rest.ErrorHandlers$DefaultErrorHandler"
        )) {
            assertTrue(client, ExternalFailures.composedByStorageClient(builtBy(client, new IOException("remote said"))));
        }
        assertFalse(ExternalFailures.composedByStorageClient(new IOException("ours")));
        assertFalse(ExternalFailures.composedByStorageClient(builtBy("org.apache.parquet.hadoop.ParquetFileReader", new IOException())));
        assertFalse(ExternalFailures.composedByStorageClient(builtBy("java.util.zip.InflaterInputStream", new EOFException())));
        IOException noTrace = new IOException("no trace");
        noTrace.setStackTrace(new StackTraceElement[0]);
        assertFalse(ExternalFailures.composedByStorageClient(noTrace));
    }

    /** A cause below an ElasticsearchException that is not an ExternalException is rendered by caused_by too. */
    /**
     * A loser that already suppresses the winner must still carry that link after {@link ExternalFailures#classifySuppressed}:
     * cycle detection walks suppressed links, and dropping them would let the loser be re-attached onto the winner.
     */
    public void testClassifySuppressedKeepsElasticsearchExceptionSuppressedLinks() {
        CircuitBreakingException winner = new CircuitBreakingException("[parquet reader]", CircuitBreaker.Durability.TRANSIENT);
        CircuitBreakingException loser = new CircuitBreakingException("[parquet sliding window]", CircuitBreaker.Durability.TRANSIENT);
        loser.addSuppressed(winner);

        RuntimeException classified = ExternalFailures.classifySuppressed(loser);
        assertThat(classified, instanceOf(CircuitBreakingException.class));
        assertNull(classified.getCause());
        assertArrayEquals(new Throwable[] { winner }, classified.getSuppressed());
        assertSame(winner, classified.getSuppressed()[0]);
    }

    public void testForeignElasticsearchExceptionIsDetached() {
        ElasticsearchException foreign = new ElasticsearchStatusException(
            "read refused",
            RestStatus.BAD_REQUEST,
            builtBySdk(new RuntimeException(IAM_DENIAL))
        );
        foreign.addMetadata("es.reason_code", "refused");
        foreign.addBodyHeader("body_header", "b");
        foreign.addHttpHeader("Retry-After", "5");
        RuntimeException classified = ExternalFailures.classify(foreign);
        assertNull(classified.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        assertEquals("read refused", classified.getMessage());
        ElasticsearchException copy = (ElasticsearchException) classified;
        assertEquals(List.of("refused"), copy.getMetadata("es.reason_code"));
        assertEquals(List.of("b"), copy.getBodyHeader("body_header"));
        assertEquals(List.of("5"), copy.getHttpHeader("Retry-After"));

        CircuitBreakingException breaker = new CircuitBreakingException("tripped", 10, 5, CircuitBreaker.Durability.TRANSIENT);
        breaker.initCause(new RuntimeException(IAM_DENIAL));
        RuntimeException detachedBreaker = ExternalFailures.classify(breaker);
        assertThat(detachedBreaker, instanceOf(CircuitBreakingException.class));
        assertNull(detachedBreaker.getCause());
        assertEquals(10, ((CircuitBreakingException) detachedBreaker).getBytesWanted());

        ElasticsearchException causeless = new ElasticsearchStatusException("throttled", RestStatus.TOO_MANY_REQUESTS);
        assertSame(causeless, ExternalFailures.classify(causeless));

        ElasticsearchException messageless = new ElasticsearchStatusException(null, RestStatus.BAD_REQUEST, new IOException("root"));
        assertEquals("ElasticsearchStatusException", ExternalFailures.classify(messageless).getMessage());
    }

    /**
     * The leak guard checks what the caller receives: a foreign exception whose dropped cause names a location must
     * not trip it under {@code -ea}.
     */
    public void testLeakGuardChecksTheDetachedForeignException() {
        ElasticsearchException foreign = new ElasticsearchStatusException(
            "read refused",
            RestStatus.BAD_REQUEST,
            new IOException("GET s3://bucket/secret/x.parquet failed")
        );
        RuntimeException classified = ExternalFailures.classify(foreign);
        assertNull(classified.getCause());
        assertEquals("read refused", classified.getMessage());
    }

    /** The result carries no cause, so query failure ranking could not find a cancellation buried in it. */
    public void testNestedCancellationIsReportedAsTheCancellation() {
        RuntimeException classified = ExternalFailures.classify(new CompletionException(new TaskCancelledException("cancelled")));
        assertThat(classified, instanceOf(TaskCancelledException.class));
        assertNull(classified.getCause());
    }

    /**
     * A resolution failure's WARN line names the deepest cause (an access denial's reason, which the response never
     * carries) at most once per interval: anyone who can query a failing dataset can repeat the failure.
     */
    public void testWithheldCauseIsLoggedOncePerInterval() {
        ExternalFailures.WITHHELD_MESSAGE_WARN.reset();
        RuntimeException sdk = builtBySdk(new RuntimeException(IAM_DENIAL));
        IOException failure = new IOException("wrapped", sdk);
        assertSame(sdk, ExternalFailures.withheldCauseToLog("Access denied reading [x.parquet]", failure));
        assertNull("throttled", ExternalFailures.withheldCauseToLog("Access denied reading [x.parquet]", failure));

        ExternalFailures.WITHHELD_MESSAGE_WARN.reset();
        assertNull("no cause to add", ExternalFailures.withheldCauseToLog("detail", new IOException("alone")));
        assertNull("detail already says it", ExternalFailures.withheldCauseToLog("x: " + IAM_DENIAL, failure));
        assertSame("those did not use up the interval", sdk, ExternalFailures.withheldCauseToLog("detail", failure));
    }

    /** The provider's reason for refusing a read survives only in this log, so it reaches WARN, once per interval. */
    public void testAccessDeniedReasonReachesWarnOncePerInterval() {
        ExternalFailures.ACCESS_DENIED_WARN.reset();
        RuntimeException sdk = builtBySdk(new RuntimeException(IAM_DENIAL));
        ExternalClientException denied = new ExternalClientException(
            ExternalClientException.Condition.ACCESS_DENIED,
            StoragePath.of("s3://bucket/key.parquet"),
            "HTTP 403 AccessDenied",
            "Verify the access_key and secret_key",
            sdk
        );
        MockLog.assertThatLogger(() -> {
            RuntimeException classified = ExternalFailures.classify(denied);
            assertNull(classified.getCause());
            assertThat(classified.getMessage(), not(containsString("arn:aws")));
        }, ExternalFailures.class, new MockLog.LoggingExpectation() {
            private boolean seen;

            @Override
            public void match(LogEvent event) {
                if (event.getLevel() == Level.WARN && event.getThrown() == denied) {
                    seen = true;
                }
            }

            @Override
            public void assertMatched() {
                assertTrue("the denial, with its SDK cause, must be logged at WARN", seen);
            }
        });
        MockLog.assertThatLogger(
            () -> ExternalFailures.classify(denied),
            ExternalFailures.class,
            new MockLog.UnseenEventExpectation("throttled", ExternalFailures.class.getCanonicalName(), Level.WARN, "*")
        );
    }

    /**
     * A withheld message has no other record, so it reaches WARN; once per interval across every read on the node,
     * since a corrupt file or a failing library would otherwise write one line per read. A forwarded one stays at DEBUG.
     */
    public void testWithheldMessageWarnsOncePerInterval() {
        ExternalFailures.WITHHELD_MESSAGE_WARN.reset();
        AtomicInteger warns = new AtomicInteger();
        MockLog.assertThatLogger(() -> {
            for (int i = 0; i < 100; i++) {
                ExternalFailures.classify(builtBySdk(new IOException("Unexpected end of ZLIB input stream")));
                ExternalFailures.classify(new IOException("Object not found: x.csv"));
            }
        }, ExternalFailures.class, new MockLog.LoggingExpectation() {
            @Override
            public void match(LogEvent event) {
                if (event.getLevel() == Level.WARN) {
                    warns.incrementAndGet();
                }
            }

            @Override
            public void assertMatched() {
                assertEquals(1, warns.get());
            }
        });
    }
}
