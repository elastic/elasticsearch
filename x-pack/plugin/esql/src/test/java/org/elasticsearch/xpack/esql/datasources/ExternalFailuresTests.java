/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class ExternalFailuresTests extends ESTestCase {

    public void testErrorIsRethrown() {
        AssertionError error = new AssertionError("boom");
        AssertionError thrown = expectThrows(AssertionError.class, () -> ExternalFailures.classify(error));
        assertSame(error, thrown);
    }

    public void testExternalExceptionsPassThroughWithTheirStatus() {
        var client = new ExternalClientException("bad file", new IOException("truncated"));
        assertDetached(client, ExternalFailures.classify(client), RestStatus.BAD_REQUEST);

        var server = new ExternalServerException("invariant violated", new IllegalStateException());
        assertDetached(server, ExternalFailures.classify(server), RestStatus.INTERNAL_SERVER_ERROR);

        var unavailable = new ExternalUnavailableException("store 503", new IOException());
        assertDetached(unavailable, ExternalFailures.classify(unavailable), RestStatus.SERVICE_UNAVAILABLE);
    }

    /** classify returns a new copy of an ExternalException: same type, status and message, but no cause. */
    private static void assertDetached(ExternalException original, RuntimeException classified, RestStatus status) {
        assertNotSame(original, classified);
        assertSame(original.getClass(), classified.getClass());
        assertEquals(status, ExceptionsHelper.status(classified));
        assertEquals(original.getMessage(), classified.getMessage());
        assertNull(classified.getCause());
    }

    public void testCircuitBreakingAndCancellationKeepTheirStatus() {
        var breaking = new CircuitBreakingException("over", 10, 5, CircuitBreaker.Durability.TRANSIENT);
        assertSame(breaking, ExternalFailures.classify(breaking));
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(ExternalFailures.classify(breaking)));

        var cancelled = new TaskCancelledException("cancelled");
        assertSame(cancelled, ExternalFailures.classify(cancelled));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(cancelled)));
    }

    public void testRejectedExecutionIsBackpressureNotServerError() {
        // A saturated thread pool (or the node shutting down) can reject work as an EsRejectedExecutionException.
        // That is load-shed backpressure (429), not a broken invariant in our reading code (500): classify must
        // return it unchanged so its self-carried 429 survives, rather than wrapping it as an ExternalServerException.
        // Storage concurrency permit exhaustion is a separate case: it is raised as a 503-class
        // ExternalUnavailableException at the concurrency-limiter boundary so the storage retry layer engages, so it
        // does not reach classify() as an EsRejectedExecutionException.
        var rejected = new EsRejectedExecutionException("rejected execution while reading external source");
        RuntimeException classified = ExternalFailures.classify(rejected);
        assertSame(rejected, classified);
        assertEquals(
            "permit/queue exhaustion must surface as 429 backpressure, not 500",
            RestStatus.TOO_MANY_REQUESTS,
            ExceptionsHelper.status(classified)
        );
    }

    public void testGenericElasticsearchExceptionPassesThrough() {
        var ese = new ElasticsearchException("generic");
        assertSame(ese, ExternalFailures.classify(ese));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(ExternalFailures.classify(ese)));
    }

    public void testIllegalArgumentExceptionKeptAs400() {
        var iae = new IllegalArgumentException("bad arg");
        RuntimeException classified = ExternalFailures.classify(iae);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertEquals("Malformed external data: bad arg", classified.getMessage());
        assertNull(classified.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));

        var located = new IllegalArgumentException("cannot parse s3://bucket/key.csv");
        RuntimeException locatedClassified = ExternalFailures.classify(located);
        assertThat(locatedClassified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertThat(locatedClassified.getMessage(), org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("bucket")));
        assertThat(locatedClassified.getMessage(), org.hamcrest.Matchers.containsString("IllegalArgumentException"));
        assertNull(locatedClassified.getCause());
    }

    public void testIoErrorsBecomeClientException() {
        for (Throwable io : new Throwable[] {
            new IOException("read failed"),
            new EOFException("truncated"),
            new UncheckedIOException(new IOException("wrapped")) }) {
            RuntimeException classified = ExternalFailures.classify(io);
            assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
            assertNull(classified.getCause());
            assertThat(classified.getMessage(), org.hamcrest.Matchers.startsWith("Failed to read external source: "));
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        }
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
            assertNull(classified.getCause());
            assertThat(classified.getMessage(), org.hamcrest.Matchers.startsWith("Unexpected failure reading external source: "));
            assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classified));
        }
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
        IOException ioe = new IOException("record exceeded max_record_size");
        RuntimeException surfaced = ExternalFailures.surface(ioe, "Streaming parallel parsing failed");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertNull(surfaced.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(surfaced));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("Streaming parallel parsing failed"));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("record exceeded max_record_size"));
    }

    public void testSurfaceWrapsUncheckedIoExceptionAsExternalClient() {
        // UncheckedIOException is a RuntimeException — but it represents an underlying IO failure, so it must
        // route through the IO branch (400 + context prefix), not the generic RuntimeException passthrough.
        IOException cause = new IOException("upstream");
        UncheckedIOException uioe = new UncheckedIOException("wrapped", cause);
        RuntimeException surfaced = ExternalFailures.surface(uioe, "Failed to read CSV batch");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertNull(surfaced.getCause());
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
        // at surface(), which classify() then passes through unchanged at the read boundary.
        InterruptedException interrupted = new InterruptedException("worker interrupted");
        RuntimeException surfaced = ExternalFailures.surface(interrupted, "Parallel parsing failed");
        RuntimeException classified = ExternalFailures.classify(surfaced);
        assertDetached((ExternalException) surfaced, classified, RestStatus.INTERNAL_SERVER_ERROR);
    }

    public void testSurfaceWrapsCheckedNonIoAsExternalServer() {
        // Bare InterruptedException stored after a worker thread was interrupted is the canonical case here:
        // we have no evidence of bad input, so the bug stays visible as a 500 with the context prefix.
        InterruptedException interrupted = new InterruptedException("worker interrupted");
        RuntimeException surfaced = ExternalFailures.surface(interrupted, "Parallel parsing failed");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertNull(surfaced.getCause());
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
        // at the read boundary. surface()'s typed output must pass through classify() as the same instance
        // so the prefix and status the worker site set are preserved end-to-end.
        IOException ioe = new IOException("truncated");
        RuntimeException surfaced = ExternalFailures.surface(ioe, "Failed to read NDJSON page");
        RuntimeException classified = ExternalFailures.classify(surfaced);
        assertDetached((ExternalException) surfaced, classified, RestStatus.BAD_REQUEST);
    }

    public void testDetachCopiesAndKeepsOnlyExternalSuppressed() {
        var original = new ExternalClientException("bad file", new IOException("s3://bucket/key"));
        original.addSuppressed(new ExternalServerException("kept"));
        original.addSuppressed(new IOException("dropped"));
        ExternalException detached = ExternalFailures.detach(original);
        assertNotSame(original, detached);
        assertNull(detached.getCause());
        assertEquals("bad file", detached.getMessage());
        assertEquals(1, detached.getSuppressed().length);
        assertThat(detached.getSuppressed()[0], org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertEquals("kept", detached.getSuppressed()[0].getMessage());
    }

    public void testWithoutCausePreservesThrottlingAndRetryAfter() {
        var unavailable = new ExternalUnavailableException(true, 1500L, new IOException("cause"), "slow down");
        ExternalUnavailableException copy = unavailable.withoutCause();
        assertNotSame(unavailable, copy);
        assertNull(copy.getCause());
        assertTrue(copy.throttling());
        assertEquals(1500L, copy.retryAfterMs());
        assertEquals("slow down", copy.getMessage());
    }

    public void testSafeForUserMessage() {
        assertFalse(ExternalFailures.safeForUserMessage("failed s3://bucket/key.csv"));
        assertFalse(ExternalFailures.safeForUserMessage("failed https://example.com/key.csv"));
        assertFalse(ExternalFailures.safeForUserMessage("bucket.s3.amazonaws.com: Name or service not known"));
        assertFalse(ExternalFailures.safeForUserMessage("cannot open /data/x.csv"));
        assertTrue(ExternalFailures.safeForUserMessage("column [id] missing"));
    }

    public void testBuriedCancellationAndBreakerTripSurviveClassify() {
        TaskCancelledException cancelled = new TaskCancelledException("cancelled");
        assertSame(cancelled, ExternalFailures.classify(new IOException("read failed", cancelled)));
        assertSame(cancelled, ExternalFailures.surface(new UncheckedIOException(new IOException("read failed", cancelled)), "ctx"));
        CircuitBreakingException tripped = new CircuitBreakingException("too big", CircuitBreaker.Durability.TRANSIENT);
        assertSame(tripped, ExternalFailures.classify(new IllegalStateException("wrapped", tripped)));
    }

    public void testNetworkFailuresNamingAnEndpointAreNotForwarded() {
        RuntimeException result = ExternalFailures.classify(
            new IOException("Unable to resolve host minio.internal", new java.net.UnknownHostException("minio.internal"))
        );
        assertThat(result.getMessage(), not(containsString("minio.internal")));
        result = ExternalFailures.classify(new java.net.ConnectException("Connect to minio.internal:9000 failed"));
        assertThat(result.getMessage(), not(containsString("minio.internal")));
    }
}
