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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;

public class ExternalFailuresTests extends ESTestCase {

    public void testErrorIsRethrown() {
        AssertionError error = new AssertionError("boom");
        AssertionError thrown = expectThrows(AssertionError.class, () -> ExternalFailures.classify(error));
        assertSame(error, thrown);
    }

    public void testExternalExceptionsPassThroughWithTheirStatus() {
        var client = new ExternalClientException("bad file", new IOException("truncated"));
        assertSame(client, ExternalFailures.classify(client));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(client)));

        var server = new ExternalServerException("invariant violated", new IllegalStateException());
        assertSame(server, ExternalFailures.classify(server));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(ExternalFailures.classify(server)));

        var unavailable = new ExternalUnavailableException("store 503", new IOException());
        assertSame(unavailable, ExternalFailures.classify(unavailable));
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(ExternalFailures.classify(unavailable)));
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
        // A saturated thread pool or the storage concurrency guardrail can reject work as an
        // EsRejectedExecutionException. That is load-shed backpressure (429), not a broken invariant in our
        // reading code (500): classify must return it unchanged so its self-carried 429 survives, rather than
        // wrapping it as an ExternalServerException.
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
        assertSame(iae, ExternalFailures.classify(iae));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(iae)));
    }

    public void testIoErrorsBecomeClientException() {
        for (Throwable io : new Throwable[] {
            new IOException("read failed"),
            new EOFException("truncated"),
            new UncheckedIOException(new IOException("wrapped")) }) {
            RuntimeException classified = ExternalFailures.classify(io);
            assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
            assertSame(io, classified.getCause());
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
            assertSame(bug, classified.getCause());
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
        assertSame(ioe, surfaced.getCause());
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
        assertSame(uioe, surfaced.getCause());
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
        assertSame(surfaced, classified);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classified));
    }

    public void testSurfaceWrapsCheckedNonIoAsExternalServer() {
        // Bare InterruptedException stored after a worker thread was interrupted is the canonical case here:
        // we have no evidence of bad input, so the bug stays visible as a 500 with the context prefix.
        InterruptedException interrupted = new InterruptedException("worker interrupted");
        RuntimeException surfaced = ExternalFailures.surface(interrupted, "Parallel parsing failed");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertSame(interrupted, surfaced.getCause());
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
        assertSame("classify must pass an already-typed surface() result through unchanged", surfaced, classified);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
    }
}
