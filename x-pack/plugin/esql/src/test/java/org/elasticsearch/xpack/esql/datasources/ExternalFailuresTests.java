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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

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

    public void testIoExceptionBuriedUnderRuntimeWrapperBecomesClientException() {
        // Reproduces the parallel-parsing path: a worker read IOException rewrapped in a RuntimeException.
        // Keyed on the top-level type alone this is a 500; the cause-chain backstop must surface it as 400.
        var ioe = new IOException("record exceeded max_record_size [67108864] ... for format [tsv]");
        var wrapped = new RuntimeException("Streaming parallel parsing failed", ioe);
        RuntimeException classified = ExternalFailures.classify(wrapped);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        // The full wrapper chain is preserved as the cause, but the actionable message comes from the IOException.
        assertSame(wrapped, classified.getCause());
        assertThat(classified.getMessage(), org.hamcrest.Matchers.containsString("max_record_size"));
    }

    public void testDeeplyNestedIoExceptionIsFound() {
        // The data-class cause can sit several wrappers down, including under a bug-class type. The first
        // data-class cause in the chain still drives the 400 — wrapping never escalates a data error to 500.
        for (Throwable buried : new Throwable[] {
            new RuntimeException("a", new IllegalStateException("b", new IOException("truncated"))),
            new RuntimeException("outer", new EOFException("eof")),                       // IOException subclass
            new RuntimeException("outer", new UncheckedIOException(new IOException("x"))), // already-unchecked IO
            new IllegalStateException("wrap", new IOException("nested")) }) {
            RuntimeException classified = ExternalFailures.classify(buried);
            assertThat("expected 400 for " + buried, classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        }
    }

    public void testNonIoCausesStayServerException() {
        // The backstop must not over-trigger: a bug-class throwable whose chain holds no data-class cause is
        // still our fault (500). Guards against the cause-chain unwrap silently downgrading real bugs.
        for (Throwable bug : new Throwable[] {
            new RuntimeException("outer", new IllegalStateException("inner bug")),
            new IllegalStateException("broken", new NullPointerException()) }) {
            RuntimeException classified = ExternalFailures.classify(bug);
            assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
            assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classified));
        }
    }

    public void testExplicitServerExceptionWrappingIoStays500() {
        // An intentional ExternalServerException (a broken invariant in our own reader) keeps its 500 even
        // when it wraps an IOException: the ElasticsearchException branch wins before the backstop runs, so
        // the unwrap never downgrades a deliberately-classified server fault.
        var server = new ExternalServerException(new IOException("io detail"), "invariant violated");
        var classified = ExternalFailures.classify(server);
        assertSame(server, classified);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classified));
    }

    public void testCycleInCauseChainDoesNotHang() {
        // A self-referential cause chain (pathological, but possible) must terminate, not spin forever.
        var a = new RuntimeException("a");
        var b = new RuntimeException("b", a);
        a.initCause(b);
        var classified = ExternalFailures.classify(a);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalServerException.class));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classified));
    }

    public void testAsUncheckedPreservesIoExceptionAs400() {
        // The throw-site rule: an IOException becomes an UncheckedIOException so classify still sees a 400.
        var ioe = new IOException("record exceeded max_record_size");
        RuntimeException out = ExternalFailures.asUnchecked(ioe, "parsing failed");
        assertThat(out, org.hamcrest.Matchers.instanceOf(UncheckedIOException.class));
        assertSame(ioe, out.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(out)));
    }

    public void testAsUncheckedReturnsRuntimeExceptionUnchanged() {
        var re = new IllegalStateException("boom");
        assertSame(re, ExternalFailures.asUnchecked(re, "parsing failed"));
    }

    public void testAsUncheckedWrapsOtherCheckedWithFallbackMessage() {
        var checked = new Exception("checked, non-IO");
        RuntimeException out = ExternalFailures.asUnchecked(checked, "parsing failed");
        assertEquals("parsing failed", out.getMessage());
        assertSame(checked, out.getCause());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(ExternalFailures.classify(out)));
    }

    public void testUnwrapAsyncPeelsExecutionAndCompletionWrappers() {
        var ioe = new IOException("io");
        assertSame(ioe, ExternalFailures.unwrapAsync(new ExecutionException(ioe)));
        assertSame(ioe, ExternalFailures.unwrapAsync(new CompletionException(ioe)));
        assertSame(ioe, ExternalFailures.unwrapAsync(new CompletionException(new ExecutionException(ioe))));
        // A non-wrapper is returned as-is; a wrapper without a cause is too (nothing to peel).
        var iae = new IllegalArgumentException("x");
        assertSame(iae, ExternalFailures.unwrapAsync(iae));
        var causeless = new ExecutionException("no cause", null);
        assertSame(causeless, ExternalFailures.unwrapAsync(causeless));
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
}
