/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
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
            assertSame(io, classified.getCause());
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        }
    }

    public void testInflaterPrematureEofIsMalformedInput() {
        EOFException inflater = new EOFException("Unexpected end of ZLIB input stream");
        RuntimeException classified = ExternalFailures.classify(inflater);
        assertThat(classified, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertSame(inflater, classified.getCause());
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
        assertSame(expired, ExternalFailures.classify(expired));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(expired)));
        assertSame(expired, ExternalFailures.surface(expired, "ctx"));
        assertNotNull(ExceptionsHelper.unwrap(new ExecutionException(expired), ExternalCredentialsExpiredException.class));
        assertNull(ExceptionsHelper.unwrap(new IOException("HTTP 400 ExpiredToken"), ExternalCredentialsExpiredException.class));
    }

    public void testObjectChangedPassesThroughAs503() {
        var changed = new ExternalObjectChangedException("Object changed during read of [k]");
        assertSame(changed, ExternalFailures.classify(changed));
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(ExternalFailures.classify(changed)));
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
        IOException ioe = new IOException("record exceeded external_max_record_size");
        RuntimeException surfaced = ExternalFailures.surface(ioe, "Streaming parallel parsing failed");
        assertThat(surfaced, org.hamcrest.Matchers.instanceOf(ExternalClientException.class));
        assertSame(ioe, surfaced.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(surfaced));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("Streaming parallel parsing failed"));
        assertThat(surfaced.getMessage(), org.hamcrest.Matchers.containsString("record exceeded external_max_record_size"));
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

    public void testLocateOmitsThePrefixWhenTheDetailAlreadyNamesTheLocation() {
        String location = "s3://bucket/data/good.csv";
        assertEquals(
            "Object not found: s3://bucket/data/good.csv",
            ExternalFailures.locate("Failed to resolve external source", location, "Object not found: " + location)
        );
    }

    public void testLocateAddsThePrefixWhenTheDetailDoesNotNameTheLocation() {
        assertEquals(
            "Failed to resolve external source [s3://bucket/data/good.csv]: CSV file has no schema line",
            ExternalFailures.locate("Failed to resolve external source", "s3://bucket/data/good.csv", "CSV file has no schema line")
        );
    }

    public void testLocateHandlesAMessagelessFailure() {
        // EsRejectedExecutionException has a no-argument constructor, and the rejection arm passes getMessage()
        // straight into locate -- so a null detail is reachable, not hypothetical.
        assertEquals(
            "Failed to resolve external source [s3://bucket/data/good.csv]",
            ExternalFailures.locate("Failed to resolve external source", "s3://bucket/data/good.csv", null)
        );
    }

    public void testLocateRedactsAPreSignedUrlNamedInTheDetail() {
        // The redacted form is a prefix of the raw one here, so a detail naming the raw URL must still be redacted.
        String location = "https://bkt.s3.eu-west-1.amazonaws.com/w/x.parquet?X-Amz-Signature=deadbeef";
        String message = ExternalFailures.locate("Failed to resolve metadata for", location, "File does not exist: " + location);
        assertEquals("File does not exist: https://bkt.s3.eu-west-1.amazonaws.com/w/x.parquet", message);
    }

    public void testLocateRedactsUserInfoNamedInTheDetail() {
        String location = "https://u:p@bkt.example.com/w/x.parquet?X-Amz-Signature=deadbeef";
        String message = ExternalFailures.locate("Failed to resolve metadata for", location, "File does not exist: " + location);
        assertEquals("File does not exist: https://bkt.example.com/w/x.parquet", message);
    }

    public void testLocateRedactsTheLocationItAdds() {
        String location = "https://u:p@bkt.example.com/w/x.parquet?X-Amz-Signature=deadbeef";
        assertEquals(
            "Failed to resolve metadata for [https://bkt.example.com/w/x.parquet]: CSV file has no schema line",
            ExternalFailures.locate("Failed to resolve metadata for", location, "CSV file has no schema line")
        );
        assertEquals(
            "Failed to resolve metadata for [https://bkt.example.com/w/x.parquet]",
            ExternalFailures.locate("Failed to resolve metadata for", location, null)
        );
    }

    public void testLocateLeavesANonHttpLocationUnchanged() {
        String location = "wasbs://container@account.blob.core.windows.net/w/x.parquet?snapshot=1";
        assertEquals(
            "File does not exist: " + location,
            ExternalFailures.locate("Failed to resolve metadata for", location, "File does not exist: " + location)
        );
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

}
