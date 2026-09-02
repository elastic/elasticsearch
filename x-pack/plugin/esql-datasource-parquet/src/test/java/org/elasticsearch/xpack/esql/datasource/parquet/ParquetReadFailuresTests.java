/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.datasources.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ParquetReadFailuresTests extends ESTestCase {

    public void testIoExceptionBecomesExternalClient400() {
        IOException io = new IOException("truncated");
        RuntimeException wrapped = ParquetReadFailures.wrap(io, "Failed to read column [id]");
        assertThat(wrapped, instanceOf(ExternalClientException.class));
        assertSame(io, wrapped.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(wrapped));
        assertSame(wrapped, ExternalFailures.classify(wrapped));
    }

    public void testUncheckedIoExceptionBecomesExternalClient400() {
        IOException cause = new IOException("upstream");
        UncheckedIOException uioe = new UncheckedIOException("wrapped", cause);
        RuntimeException wrapped = ParquetReadFailures.wrap(uioe, "Failed to parse page header");
        assertThat(wrapped, instanceOf(ExternalClientException.class));
        assertSame(uioe, wrapped.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(wrapped));
        assertSame(wrapped, ExternalFailures.classify(wrapped));
    }

    public void testExternalUnavailableIdentityAnd503() {
        ExternalUnavailableException unavailable = new ExternalUnavailableException("store 503", new IOException());
        RuntimeException wrapped = ParquetReadFailures.wrap(unavailable, "ctx");
        assertSame(unavailable, wrapped);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(ExternalFailures.classify(wrapped)));
    }

    public void testCircuitBreakingIdentity() {
        CircuitBreakingException breaking = new CircuitBreakingException("over", 10, 5, CircuitBreaker.Durability.TRANSIENT);
        assertSame(breaking, ParquetReadFailures.wrap(breaking, "ctx"));
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(ExternalFailures.classify(breaking)));
    }

    public void testCompletionExceptionUnwrapsExternalUnavailableTo503() {
        ExternalUnavailableException unavailable = new ExternalUnavailableException("store 503", new IOException());
        RuntimeException wrapped = ParquetReadFailures.wrap(new CompletionException(unavailable), "Phase 2 prefetch failed");
        assertSame(unavailable, wrapped);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(ExternalFailures.classify(wrapped)));
    }

    public void testExecutionExceptionUnwrapsIoExceptionTo400() {
        IOException io = new IOException("truncated");
        RuntimeException wrapped = ParquetReadFailures.wrap(new ExecutionException(io), "Failed to read column");
        assertThat(wrapped, instanceOf(ExternalClientException.class));
        assertSame(io, wrapped.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(wrapped));
    }

    public void testIllegalStateExceptionIsNotIaeAndClassifies500() {
        IllegalStateException ise = new IllegalStateException("broken invariant");
        RuntimeException wrapped = ParquetReadFailures.wrap(ise, "ctx");
        assertSame(ise, wrapped);
        assertFalse(wrapped instanceof IllegalArgumentException);
        RuntimeException classified = ExternalFailures.classify(wrapped);
        assertThat(classified, instanceOf(ExternalServerException.class));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(classified));
    }

    public void testIaeKeepsContext400() {
        IllegalArgumentException iae = new IllegalArgumentException("bad page");
        RuntimeException wrapped = ParquetReadFailures.wrap(iae, "ctx");
        assertThat(wrapped, instanceOf(IllegalArgumentException.class));
        assertSame(iae, wrapped.getCause());
        assertThat(wrapped.getMessage(), containsString("ctx"));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(wrapped)));
    }

    public void testInvalidArgumentExceptionIdentity400() {
        InvalidArgumentException invalid = new InvalidArgumentException("cannot coerce");
        RuntimeException wrapped = ParquetReadFailures.wrap(invalid, "ctx");
        assertSame(invalid, wrapped);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(wrapped)));
    }

    public void testErrorIsRethrown() {
        AssertionError error = new AssertionError("boom");
        AssertionError thrown = expectThrows(AssertionError.class, () -> ParquetReadFailures.wrap(error, "ctx"));
        assertSame(error, thrown);
    }

    public void testErrorBuriedUnderCompletionExceptionIsRethrown() {
        AssertionError error = new AssertionError("boom");
        AssertionError thrown = expectThrows(AssertionError.class, () -> ParquetReadFailures.wrap(new CompletionException(error), "ctx"));
        assertSame(error, thrown);
    }
}
