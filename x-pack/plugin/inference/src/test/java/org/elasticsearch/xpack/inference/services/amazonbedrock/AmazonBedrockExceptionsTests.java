/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class AmazonBedrockExceptionsTests extends ESTestCase {

    private static final String MESSAGE = "message";

    public void testFromBedrockException_BadRequest() {
        var cause = bedrockException(400);

        var result = AmazonBedrockExceptions.fromBedrockException(MESSAGE, cause);

        assertThat(result.status(), is(RestStatus.BAD_REQUEST));
        assertThat(result.getMessage(), is(MESSAGE));
        assertThat(result.getCause(), sameInstance(cause));
    }

    public void testFromBedrockException_TooManyRequests() {
        assertThat(AmazonBedrockExceptions.fromBedrockException(MESSAGE, bedrockException(429)).status(), is(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testFromBedrockException_ServerError() {
        assertThat(
            AmazonBedrockExceptions.fromBedrockException(MESSAGE, bedrockException(503)).status(),
            is(RestStatus.SERVICE_UNAVAILABLE)
        );
    }

    public void testFromBedrockException_UnknownStatusCodeFallsBackToInternalServerError() {
        assertThat(
            AmazonBedrockExceptions.fromBedrockException(MESSAGE, bedrockException(299)).status(),
            is(RestStatus.INTERNAL_SERVER_ERROR)
        );
    }

    public void testWrap_BedrockException() {
        var cause = bedrockException(400);

        var result = AmazonBedrockExceptions.wrap(MESSAGE, cause);

        assertThat(result, instanceOf(ElasticsearchStatusException.class));
        assertThat(result.status(), is(RestStatus.BAD_REQUEST));
        assertThat(result.getCause(), sameInstance(cause));
    }

    public void testWrap_UnwrapsCompletionAndExecutionExceptions() {
        var cause = bedrockException(400);
        var wrapped = new CompletionException(new ExecutionException(cause));

        var result = AmazonBedrockExceptions.wrap(MESSAGE, wrapped);

        assertThat(result.status(), is(RestStatus.BAD_REQUEST));
        assertThat(result.getCause(), sameInstance(cause));
    }

    public void testWrap_BedrockExceptionNestedUnderElasticsearchException() {
        var cause = bedrockException(403);
        var wrapped = new ElasticsearchException("outer", new CompletionException(cause));

        var result = AmazonBedrockExceptions.wrap(MESSAGE, wrapped);

        assertThat(result.status(), is(RestStatus.FORBIDDEN));
        assertThat(result.getCause(), sameInstance(cause));
    }

    public void testWrap_ReturnsExistingStatusExceptionUnchanged() {
        var existing = new ElasticsearchStatusException("already has a status", RestStatus.NOT_FOUND);

        assertThat(AmazonBedrockExceptions.wrap(MESSAGE, existing), sameInstance(existing));
    }

    public void testWrap_ReturnsStatusExceptionNestedUnderOtherExceptionsUnchanged() {
        var existing = new UnifiedChatCompletionException(RestStatus.BAD_REQUEST, "unified", "type", "code");
        var wrapped = new ElasticsearchException("outer", new CompletionException(existing));

        assertThat(AmazonBedrockExceptions.wrap(MESSAGE, wrapped), sameInstance(existing));
    }

    public void testWrap_StatusExceptionWinsOverBedrockCause() {
        var existing = new ElasticsearchStatusException("already has a status", RestStatus.NOT_FOUND, bedrockException(400));

        assertThat(AmazonBedrockExceptions.wrap(MESSAGE, existing), sameInstance(existing));
    }

    public void testWrap_NonBedrockExceptionIsInternalServerError() {
        var cause = new IllegalStateException("boom");

        var result = AmazonBedrockExceptions.wrap(MESSAGE, cause);

        assertThat(result, not(instanceOf(ElasticsearchStatusException.class)));
        assertThat(result.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(result.getMessage(), is(MESSAGE));
        assertThat(result.getCause(), sameInstance(cause));
    }

    private static BedrockRuntimeException bedrockException(int statusCode) {
        return (BedrockRuntimeException) BedrockRuntimeException.builder().message("bedrock failure").statusCode(statusCode).build();
    }
}
