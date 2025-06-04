/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChunkedTrainedModelRestorerTests extends ESTestCase {
    public void testRetryingSearch_ReturnsSearchResults() throws InterruptedException {
        var mockClient = mock(Client.class);
        var mockSearchResponse = mock(SearchResponse.class, RETURNS_DEEP_STUBS);

        PlainActionFuture<SearchResponse> searchFuture = new PlainActionFuture<>();
        searchFuture.onResponse(mockSearchResponse);
        when(mockClient.search(any())).thenReturn(searchFuture);

        var request = createSearchRequest();

        assertThat(
            ChunkedTrainedModelRestorer.retryingSearch(mockClient, "", request, 5, new TimeValue(1, TimeUnit.NANOSECONDS)),
            is(mockSearchResponse)
        );

        verify(mockClient, times(1)).search(any());
    }

    public void testRetryingSearch_ThrowsSearchPhaseExceptionWithNoRetries() {
        final var mockClient = mock(Client.class);
        var searchPhaseException = new SearchPhaseExecutionException("phase", "error", ShardSearchFailure.EMPTY_ARRAY);
        when(mockClient.search(any())).thenThrow(searchPhaseException);

        var request = createSearchRequest();

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> ChunkedTrainedModelRestorer.retryingSearch(mockClient, "1", request, 0, new TimeValue(1, TimeUnit.NANOSECONDS))
        );

        assertThat(exception.getCause(), is(searchPhaseException));
        assertThat(
            exception.getMessage(),
            is(
                "loading model [1] failed after [0] retries. The deployment is now in a failed state, the error may be "
                    + "transient please stop the deployment and restart"
            )
        );
        verify(mockClient, times(1)).search(any());
    }

    public void testRetryingSearch_ThrowsSearchPhaseExceptionAfterOneRetry() {
        final var mockClient = mock(Client.class);
        var searchPhaseException = new SearchPhaseExecutionException("phase", "error", ShardSearchFailure.EMPTY_ARRAY);
        when(mockClient.search(any())).thenThrow(searchPhaseException);

        var request = createSearchRequest();

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> ChunkedTrainedModelRestorer.retryingSearch(mockClient, "", request, 1, new TimeValue(1, TimeUnit.NANOSECONDS))
        );

        assertThat(exception.getCause(), is(searchPhaseException));
        verify(mockClient, times(2)).search(any());
    }

    public void testRetryingSearch_ThrowsCircuitBreakingExceptionAfterOneRetry_FromSearchPhaseException() {
        final var mockClient = mock(Client.class);
        var searchPhaseException = new SearchPhaseExecutionException("phase", "error", ShardSearchFailure.EMPTY_ARRAY);
        var circuitBreakerException = new CircuitBreakingException("error", CircuitBreaker.Durability.TRANSIENT);
        when(mockClient.search(any())).thenThrow(searchPhaseException).thenThrow(circuitBreakerException);

        var request = createSearchRequest();

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> ChunkedTrainedModelRestorer.retryingSearch(mockClient, "", request, 1, new TimeValue(1, TimeUnit.NANOSECONDS))
        );

        assertThat(exception.getCause(), is(circuitBreakerException));
        verify(mockClient, times(2)).search(any());
    }

    public void testRetryingSearch_EnsureExceptionCannotBeUnwrapped() {
        final var mockClient = mock(Client.class);
        var searchPhaseExecutionException = new SearchPhaseExecutionException("phase", "error", ShardSearchFailure.EMPTY_ARRAY);
        when(mockClient.search(any())).thenThrow(searchPhaseExecutionException);

        var request = createSearchRequest();

        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> ChunkedTrainedModelRestorer.retryingSearch(mockClient, "", request, 1, new TimeValue(1, TimeUnit.NANOSECONDS))
        );

        assertThat(ExceptionsHelper.unwrapCause(exception), is(exception));
        assertThat(ExceptionsHelper.unwrapCause(exception), instanceOf(ElasticsearchException.class));
        verify(mockClient, times(2)).search(any());
    }

    public void testRetryingSearch_ThrowsIllegalArgumentExceptionIgnoringRetries() {
        final var mockClient = mock(Client.class);
        var exception = new IllegalArgumentException("Error");
        when(mockClient.search(any())).thenThrow(exception);

        var request = createSearchRequest();

        IllegalArgumentException thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> ChunkedTrainedModelRestorer.retryingSearch(mockClient, "", request, 1, new TimeValue(1, TimeUnit.NANOSECONDS))
        );

        assertThat(thrownException, is(exception));
        verify(mockClient, times(1)).search(any());
    }

    public void testRetryingSearch_ThrowsSearchPhaseExceptionOnce_ThenReturnsResponse() throws InterruptedException {
        final var mockClient = mock(Client.class);
        var mockSearchResponse = mock(SearchResponse.class, RETURNS_DEEP_STUBS);

        PlainActionFuture<SearchResponse> searchFuture = new PlainActionFuture<>();
        searchFuture.onResponse(mockSearchResponse);

        var searchPhaseException = new SearchPhaseExecutionException("phase", "error", ShardSearchFailure.EMPTY_ARRAY);
        when(mockClient.search(any())).thenThrow(searchPhaseException).thenReturn(searchFuture);

        var request = createSearchRequest();

        assertThat(
            ChunkedTrainedModelRestorer.retryingSearch(mockClient, "", request, 1, new TimeValue(1, TimeUnit.NANOSECONDS)),
            is(mockSearchResponse)
        );

        verify(mockClient, times(2)).search(any());
    }

    private static SearchRequest createSearchRequest() {
        return new SearchRequest("index");
    }
}
