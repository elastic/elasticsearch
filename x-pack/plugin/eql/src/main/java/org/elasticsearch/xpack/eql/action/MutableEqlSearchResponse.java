/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.eql.action.AsyncEqlSearchResponse;
import org.elasticsearch.xpack.core.eql.action.EqlSearchResponse;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.async.AsyncTaskIndexService.restoreResponseHeadersContext;

/**
 * A mutable search response that allows to update and create partial response synchronously.
 * Synchronized methods ensure that updates of the content are blocked if another thread is
 * creating an async response concurrently. This limits the number of final reduction that can
 * run concurrently to 1 and ensures that we pause the search progress when an {@link AsyncEqlSearchResponse} is built.
 */
class MutableEqlSearchResponse {
    private final ThreadContext threadContext;

    private ElasticsearchException failure;
    private Map<String, List<String>> responseHeaders;
    private EqlSearchResponse response;
    private boolean isPartial;
    private boolean frozen;

    /**
     * Creates a new mutable search response.
     *
     * @param threadContext The thread context to retrieve the final response headers.
     */
    MutableEqlSearchResponse(ThreadContext threadContext) {
        this.threadContext = threadContext;
        this.isPartial = true;
    }

    /**
     * Updates the response with the partial {@link SearchResponseSections} merged from #<code>successfulShards</code>
     * shards.
     */
    synchronized void updatePartialResponse(EqlSearchResponse response) {
        failIfFrozen();
        this.response.merge(response);
        this.isPartial = true;
    }

    /**
     * Updates the response with the final {@link SearchResponseSections} merged from #<code>successfulShards</code>
     * shards.
     */
    synchronized void updateFinalResponse(EqlSearchResponse response) {
        failIfFrozen();
        // copy the response headers from the current context
        this.responseHeaders = threadContext.getResponseHeaders();
        this.isPartial = false;
        this.frozen = true;
        this.response = response;
    }

    /**
     * Updates the response with a fatal failure. This method preserves the partial response
     * received from previous updates
     */
    synchronized void updateWithFailure(Exception exc) {
        failIfFrozen();
        // copy the response headers from the current context
        this.responseHeaders = threadContext.getResponseHeaders();
        this.failure = ElasticsearchException.guessRootCauses(exc)[0];
        this.frozen = true;
    }

    /**
     * Creates an {@link AsyncEqlSearchResponse} based on the current state of the mutable response.
     * The final reduce of the aggregations is executed if needed (partial response).
     * This method is synchronized to ensure that we don't perform final reduces concurrently.
     */
    synchronized AsyncEqlSearchResponse toAsyncEqlSearchResponse(AsyncEqlSearchTask task, long expirationTime) {
        return new AsyncEqlSearchResponse(task.getExecutionId().getEncoded(), response, failure, isPartial,
            frozen == false, task.getStartTime(), expirationTime);
    }

    /**
     * Creates an {@link AsyncEqlSearchResponse} based on the current state of the mutable response.
     * This method also restores the response headers in the current thread context if the final response is available.
     */
    synchronized AsyncEqlSearchResponse toAsyncEqlSearchResponseWithHeaders(AsyncEqlSearchTask task, long expirationTime) {
        AsyncEqlSearchResponse resp = toAsyncEqlSearchResponse(task, expirationTime);
        if (responseHeaders != null) {
            restoreResponseHeadersContext(threadContext, responseHeaders);
        }
        return resp;
    }


    private void failIfFrozen() {
        if (frozen) {
            throw new IllegalStateException("invalid update received after the completion of the request");
        }
    }
}
