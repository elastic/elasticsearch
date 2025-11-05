/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;

/**
 * Interface for request builders that provide a fluent API for constructing and executing action requests.
 * Request builders offer a convenient way to build action requests with a chainable method syntax and
 * execute them in various ways (synchronously, asynchronously, or with a future).
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Using a request builder for a search operation
 * SearchRequestBuilder builder = client.prepareSearch("myindex");
 * builder.setQuery(QueryBuilders.matchQuery("field", "value"))
 *        .setSize(10)
 *        .setFrom(0);
 *
 * // Execute synchronously
 * SearchResponse response = builder.get();
 *
 * // Execute asynchronously with a listener
 * builder.execute(ActionListener.wrap(
 *     response -> {
 *         // Handle successful response
 *         System.out.println("Found " + response.getHits().getTotalHits() + " hits");
 *     },
 *     e -> {
 *         // Handle failure
 *         System.err.println("Search failed: " + e.getMessage());
 *     }
 * ));
 *
 * // Execute with a future
 * ActionFuture<SearchResponse> future = builder.execute();
 * SearchResponse response = future.actionGet(TimeValue.timeValueSeconds(30));
 * }</pre>
 *
 * @param <Request> the type of request this builder constructs
 * @param <Response> the type of response produced by executing the request
 */
public interface RequestBuilder<Request, Response extends RefCounted> {
    /**
     * Returns the request that this builder constructs. Depending on the implementation,
     * this might return a new request instance with each call or the same request instance.
     *
     * <p>The returned request reflects all the configuration that has been applied to the
     * builder up to this point.
     *
     * @return the request object built by this builder
     */
    Request request();

    /**
     * Executes the request asynchronously and returns a future that can be used to retrieve
     * the response. The future allows the caller to wait for the result or check if it's ready.
     *
     * @return an {@link ActionFuture} that will be completed with the response or an exception
     */
    ActionFuture<Response> execute();

    /**
     * Executes the request synchronously and returns the response, blocking until the operation
     * completes. This is a convenience method equivalent to {@code execute().actionGet()}.
     *
     * @return the response from executing the request
     * @throws RuntimeException if the execution fails
     */
    Response get();

    /**
     * Executes the request synchronously with a timeout and returns the response, blocking until
     * the operation completes or the timeout expires. This is a convenience method equivalent to
     * {@code execute().actionGet(timeout)}.
     *
     * @param timeout the maximum time to wait for the response
     * @return the response from executing the request
     * @throws java.util.concurrent.TimeoutException if the timeout expires
     * @throws RuntimeException if the execution fails
     */
    Response get(TimeValue timeout);

    /**
     * Executes the request asynchronously and invokes the provided listener when the operation
     * completes. The listener's {@link ActionListener#onResponse} method is called on success,
     * or {@link ActionListener#onFailure} is called if an error occurs.
     *
     * @param listener the listener to notify when the operation completes
     */
    void execute(ActionListener<Response> listener);
}
