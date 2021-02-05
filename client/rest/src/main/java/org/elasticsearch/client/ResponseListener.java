/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

/**
 * Listener to be provided when calling async performRequest methods provided by {@link RestClient}.
 * Those methods that do accept a listener will return immediately, execute asynchronously, and notify
 * the listener whenever the request yielded a response, or failed with an exception.
 *
 * <p>
 * Note that it is <strong>not</strong> safe to call {@link RestClient#close()} from either of these
 * callbacks.
 */
public interface ResponseListener {

    /**
     * Method invoked if the request yielded a successful response
     */
    void onSuccess(Response response);

    /**
     * Method invoked if the request failed. There are two main categories of failures: connection failures (usually
     * {@link java.io.IOException}s, or responses that were treated as errors based on their error response code
     * ({@link ResponseException}s).
     */
    void onFailure(Exception exception);
}
