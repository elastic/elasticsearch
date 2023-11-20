/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.util.List;

/**
 * A contract for clients to specify behavior for handling http responses. Clients can pass this contract to the retry sender to parse
 * the response and help with logging.
 */
public interface ResponseHandler {

    /**
     * A method for checking the response from the 3rd party service. This could check the status code and that the response body
     * is in the correct form.
     *
     * @param throttlerManager a throttler for the logs
     * @param logger the logger to use for logging
     * @param request the original request
     * @param result the response from the server
     * @throws RetryException if the response is invalid
     */
    void validateResponse(ThrottlerManager throttlerManager, Logger logger, HttpRequestBase request, HttpResult result)
        throws RetryException;

    /**
     * A method for parsing the response from the server.
     * @param result The wrapped response from the server.
     * @return the parsed inference results
     * @throws RetryException if a parsing error occurs
     */
    List<? extends InferenceResults> parseResult(HttpResult result) throws RetryException;

    /**
     * A string to uniquely identify the type of request that is being handled. This allows loggers to clarify which type of request
     * might have failed.
     *
     * @return a {@link String} indicating the request type that was sent (e.g. elser, elser hugging face etc)
     */
    String getRequestType();
}
