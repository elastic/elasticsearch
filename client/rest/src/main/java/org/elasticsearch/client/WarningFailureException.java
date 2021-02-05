/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import java.io.IOException;

import static org.elasticsearch.client.ResponseException.buildMessage;

/**
 * This exception is used to indicate that one or more {@link Response#getWarnings()} exist
 * and is typically used when the {@link RestClient} is set to fail by setting
 * {@link RestClientBuilder#setStrictDeprecationMode(boolean)} to `true`.
 */
// This class extends RuntimeException in order to deal with wrapping that is done in FutureUtils on exception.
// if the exception is not of type ElasticsearchException or RuntimeException it will be wrapped in a UncategorizedExecutionException
public final class WarningFailureException extends RuntimeException {

    private final Response response;

    public WarningFailureException(Response response) throws IOException {
        super(buildMessage(response));
        this.response = response;
    }

    /**
     * Wrap a {@linkplain WarningFailureException} with another one with the current
     * stack trace. This is used during synchronous calls so that the caller
     * ends up in the stack trace of the exception thrown.
     */
    WarningFailureException(WarningFailureException e) {
        super(e.getMessage(), e);
        this.response = e.getResponse();
    }

    /**
     * Returns the {@link Response} that caused this exception to be thrown.
     */
    public Response getResponse() {
        return response;
    }
}
