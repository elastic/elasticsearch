/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
