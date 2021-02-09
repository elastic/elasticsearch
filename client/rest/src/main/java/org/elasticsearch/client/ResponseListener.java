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
