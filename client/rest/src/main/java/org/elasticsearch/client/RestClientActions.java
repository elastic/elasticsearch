/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

/**
 * Actions that can be taken on a {@link RestClient} or the stateless views
 * returned by {@link RestClientActions#withHostSelector withHostSelector}.
 */
public interface RestClientActions {
    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, Header...)} but without parameters
     * and request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    Response performRequest(String method, String endpoint, Header... headers) throws IOException;

    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, Header...)} but without request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    Response performRequest(String method, String endpoint, Map<String, String> params, Header... headers) throws IOException;

    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, HttpAsyncResponseConsumerFactory, Header...)}
     * which doesn't require specifying an {@link HttpAsyncResponseConsumerFactory} instance,
     * {@link HttpAsyncResponseConsumerFactory} will be used to create the needed instances of {@link HttpAsyncResponseConsumer}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, Header... headers) throws IOException;

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Blocks until the request is completed and returns
     * its response or fails by throwing an exception. Selects a host out of the provided ones in a round-robin fashion. Failing hosts
     * are marked dead and retried after a certain amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times
     * they previously failed (the more failures, the later they will be retried). In case of failures all of the alive nodes (or dead
     * nodes that deserve a retry) are retried until one responds or none of them does, in which case an {@link IOException} will be thrown.
     *
     * This method works by performing an asynchronous call and waiting
     * for the result. If the asynchronous call throws an exception we wrap
     * it and rethrow it so that the stack trace attached to the exception
     * contains the call site. While we attempt to preserve the original
     * exception this isn't always possible and likely haven't covered all of
     * the cases. You can get the original exception from
     * {@link Exception#getCause()}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param httpAsyncResponseConsumerFactory the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the response body gets streamed from a non-blocking HTTP
     * connection on the client side.
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                   Header... headers) throws IOException;

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure. Shortcut to
     * {@link #performRequestAsync(String, String, Map, HttpEntity, ResponseListener, Header...)} but without parameters and  request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     */
    void performRequestAsync(String method, String endpoint, ResponseListener responseListener, Header... headers);

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure. Shortcut to
     * {@link #performRequestAsync(String, String, Map, HttpEntity, ResponseListener, Header...)} but without request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     */
    void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    ResponseListener responseListener, Header... headers);

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure.
     * Shortcut to {@link #performRequestAsync(String, String, Map, HttpEntity, HttpAsyncResponseConsumerFactory, ResponseListener,
     * Header...)} which doesn't require specifying an {@link HttpAsyncResponseConsumerFactory} instance,
     * {@link HttpAsyncResponseConsumerFactory} will be used to create the needed instances of {@link HttpAsyncResponseConsumer}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     */
    void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    HttpEntity entity, ResponseListener responseListener, Header... headers);

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. The request is executed asynchronously
     * and the provided {@link ResponseListener} gets notified upon request completion or failure.
     * Selects a host out of the provided ones in a round-robin fashion. Failing hosts are marked dead and retried after a certain
     * amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously failed (the more failures,
     * the later they will be retried). In case of failures all of the alive nodes (or dead nodes that deserve a retry) are retried
     * until one responds or none of them does, in which case an {@link IOException} will be thrown.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param httpAsyncResponseConsumerFactory the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the response body gets streamed from a non-blocking HTTP
     * connection on the client side.
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     */
    void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                    ResponseListener responseListener, Header... headers);

    /**
     * Create a client that only runs requests on selected hosts. This client
     * has no state of its own and backs everything to the {@link RestClient}
     * that created it.
     */
    RestClientActions withHostSelector(HostSelector hostSelector);
}
