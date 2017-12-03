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


import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.threadpool.ThreadPool;

public interface ElasticsearchClient {

    /**
     * Executes a generic action, denoted by an {@link org.elasticsearch.action.Action}.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param <Request>        The request type.
     * @param <Response>       the response type.
     * @param <RequestBuilder> The request builder type.
     * @return A future allowing to get back the response.
     */
    <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(
            Action<Request, Response, RequestBuilder> action, Request request);

    /**
     * Executes a generic action, denoted by an {@link Action}.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param listener         The listener to receive the response back.
     * @param <Request>        The request type.
     * @param <Response>       The response type.
     * @param <RequestBuilder> The request builder type.
     */
    <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(
            Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener);

    /**
     * Prepares a request builder to execute, specified by {@link Action}.
     *
     * @param action           The action type to execute.
     * @param <Request>        The request type.
     * @param <Response>       The response type.
     * @param <RequestBuilder> The request builder.
     * @return The request builder, that can, at a later stage, execute the request.
     */
    <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(
            Action<Request, Response, RequestBuilder> action);

    /**
     * Returns the threadpool used to execute requests on this client
     */
    ThreadPool threadPool();

}
