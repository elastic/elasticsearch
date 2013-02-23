/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.client.internal.InternalGenericClient;

/**
 */
public abstract class BroadcastOperationRequestBuilder<Request extends BroadcastOperationRequest<Request>, Response extends BroadcastOperationResponse, RequestBuilder extends BroadcastOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response, RequestBuilder> {

    protected BroadcastOperationRequestBuilder(InternalGenericClient client, Request request) {
        super(client, request);
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return (RequestBuilder) this;
    }

    /**
     * Controls the operation threading model.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setOperationThreading(BroadcastOperationThreading operationThreading) {
        request.operationThreading(operationThreading);
        return (RequestBuilder) this;
    }

    /**
     * Controls the operation threading model.
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setOperationThreading(String operationThreading) {
        request.operationThreading(operationThreading);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setIgnoreIndices(IgnoreIndices ignoreIndices) {
        request.ignoreIndices(ignoreIndices);
        return (RequestBuilder) this;
    }
}
