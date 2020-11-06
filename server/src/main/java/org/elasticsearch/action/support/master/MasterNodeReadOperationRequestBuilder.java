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

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Base request builder for master node read operations that can be executed on the local node as well
 */
public abstract class MasterNodeReadOperationRequestBuilder<Request extends MasterNodeReadRequest<Request>, Response extends ActionResponse,
        RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

    protected MasterNodeReadOperationRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Specifies if the request should be executed on local node rather than on master
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setLocal(boolean local) {
        request.local(local);
        return (RequestBuilder) this;
    }
}
