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

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 */
public abstract class NodesOperationRequestBuilder<Request extends BaseNodesRequest<Request>, Response extends BaseNodesResponse, RequestBuilder extends NodesOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response, RequestBuilder> {

    protected NodesOperationRequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action, Request request) {
        super(client, action, request);
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setNodesIds(String... nodesIds) {
        request.nodesIds(nodesIds);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }
}
