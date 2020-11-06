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

package org.elasticsearch.action.support.single.instance;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;

public abstract class InstanceShardOperationRequestBuilder<Request extends InstanceShardOperationRequest<Request>,
        Response extends ActionResponse, RequestBuilder extends InstanceShardOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response> {

    protected InstanceShardOperationRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setIndex(String index) {
        request.index(index);
        return (RequestBuilder) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }
}
