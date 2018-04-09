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

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Base request builder for master node operations that support acknowledgements
 */
public abstract class AcknowledgedRequestBuilder<Request extends AcknowledgedRequest<Request>, Response extends AcknowledgedResponse, RequestBuilder extends AcknowledgedRequestBuilder<Request, Response, RequestBuilder>>
        extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

    protected AcknowledgedRequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action, Request request) {
        super(client, action, request);
    }

    /**
     * Sets the maximum wait for acknowledgement from other nodes
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Timeout to wait for the operation to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    @SuppressWarnings("unchecked")
    public RequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }
}
