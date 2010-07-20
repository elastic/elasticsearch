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

package org.elasticsearch.client.action.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.support.BaseRequestBuilder;

import javax.annotation.Nullable;

/**
 * A get document action request builder.
 *
 * @author kimchy (shay.banon)
 */
public class GetRequestBuilder extends BaseRequestBuilder<GetRequest, GetResponse> {

    public GetRequestBuilder(Client client, @Nullable String index) {
        super(client, new GetRequest(index));
    }

    /**
     * Sets the index of the document to fetch.
     */
    public GetRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * Sets the type of the document to fetch.
     */
    public GetRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the id of the document to fetch.
     */
    public GetRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, the <tt>_source</tt>
     * field will be returned.
     */
    public GetRequestBuilder setFields(String... fields) {
        request.fields(fields);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public GetRequestBuilder setListenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public GetRequestBuilder setOperationThreaded(boolean threadedOperation) {
        request.operationThreaded(threadedOperation);
        return this;
    }

    @Override protected void doExecute(ActionListener<GetResponse> listener) {
        client.get(request, listener);
    }
}
