/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.support.BaseIndicesRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.IndicesAdminClient;

/**
 * A refresh request making all operations performed since the last refresh available for search. The (near) real-time
 * capabilities depends on the index engine used. For example, the robin one requires refresh to be called, but by
 * default a refresh is scheduled periodically.
 */
public class RefreshRequestBuilder extends BaseIndicesRequestBuilder<RefreshRequest, RefreshResponse> {

    public RefreshRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new RefreshRequest());
    }

    public RefreshRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public RefreshRequestBuilder setWaitForOperations(boolean waitForOperations) {
        request.waitForOperations(waitForOperations);
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    public RefreshRequestBuilder setListenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls the operation threading model.
     */
    public RefreshRequestBuilder setOperationThreading(BroadcastOperationThreading operationThreading) {
        request.operationThreading(operationThreading);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<RefreshResponse> listener) {
        client.refresh(request, listener);
    }
}
