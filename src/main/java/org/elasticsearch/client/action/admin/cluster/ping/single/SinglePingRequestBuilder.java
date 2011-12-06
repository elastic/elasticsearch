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

package org.elasticsearch.client.action.admin.cluster.ping.single;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingRequest;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.action.admin.cluster.support.BaseClusterRequestBuilder;

/**
 *
 */
public class SinglePingRequestBuilder extends BaseClusterRequestBuilder<SinglePingRequest, SinglePingResponse> {

    public SinglePingRequestBuilder(ClusterAdminClient clusterClient) {
        super(clusterClient, new SinglePingRequest());
    }

    public SinglePingRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    public SinglePingRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    public SinglePingRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    public SinglePingRequestBuilder setListenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    public SinglePingRequestBuilder setOperationThreaded(boolean threadedOperation) {
        request.operationThreaded(threadedOperation);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<SinglePingResponse> listener) {
        client.ping(request, listener);
    }
}