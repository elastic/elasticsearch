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

package org.elasticsearch.client.action.admin.cluster.ping.broadcast;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.action.admin.cluster.support.BaseClusterRequestBuilder;

/**
 * @author kimchy (shay.banon)
 */
public class BroadcastPingRequestBuilder extends BaseClusterRequestBuilder<BroadcastPingRequest, BroadcastPingResponse> {

    public BroadcastPingRequestBuilder(ClusterAdminClient clusterClient) {
        super(clusterClient, new BroadcastPingRequest());
    }

    public BroadcastPingRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }


    public BroadcastPingRequestBuilder setOperationThreading(BroadcastOperationThreading operationThreading) {
        request.operationThreading(operationThreading);
        return this;
    }

    public BroadcastPingRequestBuilder setListenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    public BroadcastPingRequestBuilder setQueryHint(String queryHint) {
        request.queryHint(queryHint);
        return this;
    }

    @Override protected void doExecute(ActionListener<BroadcastPingResponse> listener) {
        client.ping(request, listener);
    }
}
