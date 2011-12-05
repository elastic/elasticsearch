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

package org.elasticsearch.client.action.admin.cluster.ping.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.action.admin.cluster.support.BaseClusterRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;

/**
 * @author kimchy (shay.banon)
 */
public class ReplicationPingRequestBuilder extends BaseClusterRequestBuilder<ReplicationPingRequest, ReplicationPingResponse> {

    public ReplicationPingRequestBuilder(ClusterAdminClient clusterClient) {
        super(clusterClient, new ReplicationPingRequest());
    }

    public ReplicationPingRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public ReplicationPingRequestBuilder setListenerThreaded(boolean threadedListener) {
        request.listenerThreaded(threadedListener);
        return this;
    }

    public ReplicationPingRequestBuilder setReplicationType(ReplicationType replicationType) {
        request.replicationType(replicationType);
        return this;
    }

    public ReplicationPingRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    public ReplicationPingRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    @Override protected void doExecute(ActionListener<ReplicationPingResponse> listener) {
        client.ping(request, listener);
    }
}