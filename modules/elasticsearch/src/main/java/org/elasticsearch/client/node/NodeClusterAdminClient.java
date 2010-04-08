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

package org.elasticsearch.client.node;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfo;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportNodesShutdown;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.elasticsearch.action.admin.cluster.ping.broadcast.TransportBroadcastPingAction;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingResponse;
import org.elasticsearch.action.admin.cluster.ping.replication.TransportReplicationPingAction;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingRequest;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingResponse;
import org.elasticsearch.action.admin.cluster.ping.single.TransportSinglePingAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (shay.banon)
 */
public class NodeClusterAdminClient extends AbstractComponent implements ClusterAdminClient {

    private final TransportClusterHealthAction clusterHealthAction;

    private final TransportClusterStateAction clusterStateAction;

    private final TransportSinglePingAction singlePingAction;

    private final TransportBroadcastPingAction broadcastPingAction;

    private final TransportReplicationPingAction replicationPingAction;

    private final TransportNodesInfo nodesInfo;

    private final TransportNodesShutdown nodesShutdown;

    @Inject public NodeClusterAdminClient(Settings settings,
                                          TransportClusterHealthAction clusterHealthAction, TransportClusterStateAction clusterStateAction,
                                          TransportSinglePingAction singlePingAction, TransportBroadcastPingAction broadcastPingAction, TransportReplicationPingAction replicationPingAction,
                                          TransportNodesInfo nodesInfo, TransportNodesShutdown nodesShutdown) {
        super(settings);
        this.clusterHealthAction = clusterHealthAction;
        this.clusterStateAction = clusterStateAction;
        this.nodesInfo = nodesInfo;
        this.nodesShutdown = nodesShutdown;
        this.singlePingAction = singlePingAction;
        this.broadcastPingAction = broadcastPingAction;
        this.replicationPingAction = replicationPingAction;
    }

    @Override public ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request) {
        return clusterHealthAction.execute(request);
    }

    @Override public void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener) {
        clusterHealthAction.execute(request, listener);
    }

    @Override public ActionFuture<ClusterStateResponse> state(ClusterStateRequest request) {
        return clusterStateAction.execute(request);
    }

    @Override public void state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener) {
        clusterStateAction.execute(request, listener);
    }

    @Override public ActionFuture<SinglePingResponse> ping(SinglePingRequest request) {
        return singlePingAction.execute(request);
    }

    @Override public void ping(SinglePingRequest request, ActionListener<SinglePingResponse> listener) {
        singlePingAction.execute(request, listener);
    }

    @Override public ActionFuture<BroadcastPingResponse> ping(BroadcastPingRequest request) {
        return broadcastPingAction.execute(request);
    }

    @Override public void ping(BroadcastPingRequest request, ActionListener<BroadcastPingResponse> listener) {
        broadcastPingAction.execute(request, listener);
    }

    @Override public ActionFuture<ReplicationPingResponse> ping(ReplicationPingRequest request) {
        return replicationPingAction.execute(request);
    }

    @Override public void ping(ReplicationPingRequest request, ActionListener<ReplicationPingResponse> listener) {
        replicationPingAction.execute(request, listener);
    }

    @Override public ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request) {
        return nodesInfo.execute(request);
    }

    @Override public void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener) {
        nodesInfo.execute(request, listener);
    }

    @Override public ActionFuture<NodesShutdownResponse> nodesShutdown(NodesShutdownRequest request) {
        return nodesShutdown.execute(request);
    }

    @Override public void nodesShutdown(NodesShutdownRequest request, ActionListener<NodesShutdownResponse> listener) {
        nodesShutdown.execute(request, listener);
    }
}
