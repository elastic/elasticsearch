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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartRequest;
import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartResponse;
import org.elasticsearch.action.admin.cluster.node.restart.TransportNodesRestartAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportNodesShutdownAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.elasticsearch.action.admin.cluster.ping.broadcast.TransportBroadcastPingAction;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingResponse;
import org.elasticsearch.action.admin.cluster.ping.replication.TransportReplicationPingAction;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingRequest;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingResponse;
import org.elasticsearch.action.admin.cluster.ping.single.TransportSinglePingAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.client.internal.InternalClusterAdminClient;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * @author kimchy (shay.banon)
 */
public class NodeClusterAdminClient extends AbstractClusterAdminClient implements InternalClusterAdminClient {

    private final ThreadPool threadPool;

    private final TransportClusterHealthAction clusterHealthAction;

    private final TransportClusterStateAction clusterStateAction;

    private final TransportClusterUpdateSettingsAction clusterUpdateSettingsAction;

    private final TransportSinglePingAction singlePingAction;

    private final TransportBroadcastPingAction broadcastPingAction;

    private final TransportReplicationPingAction replicationPingAction;

    private final TransportNodesInfoAction nodesInfoAction;

    private final TransportNodesStatsAction nodesStatsAction;

    private final TransportNodesShutdownAction nodesShutdown;

    private final TransportNodesRestartAction nodesRestart;

    @Inject public NodeClusterAdminClient(Settings settings, ThreadPool threadPool,
                                          TransportClusterHealthAction clusterHealthAction, TransportClusterStateAction clusterStateAction, TransportClusterUpdateSettingsAction clusterUpdateSettingsAction,
                                          TransportSinglePingAction singlePingAction, TransportBroadcastPingAction broadcastPingAction, TransportReplicationPingAction replicationPingAction,
                                          TransportNodesInfoAction nodesInfoAction, TransportNodesShutdownAction nodesShutdown, TransportNodesRestartAction nodesRestart, TransportNodesStatsAction nodesStatsAction) {
        this.threadPool = threadPool;
        this.clusterHealthAction = clusterHealthAction;
        this.clusterStateAction = clusterStateAction;
        this.clusterUpdateSettingsAction = clusterUpdateSettingsAction;
        this.nodesInfoAction = nodesInfoAction;
        this.nodesShutdown = nodesShutdown;
        this.nodesRestart = nodesRestart;
        this.singlePingAction = singlePingAction;
        this.broadcastPingAction = broadcastPingAction;
        this.replicationPingAction = replicationPingAction;
        this.nodesStatsAction = nodesStatsAction;
    }

    @Override public ThreadPool threadPool() {
        return this.threadPool;
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

    @Override public ActionFuture<ClusterUpdateSettingsResponse> updateSettings(ClusterUpdateSettingsRequest request) {
        return clusterUpdateSettingsAction.execute(request);
    }

    @Override public void updateSettings(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener) {
        clusterUpdateSettingsAction.execute(request, listener);
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
        return nodesInfoAction.execute(request);
    }

    @Override public void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener) {
        nodesInfoAction.execute(request, listener);
    }

    @Override public ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request) {
        return nodesStatsAction.execute(request);
    }

    @Override public void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener) {
        nodesStatsAction.execute(request, listener);
    }

    @Override public ActionFuture<NodesShutdownResponse> nodesShutdown(NodesShutdownRequest request) {
        return nodesShutdown.execute(request);
    }

    @Override public void nodesShutdown(NodesShutdownRequest request, ActionListener<NodesShutdownResponse> listener) {
        nodesShutdown.execute(request, listener);
    }

    @Override public ActionFuture<NodesRestartResponse> nodesRestart(NodesRestartRequest request) {
        return nodesRestart.execute(request);
    }

    @Override public void nodesRestart(NodesRestartRequest request, ActionListener<NodesRestartResponse> listener) {
        nodesRestart.execute(request, listener);
    }
}
