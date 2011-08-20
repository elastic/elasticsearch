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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartRequest;
import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingResponse;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingRequest;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.node.restart.NodesRestartRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.node.shutdown.NodesShutdownRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.ping.broadcast.BroadcastPingRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.ping.replication.ReplicationPingRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.ping.single.SinglePingRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.client.action.admin.cluster.state.ClusterStateRequestBuilder;

/**
 * Administrative actions/operations against indices.
 *
 * @author kimchy (shay.banon)
 * @see AdminClient#cluster()
 */
public interface ClusterAdminClient {

    /**
     * The health of the cluster.
     *
     * @param request The cluster state request
     * @return The result future
     * @see Requests#clusterHealthRequest(String...)
     */
    ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request);

    /**
     * The health of the cluster.
     *
     * @param request  The cluster state request
     * @param listener A listener to be notified with a result
     * @see Requests#clusterHealthRequest(String...)
     */
    void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener);

    /**
     * The health of the cluster.
     */
    ClusterHealthRequestBuilder prepareHealth(String... indices);

    /**
     * The state of the cluster.
     *
     * @param request The cluster state request.
     * @return The result future
     * @see Requests#clusterStateRequest()
     */
    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request);

    /**
     * The state of the cluster.
     *
     * @param request  The cluster state request.
     * @param listener A listener to be notified with a result
     * @see Requests#clusterStateRequest()
     */
    void state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener);

    /**
     * The state of the cluster.
     */
    ClusterStateRequestBuilder prepareState();

    /**
     * Updates settings in the cluster.
     */
    ActionFuture<ClusterUpdateSettingsResponse> updateSettings(ClusterUpdateSettingsRequest request);

    /**
     * Update settings in the cluster.
     */
    void updateSettings(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterUpdateSettingsRequestBuilder prepareUpdateSettings();

    /**
     * Nodes info of the cluster.
     *
     * @param request The nodes info request
     * @return The result future
     * @see org.elasticsearch.client.Requests#nodesInfoRequest(String...)
     */
    ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request);

    /**
     * Nodes info of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#nodesInfoRequest(String...)
     */
    void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);

    /**
     * Nodes info of the cluster.
     */
    NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds);

    /**
     * Nodes stats of the cluster.
     *
     * @param request The nodes info request
     * @return The result future
     * @see org.elasticsearch.client.Requests#nodesStatsRequest(String...)
     */
    ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request);

    /**
     * Nodes stats of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#nodesStatsRequest(String...)
     */
    void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);

    /**
     * Nodes stats of the cluster.
     */
    NodesStatsRequestBuilder prepareNodesStats(String... nodesIds);

    /**
     * Shutdown nodes in the cluster.
     *
     * @param request The nodes shutdown request
     * @return The result future
     * @see org.elasticsearch.client.Requests#nodesShutdownRequest(String...)
     */
    ActionFuture<NodesShutdownResponse> nodesShutdown(NodesShutdownRequest request);

    /**
     * Shutdown nodes in the cluster.
     *
     * @param request  The nodes shutdown request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#nodesShutdownRequest(String...)
     */
    void nodesShutdown(NodesShutdownRequest request, ActionListener<NodesShutdownResponse> listener);

    /**
     * Shutdown nodes in the cluster.
     */
    NodesShutdownRequestBuilder prepareNodesShutdown(String... nodesIds);

    /**
     * Restarts nodes in the cluster.
     *
     * @param request The nodes restart request
     * @return The result future
     * @see org.elasticsearch.client.Requests#nodesRestartRequest(String...)
     */
    ActionFuture<NodesRestartResponse> nodesRestart(NodesRestartRequest request);

    /**
     * Restarts nodes in the cluster.
     *
     * @param request  The nodes restart request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#nodesRestartRequest(String...)
     */
    void nodesRestart(NodesRestartRequest request, ActionListener<NodesRestartResponse> listener);

    /**
     * Restarts nodes in the cluster.
     */
    NodesRestartRequestBuilder prepareNodesRestart(String... nodesIds);

    ActionFuture<SinglePingResponse> ping(SinglePingRequest request);

    void ping(SinglePingRequest request, ActionListener<SinglePingResponse> listener);

    SinglePingRequestBuilder preparePingSingle();

    SinglePingRequestBuilder preparePingSingle(String index, String type, String id);

    ActionFuture<BroadcastPingResponse> ping(BroadcastPingRequest request);

    void ping(BroadcastPingRequest request, ActionListener<BroadcastPingResponse> listener);

    BroadcastPingRequestBuilder preparePingBroadcast(String... indices);

    ActionFuture<ReplicationPingResponse> ping(ReplicationPingRequest request);

    void ping(ReplicationPingRequest request, ActionListener<ReplicationPingResponse> listener);

    ReplicationPingRequestBuilder preparePingReplication(String... indices);
}
