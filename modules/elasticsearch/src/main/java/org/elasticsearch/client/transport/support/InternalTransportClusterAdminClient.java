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

package org.elasticsearch.client.transport.support;

import org.elasticsearch.ElasticSearchException;
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
import org.elasticsearch.client.internal.InternalClusterAdminClient;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.client.transport.action.admin.cluster.health.ClientTransportClusterHealthAction;
import org.elasticsearch.client.transport.action.admin.cluster.node.info.ClientTransportNodesInfoAction;
import org.elasticsearch.client.transport.action.admin.cluster.node.restart.ClientTransportNodesRestartAction;
import org.elasticsearch.client.transport.action.admin.cluster.node.shutdown.ClientTransportNodesShutdownAction;
import org.elasticsearch.client.transport.action.admin.cluster.node.stats.ClientTransportNodesStatsAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.broadcast.ClientTransportBroadcastPingAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.replication.ClientTransportReplicationPingAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.single.ClientTransportSinglePingAction;
import org.elasticsearch.client.transport.action.admin.cluster.settings.ClientTransportClusterUpdateSettingsAction;
import org.elasticsearch.client.transport.action.admin.cluster.state.ClientTransportClusterStateAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalTransportClusterAdminClient extends AbstractClusterAdminClient implements InternalClusterAdminClient {

    private final TransportClientNodesService nodesService;

    private final ThreadPool threadPool;

    private final ClientTransportClusterHealthAction clusterHealthAction;

    private final ClientTransportClusterStateAction clusterStateAction;

    private final ClientTransportClusterUpdateSettingsAction clusterUpdateSettingsAction;

    private final ClientTransportSinglePingAction singlePingAction;

    private final ClientTransportReplicationPingAction replicationPingAction;

    private final ClientTransportBroadcastPingAction broadcastPingAction;

    private final ClientTransportNodesInfoAction nodesInfoAction;

    private final ClientTransportNodesStatsAction nodesStatsAction;

    private final ClientTransportNodesShutdownAction nodesShutdownAction;

    private final ClientTransportNodesRestartAction nodesRestartAction;

    @Inject public InternalTransportClusterAdminClient(Settings settings, TransportClientNodesService nodesService, ThreadPool threadPool,
                                                       ClientTransportClusterHealthAction clusterHealthAction, ClientTransportClusterStateAction clusterStateAction, ClientTransportClusterUpdateSettingsAction clusterUpdateSettingsAction,
                                                       ClientTransportSinglePingAction singlePingAction, ClientTransportReplicationPingAction replicationPingAction, ClientTransportBroadcastPingAction broadcastPingAction,
                                                       ClientTransportNodesInfoAction nodesInfoAction, ClientTransportNodesShutdownAction nodesShutdownAction, ClientTransportNodesRestartAction nodesRestartAction, ClientTransportNodesStatsAction nodesStatsAction) {
        this.nodesService = nodesService;
        this.threadPool = threadPool;
        this.clusterHealthAction = clusterHealthAction;
        this.clusterStateAction = clusterStateAction;
        this.clusterUpdateSettingsAction = clusterUpdateSettingsAction;
        this.nodesInfoAction = nodesInfoAction;
        this.nodesShutdownAction = nodesShutdownAction;
        this.nodesRestartAction = nodesRestartAction;
        this.singlePingAction = singlePingAction;
        this.replicationPingAction = replicationPingAction;
        this.broadcastPingAction = broadcastPingAction;
        this.nodesStatsAction = nodesStatsAction;
    }

    @Override public ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override public ActionFuture<ClusterHealthResponse> health(final ClusterHealthRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<ClusterHealthResponse>>() {
            @Override public ActionFuture<ClusterHealthResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return clusterHealthAction.execute(node, request);
            }
        });
    }

    @Override public void health(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<ClusterHealthResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<ClusterHealthResponse> listener) throws ElasticSearchException {
                clusterHealthAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<ClusterStateResponse>>() {
            @Override public ActionFuture<ClusterStateResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return clusterStateAction.execute(node, request);
            }
        });
    }

    @Override public void state(final ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<ClusterStateResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<ClusterStateResponse> listener) throws ElasticSearchException {
                clusterStateAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<ClusterUpdateSettingsResponse> updateSettings(final ClusterUpdateSettingsRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<ClusterUpdateSettingsResponse>>() {
            @Override public ActionFuture<ClusterUpdateSettingsResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return clusterUpdateSettingsAction.execute(node, request);
            }
        });
    }

    @Override public void updateSettings(final ClusterUpdateSettingsRequest request, final ActionListener<ClusterUpdateSettingsResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<ClusterUpdateSettingsResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<ClusterUpdateSettingsResponse> listener) throws ElasticSearchException {
                clusterUpdateSettingsAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<SinglePingResponse> ping(final SinglePingRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<SinglePingResponse>>() {
            @Override public ActionFuture<SinglePingResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return singlePingAction.execute(node, request);
            }
        });
    }

    @Override public void ping(final SinglePingRequest request, final ActionListener<SinglePingResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<SinglePingResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<SinglePingResponse> listener) throws ElasticSearchException {
                singlePingAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<BroadcastPingResponse> ping(final BroadcastPingRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<BroadcastPingResponse>>() {
            @Override public ActionFuture<BroadcastPingResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return broadcastPingAction.execute(node, request);
            }
        });
    }

    @Override public void ping(final BroadcastPingRequest request, final ActionListener<BroadcastPingResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<BroadcastPingResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<BroadcastPingResponse> listener) throws ElasticSearchException {
                broadcastPingAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<ReplicationPingResponse> ping(final ReplicationPingRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<ReplicationPingResponse>>() {
            @Override public ActionFuture<ReplicationPingResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return replicationPingAction.execute(node, request);
            }
        });
    }

    @Override public void ping(final ReplicationPingRequest request, final ActionListener<ReplicationPingResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<ReplicationPingResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<ReplicationPingResponse> listener) throws ElasticSearchException {
                replicationPingAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<NodesInfoResponse> nodesInfo(final NodesInfoRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<NodesInfoResponse>>() {
            @Override public ActionFuture<NodesInfoResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return nodesInfoAction.execute(node, request);
            }
        });
    }

    @Override public void nodesInfo(final NodesInfoRequest request, final ActionListener<NodesInfoResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<NodesInfoResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<NodesInfoResponse> listener) throws ElasticSearchException {
                nodesInfoAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<NodesStatsResponse> nodesStats(final NodesStatsRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<NodesStatsResponse>>() {
            @Override public ActionFuture<NodesStatsResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return nodesStatsAction.execute(node, request);
            }
        });
    }

    @Override public void nodesStats(final NodesStatsRequest request, final ActionListener<NodesStatsResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<NodesStatsResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<NodesStatsResponse> listener) throws ElasticSearchException {
                nodesStatsAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<NodesShutdownResponse> nodesShutdown(final NodesShutdownRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<NodesShutdownResponse>>() {
            @Override public ActionFuture<NodesShutdownResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return nodesShutdownAction.execute(node, request);
            }
        });
    }

    @Override public void nodesShutdown(final NodesShutdownRequest request, final ActionListener<NodesShutdownResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<NodesShutdownResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<NodesShutdownResponse> listener) throws ElasticSearchException {
                nodesShutdownAction.execute(node, request, listener);
            }
        }, listener);
    }

    @Override public ActionFuture<NodesRestartResponse> nodesRestart(final NodesRestartRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<org.elasticsearch.action.ActionFuture<org.elasticsearch.action.admin.cluster.node.restart.NodesRestartResponse>>() {
            @Override public ActionFuture<NodesRestartResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return nodesRestartAction.execute(node, request);
            }
        });
    }

    @Override public void nodesRestart(final NodesRestartRequest request, final ActionListener<NodesRestartResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<NodesRestartResponse>() {
            @Override public void doWithNode(DiscoveryNode node, ActionListener<NodesRestartResponse> listener) throws ElasticSearchException {
                nodesRestartAction.execute(node, request, listener);
            }
        }, listener);
    }
}
