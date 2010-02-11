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

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingResponse;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingRequest;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.client.transport.action.admin.cluster.node.info.ClientTransportNodesInfoAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.broadcast.ClientTransportBroadcastPingAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.replication.ClientTransportReplicationPingAction;
import org.elasticsearch.client.transport.action.admin.cluster.ping.single.ClientTransportSinglePingAction;
import org.elasticsearch.client.transport.action.admin.cluster.state.ClientTransportClusterStateAction;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalTransportClusterAdminClient extends AbstractComponent implements ClusterAdminClient {

    private final TransportClientNodesService nodesService;

    private final ClientTransportClusterStateAction clusterStateAction;

    private final ClientTransportSinglePingAction singlePingAction;

    private final ClientTransportReplicationPingAction replicationPingAction;

    private final ClientTransportBroadcastPingAction broadcastPingAction;

    private final ClientTransportNodesInfoAction nodesInfoAction;

    @Inject public InternalTransportClusterAdminClient(Settings settings, TransportClientNodesService nodesService,
                                                       ClientTransportClusterStateAction clusterStateAction,
                                                       ClientTransportSinglePingAction singlePingAction, ClientTransportReplicationPingAction replicationPingAction, ClientTransportBroadcastPingAction broadcastPingAction,
                                                       ClientTransportNodesInfoAction nodesInfoAction) {
        super(settings);
        this.nodesService = nodesService;
        this.clusterStateAction = clusterStateAction;
        this.nodesInfoAction = nodesInfoAction;
        this.singlePingAction = singlePingAction;
        this.replicationPingAction = replicationPingAction;
        this.broadcastPingAction = broadcastPingAction;
    }

    @Override public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<ClusterStateResponse>>() {
            @Override public ActionFuture<ClusterStateResponse> doWithNode(Node node) throws ElasticSearchException {
                return clusterStateAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<ClusterStateResponse>>() {
            @Override public ActionFuture<ClusterStateResponse> doWithNode(Node node) throws ElasticSearchException {
                return clusterStateAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execState(final ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(Node node) throws ElasticSearchException {
                clusterStateAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<SinglePingResponse> ping(final SinglePingRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<SinglePingResponse>>() {
            @Override public ActionFuture<SinglePingResponse> doWithNode(Node node) throws ElasticSearchException {
                return singlePingAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<SinglePingResponse> ping(final SinglePingRequest request, final ActionListener<SinglePingResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<SinglePingResponse>>() {
            @Override public ActionFuture<SinglePingResponse> doWithNode(Node node) throws ElasticSearchException {
                return singlePingAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execPing(final SinglePingRequest request, final ActionListener<SinglePingResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(Node node) throws ElasticSearchException {
                singlePingAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<BroadcastPingResponse> ping(final BroadcastPingRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<BroadcastPingResponse>>() {
            @Override public ActionFuture<BroadcastPingResponse> doWithNode(Node node) throws ElasticSearchException {
                return broadcastPingAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<BroadcastPingResponse> ping(final BroadcastPingRequest request, final ActionListener<BroadcastPingResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<BroadcastPingResponse>>() {
            @Override public ActionFuture<BroadcastPingResponse> doWithNode(Node node) throws ElasticSearchException {
                return broadcastPingAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execPing(final BroadcastPingRequest request, final ActionListener<BroadcastPingResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(Node node) throws ElasticSearchException {
                broadcastPingAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<ReplicationPingResponse> ping(final ReplicationPingRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<ReplicationPingResponse>>() {
            @Override public ActionFuture<ReplicationPingResponse> doWithNode(Node node) throws ElasticSearchException {
                return replicationPingAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<ReplicationPingResponse> ping(final ReplicationPingRequest request, final ActionListener<ReplicationPingResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<ReplicationPingResponse>>() {
            @Override public ActionFuture<ReplicationPingResponse> doWithNode(Node node) throws ElasticSearchException {
                return replicationPingAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execPing(final ReplicationPingRequest request, final ActionListener<ReplicationPingResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(Node node) throws ElasticSearchException {
                replicationPingAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<NodesInfoResponse> nodesInfo(final NodesInfoRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<NodesInfoResponse>>() {
            @Override public ActionFuture<NodesInfoResponse> doWithNode(Node node) throws ElasticSearchException {
                return nodesInfoAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<NodesInfoResponse> nodesInfo(final NodesInfoRequest request, final ActionListener<NodesInfoResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<NodesInfoResponse>>() {
            @Override public ActionFuture<NodesInfoResponse> doWithNode(Node node) throws ElasticSearchException {
                return nodesInfoAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execNodesInfo(final NodesInfoRequest request, final ActionListener<NodesInfoResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(Node node) throws ElasticSearchException {
                nodesInfoAction.execute(node, request, listener);
                return null;
            }
        });
    }
}
