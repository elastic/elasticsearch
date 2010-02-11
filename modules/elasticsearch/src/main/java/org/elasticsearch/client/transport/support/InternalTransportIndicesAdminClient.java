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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.client.transport.action.admin.indices.create.ClientTransportCreateIndexAction;
import org.elasticsearch.client.transport.action.admin.indices.delete.ClientTransportDeleteIndexAction;
import org.elasticsearch.client.transport.action.admin.indices.flush.ClientTransportFlushAction;
import org.elasticsearch.client.transport.action.admin.indices.gateway.snapshot.ClientTransportGatewaySnapshotAction;
import org.elasticsearch.client.transport.action.admin.indices.mapping.create.ClientTransportCreateMappingAction;
import org.elasticsearch.client.transport.action.admin.indices.refresh.ClientTransportRefreshAction;
import org.elasticsearch.client.transport.action.admin.indices.status.ClientTransportIndicesStatusAction;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalTransportIndicesAdminClient extends AbstractComponent implements IndicesAdminClient {

    private final TransportClientNodesService nodesService;

    private final ClientTransportIndicesStatusAction indicesStatusAction;

    private final ClientTransportCreateIndexAction createIndexAction;

    private final ClientTransportDeleteIndexAction deleteIndexAction;

    private final ClientTransportRefreshAction refreshAction;

    private final ClientTransportFlushAction flushAction;

    private final ClientTransportCreateMappingAction createMappingAction;

    private final ClientTransportGatewaySnapshotAction gatewaySnapshotAction;

    @Inject public InternalTransportIndicesAdminClient(Settings settings, TransportClientNodesService nodesService,
                                                       ClientTransportIndicesStatusAction indicesStatusAction,
                                                       ClientTransportCreateIndexAction createIndexAction, ClientTransportDeleteIndexAction deleteIndexAction,
                                                       ClientTransportRefreshAction refreshAction, ClientTransportFlushAction flushAction,
                                                       ClientTransportCreateMappingAction createMappingAction, ClientTransportGatewaySnapshotAction gatewaySnapshotAction) {
        super(settings);
        this.nodesService = nodesService;
        this.indicesStatusAction = indicesStatusAction;
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
        this.refreshAction = refreshAction;
        this.flushAction = flushAction;
        this.createMappingAction = createMappingAction;
        this.gatewaySnapshotAction = gatewaySnapshotAction;
    }

    @Override public ActionFuture<IndicesStatusResponse> status(final IndicesStatusRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<IndicesStatusResponse>>() {
            @Override public ActionFuture<IndicesStatusResponse> doWithNode(Node node) throws ElasticSearchException {
                return indicesStatusAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<IndicesStatusResponse> status(final IndicesStatusRequest request, final ActionListener<IndicesStatusResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<IndicesStatusResponse>>() {
            @Override public ActionFuture<IndicesStatusResponse> doWithNode(Node node) throws ElasticSearchException {
                return indicesStatusAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execStatus(final IndicesStatusRequest request, final ActionListener<IndicesStatusResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(Node node) throws ElasticSearchException {
                indicesStatusAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<CreateIndexResponse> create(final CreateIndexRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<CreateIndexResponse>>() {
            @Override public ActionFuture<CreateIndexResponse> doWithNode(Node node) throws ElasticSearchException {
                return createIndexAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<CreateIndexResponse> create(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<CreateIndexResponse>>() {
            @Override public ActionFuture<CreateIndexResponse> doWithNode(Node node) throws ElasticSearchException {
                return createIndexAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execCreate(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Object>() {
            @Override public Object doWithNode(Node node) throws ElasticSearchException {
                createIndexAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<DeleteIndexResponse> delete(final DeleteIndexRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<DeleteIndexResponse>>() {
            @Override public ActionFuture<DeleteIndexResponse> doWithNode(Node node) throws ElasticSearchException {
                return deleteIndexAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<DeleteIndexResponse> delete(final DeleteIndexRequest request, final ActionListener<DeleteIndexResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<DeleteIndexResponse>>() {
            @Override public ActionFuture<DeleteIndexResponse> doWithNode(Node node) throws ElasticSearchException {
                return deleteIndexAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execDelete(final DeleteIndexRequest request, final ActionListener<DeleteIndexResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Object>() {
            @Override public Object doWithNode(Node node) throws ElasticSearchException {
                deleteIndexAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<RefreshResponse> refresh(final RefreshRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<RefreshResponse>>() {
            @Override public ActionFuture<RefreshResponse> doWithNode(Node node) throws ElasticSearchException {
                return refreshAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<RefreshResponse> refresh(final RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<RefreshResponse>>() {
            @Override public ActionFuture<RefreshResponse> doWithNode(Node node) throws ElasticSearchException {
                return refreshAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execRefresh(final RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(Node node) throws ElasticSearchException {
                refreshAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<FlushResponse> flush(final FlushRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<FlushResponse>>() {
            @Override public ActionFuture<FlushResponse> doWithNode(Node node) throws ElasticSearchException {
                return flushAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<FlushResponse> flush(final FlushRequest request, final ActionListener<FlushResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<FlushResponse>>() {
            @Override public ActionFuture<FlushResponse> doWithNode(Node node) throws ElasticSearchException {
                return flushAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execFlush(final FlushRequest request, final ActionListener<FlushResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Object>() {
            @Override public Object doWithNode(Node node) throws ElasticSearchException {
                flushAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<CreateMappingResponse> createMapping(final CreateMappingRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<CreateMappingResponse>>() {
            @Override public ActionFuture<CreateMappingResponse> doWithNode(Node node) throws ElasticSearchException {
                return createMappingAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<CreateMappingResponse> createMapping(final CreateMappingRequest request, final ActionListener<CreateMappingResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<CreateMappingResponse>>() {
            @Override public ActionFuture<CreateMappingResponse> doWithNode(Node node) throws ElasticSearchException {
                return createMappingAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execCreateMapping(final CreateMappingRequest request, final ActionListener<CreateMappingResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(Node node) throws ElasticSearchException {
                createMappingAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(final GatewaySnapshotRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<GatewaySnapshotResponse>>() {
            @Override public ActionFuture<GatewaySnapshotResponse> doWithNode(Node node) throws ElasticSearchException {
                return gatewaySnapshotAction.submit(node, request);
            }
        });
    }

    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(final GatewaySnapshotRequest request, final ActionListener<GatewaySnapshotResponse> listener) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<GatewaySnapshotResponse>>() {
            @Override public ActionFuture<GatewaySnapshotResponse> doWithNode(Node node) throws ElasticSearchException {
                return gatewaySnapshotAction.submit(node, request, listener);
            }
        });
    }

    @Override public void execGatewaySnapshot(final GatewaySnapshotRequest request, final ActionListener<GatewaySnapshotResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Object>() {
            @Override public Object doWithNode(Node node) throws ElasticSearchException {
                gatewaySnapshotAction.execute(node, request, listener);
                return null;
            }
        });
    }
}
