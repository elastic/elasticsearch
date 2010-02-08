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

package org.elasticsearch.client.server;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse;
import org.elasticsearch.action.admin.indices.gateway.snapshot.TransportGatewaySnapshotAction;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.create.TransportCreateMappingAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.TransportIndicesStatusAction;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class ServerIndicesAdminClient extends AbstractComponent implements IndicesAdminClient {

    private final TransportIndicesStatusAction indicesStatusAction;

    private final TransportCreateIndexAction createIndexAction;

    private final TransportDeleteIndexAction deleteIndexAction;

    private final TransportRefreshAction refreshAction;

    private final TransportFlushAction flushAction;

    private final TransportCreateMappingAction createMappingAction;

    private final TransportGatewaySnapshotAction gatewaySnapshotAction;

    @Inject public ServerIndicesAdminClient(Settings settings, TransportIndicesStatusAction indicesStatusAction,
                                            TransportCreateIndexAction createIndexAction, TransportDeleteIndexAction deleteIndexAction,
                                            TransportRefreshAction refreshAction, TransportFlushAction flushAction,
                                            TransportCreateMappingAction createMappingAction, TransportGatewaySnapshotAction gatewaySnapshotAction) {
        super(settings);
        this.indicesStatusAction = indicesStatusAction;
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
        this.refreshAction = refreshAction;
        this.flushAction = flushAction;
        this.createMappingAction = createMappingAction;
        this.gatewaySnapshotAction = gatewaySnapshotAction;
    }

    @Override public ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request) {
        return indicesStatusAction.submit(request);
    }

    @Override public ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        return indicesStatusAction.submit(request, listener);
    }

    @Override public void execStatus(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        indicesStatusAction.execute(request, listener);
    }

    @Override public ActionFuture<CreateIndexResponse> create(CreateIndexRequest request) {
        return createIndexAction.submit(request);
    }

    @Override public ActionFuture<CreateIndexResponse> create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        return createIndexAction.submit(request, listener);
    }

    @Override public void execCreate(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        createIndexAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request) {
        return deleteIndexAction.submit(request);
    }

    @Override public ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        return deleteIndexAction.submit(request, listener);
    }

    @Override public void execDelete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        deleteIndexAction.execute(request, listener);
    }

    @Override public ActionFuture<RefreshResponse> refresh(RefreshRequest request) {
        return refreshAction.submit(request);
    }

    @Override public ActionFuture<RefreshResponse> refresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        return refreshAction.submit(request, listener);
    }

    @Override public void execRefresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        refreshAction.execute(request, listener);
    }

    @Override public ActionFuture<FlushResponse> flush(FlushRequest request) {
        return flushAction.submit(request);
    }

    @Override public ActionFuture<FlushResponse> flush(FlushRequest request, ActionListener<FlushResponse> listener) {
        return flushAction.submit(request, listener);
    }

    @Override public void execFlush(FlushRequest request, ActionListener<FlushResponse> listener) {
        flushAction.execute(request, listener);
    }

    @Override public ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request) {
        return createMappingAction.submit(request);
    }

    @Override public ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener) {
        return createMapping(request, listener);
    }

    @Override public void execCreateMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener) {
        createMappingAction.execute(request, listener);
    }

    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request) {
        return gatewaySnapshotAction.submit(request);
    }

    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener) {
        return gatewaySnapshotAction.submit(request, listener);
    }

    @Override public void execGatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener) {
        gatewaySnapshotAction.execute(request, listener);
    }
}
