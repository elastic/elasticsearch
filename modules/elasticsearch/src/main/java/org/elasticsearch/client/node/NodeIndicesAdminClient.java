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
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
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
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.optimize.TransportOptimizeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.TransportIndicesStatusAction;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (shay.banon)
 */
public class NodeIndicesAdminClient extends AbstractComponent implements IndicesAdminClient {

    private final TransportIndicesStatusAction indicesStatusAction;

    private final TransportCreateIndexAction createIndexAction;

    private final TransportDeleteIndexAction deleteIndexAction;

    private final TransportRefreshAction refreshAction;

    private final TransportFlushAction flushAction;

    private final TransportOptimizeAction optimizeAction;

    private final TransportPutMappingAction putMappingAction;

    private final TransportGatewaySnapshotAction gatewaySnapshotAction;

    private final TransportIndicesAliasesAction indicesAliasesAction;

    private final TransportClearIndicesCacheAction clearIndicesCacheAction;

    @Inject public NodeIndicesAdminClient(Settings settings, TransportIndicesStatusAction indicesStatusAction,
                                          TransportCreateIndexAction createIndexAction, TransportDeleteIndexAction deleteIndexAction,
                                          TransportRefreshAction refreshAction, TransportFlushAction flushAction, TransportOptimizeAction optimizeAction,
                                          TransportPutMappingAction putMappingAction, TransportGatewaySnapshotAction gatewaySnapshotAction,
                                          TransportIndicesAliasesAction indicesAliasesAction, TransportClearIndicesCacheAction clearIndicesCacheAction) {
        super(settings);
        this.indicesStatusAction = indicesStatusAction;
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
        this.refreshAction = refreshAction;
        this.flushAction = flushAction;
        this.optimizeAction = optimizeAction;
        this.putMappingAction = putMappingAction;
        this.gatewaySnapshotAction = gatewaySnapshotAction;
        this.indicesAliasesAction = indicesAliasesAction;
        this.clearIndicesCacheAction = clearIndicesCacheAction;
    }

    @Override public ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request) {
        return indicesStatusAction.execute(request);
    }

    @Override public void status(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        indicesStatusAction.execute(request, listener);
    }

    @Override public ActionFuture<CreateIndexResponse> create(CreateIndexRequest request) {
        return createIndexAction.execute(request);
    }

    @Override public void create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        createIndexAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request) {
        return deleteIndexAction.execute(request);
    }

    @Override public void delete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        deleteIndexAction.execute(request, listener);
    }

    @Override public ActionFuture<RefreshResponse> refresh(RefreshRequest request) {
        return refreshAction.execute(request);
    }

    @Override public void refresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        refreshAction.execute(request, listener);
    }

    @Override public ActionFuture<FlushResponse> flush(FlushRequest request) {
        return flushAction.execute(request);
    }

    @Override public void flush(FlushRequest request, ActionListener<FlushResponse> listener) {
        flushAction.execute(request, listener);
    }

    @Override public ActionFuture<OptimizeResponse> optimize(OptimizeRequest request) {
        return optimizeAction.execute(request);
    }

    @Override public void optimize(OptimizeRequest request, ActionListener<OptimizeResponse> listener) {
        optimizeAction.execute(request, listener);
    }

    @Override public ActionFuture<PutMappingResponse> putMapping(PutMappingRequest request) {
        return putMappingAction.execute(request);
    }

    @Override public void putMapping(PutMappingRequest request, ActionListener<PutMappingResponse> listener) {
        putMappingAction.execute(request, listener);
    }

    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request) {
        return gatewaySnapshotAction.execute(request);
    }

    @Override public void gatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener) {
        gatewaySnapshotAction.execute(request, listener);
    }

    @Override public ActionFuture<IndicesAliasesResponse> aliases(IndicesAliasesRequest request) {
        return indicesAliasesAction.execute(request);
    }

    @Override public void aliases(IndicesAliasesRequest request, ActionListener<IndicesAliasesResponse> listener) {
        indicesAliasesAction.execute(request, listener);
    }

    @Override public ActionFuture<ClearIndicesCacheResponse> clearCache(ClearIndicesCacheRequest request) {
        return clearIndicesCacheAction.execute(request);
    }

    @Override public void clearCache(ClearIndicesCacheRequest request, ActionListener<ClearIndicesCacheResponse> listener) {
        clearIndicesCacheAction.execute(request, listener);
    }
}
