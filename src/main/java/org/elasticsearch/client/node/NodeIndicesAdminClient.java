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
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.TransportIndicesExistsAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse;
import org.elasticsearch.action.admin.indices.gateway.snapshot.TransportGatewaySnapshotAction;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.TransportDeleteMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.open.TransportOpenIndexAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.optimize.TransportOptimizeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.TransportIndicesStatusAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.support.AbstractIndicesAdminClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * @author kimchy (shay.banon)
 */
public class NodeIndicesAdminClient extends AbstractIndicesAdminClient implements IndicesAdminClient {

    private final ThreadPool threadPool;

    private final TransportIndicesExistsAction indicesExistsAction;

    private final TransportIndicesStatsAction indicesStatsAction;

    private final TransportIndicesStatusAction indicesStatusAction;

    private final TransportIndicesSegmentsAction indicesSegmentsAction;

    private final TransportCreateIndexAction createIndexAction;

    private final TransportDeleteIndexAction deleteIndexAction;

    private final TransportCloseIndexAction closeIndexAction;

    private final TransportOpenIndexAction openIndexAction;

    private final TransportRefreshAction refreshAction;

    private final TransportFlushAction flushAction;

    private final TransportOptimizeAction optimizeAction;

    private final TransportPutMappingAction putMappingAction;

    private final TransportDeleteMappingAction deleteMappingAction;

    private final TransportGatewaySnapshotAction gatewaySnapshotAction;

    private final TransportIndicesAliasesAction indicesAliasesAction;

    private final TransportClearIndicesCacheAction clearIndicesCacheAction;

    private final TransportUpdateSettingsAction updateSettingsAction;

    private final TransportAnalyzeAction analyzeAction;

    private final TransportPutIndexTemplateAction putIndexTemplateAction;

    private final TransportDeleteIndexTemplateAction deleteIndexTemplateAction;

    @Inject public NodeIndicesAdminClient(Settings settings, ThreadPool threadPool, TransportIndicesExistsAction indicesExistsAction, TransportIndicesStatsAction indicesStatsAction, TransportIndicesStatusAction indicesStatusAction, TransportIndicesSegmentsAction indicesSegmentsAction,
                                          TransportCreateIndexAction createIndexAction, TransportDeleteIndexAction deleteIndexAction,
                                          TransportCloseIndexAction closeIndexAction, TransportOpenIndexAction openIndexAction,
                                          TransportRefreshAction refreshAction, TransportFlushAction flushAction, TransportOptimizeAction optimizeAction,
                                          TransportPutMappingAction putMappingAction, TransportDeleteMappingAction deleteMappingAction, TransportGatewaySnapshotAction gatewaySnapshotAction,
                                          TransportIndicesAliasesAction indicesAliasesAction, TransportClearIndicesCacheAction clearIndicesCacheAction,
                                          TransportUpdateSettingsAction updateSettingsAction, TransportAnalyzeAction analyzeAction,
                                          TransportPutIndexTemplateAction putIndexTemplateAction, TransportDeleteIndexTemplateAction deleteIndexTemplateAction) {
        this.threadPool = threadPool;
        this.indicesExistsAction = indicesExistsAction;
        this.indicesStatsAction = indicesStatsAction;
        this.indicesStatusAction = indicesStatusAction;
        this.indicesSegmentsAction = indicesSegmentsAction;
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
        this.closeIndexAction = closeIndexAction;
        this.openIndexAction = openIndexAction;
        this.refreshAction = refreshAction;
        this.flushAction = flushAction;
        this.optimizeAction = optimizeAction;
        this.deleteMappingAction = deleteMappingAction;
        this.putMappingAction = putMappingAction;
        this.gatewaySnapshotAction = gatewaySnapshotAction;
        this.indicesAliasesAction = indicesAliasesAction;
        this.clearIndicesCacheAction = clearIndicesCacheAction;
        this.updateSettingsAction = updateSettingsAction;
        this.analyzeAction = analyzeAction;
        this.putIndexTemplateAction = putIndexTemplateAction;
        this.deleteIndexTemplateAction = deleteIndexTemplateAction;
    }

    @Override public ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override public ActionFuture<IndicesExistsResponse> exists(IndicesExistsRequest request) {
        return indicesExistsAction.execute(request);
    }

    @Override public void exists(IndicesExistsRequest request, ActionListener<IndicesExistsResponse> listener) {
        indicesExistsAction.execute(request, listener);
    }

    @Override public ActionFuture<IndicesStats> stats(IndicesStatsRequest request) {
        return indicesStatsAction.execute(request);
    }

    @Override public void stats(IndicesStatsRequest request, ActionListener<IndicesStats> lister) {
        indicesStatsAction.execute(request, lister);
    }

    @Override public ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request) {
        return indicesStatusAction.execute(request);
    }

    @Override public void status(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        indicesStatusAction.execute(request, listener);
    }

    @Override public ActionFuture<IndicesSegmentResponse> segments(IndicesSegmentsRequest request) {
        return indicesSegmentsAction.execute(request);
    }

    @Override public void segments(IndicesSegmentsRequest request, ActionListener<IndicesSegmentResponse> listener) {
        indicesSegmentsAction.execute(request, listener);
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

    @Override public ActionFuture<CloseIndexResponse> close(CloseIndexRequest request) {
        return closeIndexAction.execute(request);
    }

    @Override public void close(CloseIndexRequest request, ActionListener<CloseIndexResponse> listener) {
        closeIndexAction.execute(request, listener);
    }

    @Override public ActionFuture<OpenIndexResponse> open(OpenIndexRequest request) {
        return openIndexAction.execute(request);
    }

    @Override public void open(OpenIndexRequest request, ActionListener<OpenIndexResponse> listener) {
        openIndexAction.execute(request, listener);
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

    @Override public ActionFuture<DeleteMappingResponse> deleteMapping(DeleteMappingRequest request) {
        return deleteMappingAction.execute(request);
    }

    @Override public void deleteMapping(DeleteMappingRequest request, ActionListener<DeleteMappingResponse> listener) {
        deleteMappingAction.execute(request, listener);
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

    @Override public ActionFuture<UpdateSettingsResponse> updateSettings(UpdateSettingsRequest request) {
        return updateSettingsAction.execute(request);
    }

    @Override public void updateSettings(UpdateSettingsRequest request, ActionListener<UpdateSettingsResponse> listener) {
        updateSettingsAction.execute(request, listener);
    }

    @Override public ActionFuture<AnalyzeResponse> analyze(AnalyzeRequest request) {
        return analyzeAction.execute(request);
    }

    @Override public void analyze(AnalyzeRequest request, ActionListener<AnalyzeResponse> listener) {
        analyzeAction.execute(request, listener);
    }

    @Override public ActionFuture<PutIndexTemplateResponse> putTemplate(PutIndexTemplateRequest request) {
        return putIndexTemplateAction.execute(request);
    }

    @Override public void putTemplate(PutIndexTemplateRequest request, ActionListener<PutIndexTemplateResponse> listener) {
        putIndexTemplateAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteIndexTemplateResponse> deleteTemplate(DeleteIndexTemplateRequest request) {
        return deleteIndexTemplateAction.execute(request);
    }

    @Override public void deleteTemplate(DeleteIndexTemplateRequest request, ActionListener<DeleteIndexTemplateResponse> listener) {
        deleteIndexTemplateAction.execute(request, listener);
    }
}
