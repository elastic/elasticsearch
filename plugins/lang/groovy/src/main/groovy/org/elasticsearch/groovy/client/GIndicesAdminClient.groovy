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

package org.elasticsearch.groovy.client

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.action.admin.indices.flush.FlushRequest
import org.elasticsearch.action.admin.indices.flush.FlushResponse
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse
import org.elasticsearch.action.admin.indices.stats.IndicesStats
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.client.action.admin.indices.alias.IndicesAliasesRequestBuilder
import org.elasticsearch.client.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.client.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder
import org.elasticsearch.client.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.client.action.admin.indices.delete.DeleteIndexRequestBuilder
import org.elasticsearch.client.action.admin.indices.flush.FlushRequestBuilder
import org.elasticsearch.client.action.admin.indices.gateway.snapshot.GatewaySnapshotRequestBuilder
import org.elasticsearch.client.action.admin.indices.mapping.put.PutMappingRequestBuilder
import org.elasticsearch.client.action.admin.indices.optimize.OptimizeRequestBuilder
import org.elasticsearch.client.action.admin.indices.refresh.RefreshRequestBuilder
import org.elasticsearch.client.action.admin.indices.settings.UpdateSettingsRequestBuilder
import org.elasticsearch.client.action.admin.indices.stats.IndicesStatsRequestBuilder
import org.elasticsearch.client.action.admin.indices.status.IndicesStatusRequestBuilder
import org.elasticsearch.client.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder
import org.elasticsearch.client.action.admin.indices.template.put.PutIndexTemplateRequestBuilder
import org.elasticsearch.client.internal.InternalClient
import org.elasticsearch.groovy.client.action.GActionFuture
import org.elasticsearch.groovy.common.xcontent.GXContentBuilder

/**
 * @author kimchy (shay.banon)
 */
class GIndicesAdminClient {

    static {
        CreateIndexRequest.metaClass.setSettings = {Closure c ->
            delegate.settings(new GXContentBuilder().buildAsString(c))
        }
        CreateIndexRequest.metaClass.settings = {Closure c ->
            delegate.settings(new GXContentBuilder().buildAsString(c))
        }
        CreateIndexRequest.metaClass.mapping = {String type, Closure c ->
            delegate.mapping(type, new GXContentBuilder().buildAsString(c))
        }
        CreateIndexRequest.metaClass.setMapping = {String type, Closure c ->
            delegate.mapping(type, new GXContentBuilder().buildAsString(c))
        }
        CreateIndexRequestBuilder.metaClass.setSettings = {Closure c ->
            delegate.setSettings(new GXContentBuilder().buildAsString(c))
        }
        CreateIndexRequestBuilder.metaClass.settings = {Closure c ->
            delegate.setSettings(new GXContentBuilder().buildAsString(c))
        }
        CreateIndexRequestBuilder.metaClass.addMapping = {String type, Closure c ->
            delegate.addMapping(type, new GXContentBuilder().buildAsString(c))
        }

        PutMappingRequest.metaClass.setSource = {Closure c ->
            delegate.source(new GXContentBuilder().buildAsString(c))
        }
        PutMappingRequest.metaClass.source = {Closure c ->
            delegate.source(new GXContentBuilder().buildAsString(c))
        }
        PutMappingRequestBuilder.metaClass.setSource = {Closure c ->
            delegate.setSource(new GXContentBuilder().buildAsString(c))
        }
        PutMappingRequestBuilder.metaClass.source = {Closure c ->
            delegate.setSource(new GXContentBuilder().buildAsString(c))
        }

        UpdateSettingsRequest.metaClass.setSettings = {Closure c ->
            delegate.settings(new GXContentBuilder().buildAsString(c))
        }
        UpdateSettingsRequest.metaClass.settings = {Closure c ->
            delegate.settings(new GXContentBuilder().buildAsString(c))
        }
        UpdateSettingsRequestBuilder.metaClass.setSettings = {Closure c ->
            delegate.setSettings(new GXContentBuilder().buildAsString(c))
        }
        UpdateSettingsRequestBuilder.metaClass.settings = {Closure c ->
            delegate.setSettings(new GXContentBuilder().buildAsString(c))
        }
    }

    private final GClient gClient

    private final InternalClient internalClient

    final IndicesAdminClient indicesAdminClient

    def GIndicesAdminClient(gClient) {
        this.gClient = gClient
        this.internalClient = gClient.client
        this.indicesAdminClient = internalClient.admin().indices()
    }

    // STATUS

    IndicesStatusRequestBuilder prepareStatus(String... indices) {
        indicesAdminClient.prepareStatus(indices)
    }

    GActionFuture<IndicesStatusResponse> status(Closure c) {
        IndicesStatusRequest request = new IndicesStatusRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        status(request)
    }

    GActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request) {
        GActionFuture<IndicesStatusResponse> future = new GActionFuture<IndicesStatusResponse>(internalClient.threadPool(), request)
        indicesAdminClient.status(request, future)
        return future
    }

    void status(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        indicesAdminClient.status(request, listener)
    }

    // STATS

    IndicesStatsRequestBuilder prepareStats(String... indices) {
        indicesAdminClient.prepareStats(indices)
    }

    GActionFuture<IndicesStats> stats(Closure c) {
        IndicesStatsRequest request = new IndicesStatsRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        stats(request)
    }

    GActionFuture<IndicesStats> stats(IndicesStatsRequest request) {
        GActionFuture<IndicesStats> future = new GActionFuture<IndicesStats>(internalClient.threadPool(), request)
        indicesAdminClient.stats(request, future)
        return future
    }

    void stats(IndicesStatsRequest request, ActionListener<IndicesStats> listener) {
        indicesAdminClient.stats(request, listener)
    }

    // CREATE

    CreateIndexRequestBuilder prepareCreate(String index) {
        indicesAdminClient.prepareCreate(index)
    }

    GActionFuture<CreateIndexResponse> create(Closure c) {
        CreateIndexRequest request = new CreateIndexRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        create(request)
    }

    GActionFuture<CreateIndexResponse> create(CreateIndexRequest request) {
        GActionFuture<CreateIndexResponse> future = new GActionFuture<CreateIndexResponse>(internalClient.threadPool(), request)
        indicesAdminClient.create(request, future)
        return future
    }

    void create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        indicesAdminClient.create(request, listener)
    }

    // DELETE

    DeleteIndexRequestBuilder prepareDelete(String index) {
        indicesAdminClient.prepareDelete(index)
    }

    GActionFuture<DeleteIndexResponse> delete(Closure c) {
        DeleteIndexRequest request = new DeleteIndexRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        delete(request)
    }

    GActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request) {
        GActionFuture<DeleteIndexResponse> future = new GActionFuture<DeleteIndexResponse>(internalClient.threadPool(), request)
        indicesAdminClient.delete(request, future)
        return future
    }

    void delete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        indicesAdminClient.delete(request, listener)
    }

    // REFRESH

    RefreshRequestBuilder prepareRefresh(String... indices) {
        indicesAdminClient.prepareRefresh(indices)
    }

    GActionFuture<RefreshResponse> refresh(Closure c) {
        RefreshRequest request = new RefreshRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        refresh(request)
    }

    GActionFuture<RefreshResponse> refresh(RefreshRequest request) {
        GActionFuture<RefreshResponse> future = new GActionFuture<RefreshResponse>(internalClient.threadPool(), request)
        indicesAdminClient.refresh(request, future)
        return future
    }

    void refresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        indicesAdminClient.refresh(request, listener)
    }

    // FLUSH

    FlushRequestBuilder prepareFlush(String... indices) {
        indicesAdminClient.prepareFlush(indices)
    }

    GActionFuture<FlushResponse> flush(Closure c) {
        FlushRequest request = new FlushRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        flush(request)
    }

    GActionFuture<FlushResponse> flush(FlushRequest request) {
        GActionFuture<FlushResponse> future = new GActionFuture<FlushResponse>(internalClient.threadPool(), request)
        indicesAdminClient.flush(request, future)
        return future
    }

    void flush(FlushRequest request, ActionListener<FlushResponse> listener) {
        indicesAdminClient.flush(request, listener)
    }

    // OPTIMIZE

    OptimizeRequestBuilder prepareOptimize(String... indices) {
        indicesAdminClient.prepareOptimize(indices)
    }

    GActionFuture<OptimizeResponse> optimize(Closure c) {
        OptimizeRequest request = new OptimizeRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        optimize(request)
    }

    GActionFuture<OptimizeResponse> optimize(OptimizeRequest request) {
        GActionFuture<OptimizeResponse> future = new GActionFuture<OptimizeResponse>(internalClient.threadPool(), request)
        indicesAdminClient.optimize(request, future)
        return future
    }

    void optimize(OptimizeRequest request, ActionListener<OptimizeResponse> listener) {
        indicesAdminClient.optimize(request, listener)
    }

    // PUT MAPPING

    PutMappingRequestBuilder preparePutMapping(String... indices) {
        indicesAdminClient.preparePutMapping(indices)
    }

    GActionFuture<PutMappingResponse> putMapping(Closure c) {
        PutMappingRequest request = new PutMappingRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        putMapping(request)
    }

    GActionFuture<PutMappingResponse> putMapping(PutMappingRequest request) {
        GActionFuture<PutMappingResponse> future = new GActionFuture<PutMappingResponse>(internalClient.threadPool(), request)
        indicesAdminClient.putMapping(request, future)
        return future
    }

    void putMapping(PutMappingRequest request, ActionListener<PutMappingResponse> listener) {
        indicesAdminClient.putMapping(request, listener)
    }

    // GATEWAY SNAPSHOT

    GatewaySnapshotRequestBuilder prepareGatewaySnapshot(String... indices) {
        indicesAdminClient.prepareGatewaySnapshot(indices)
    }

    GActionFuture<GatewaySnapshotResponse> gatewaySnapshot(Closure c) {
        GatewaySnapshotRequest request = new GatewaySnapshotRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        gatewaySnapshot(request)
    }

    GActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request) {
        GActionFuture<GatewaySnapshotResponse> future = new GActionFuture<GatewaySnapshotResponse>(internalClient.threadPool(), request)
        indicesAdminClient.gatewaySnapshot(request, future)
        return future
    }

    void gatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener) {
        indicesAdminClient.gatewaySnapshot(request, listener)
    }

    // Aliases

    IndicesAliasesRequestBuilder prepareAliases() {
        indicesAdminClient.prepareAliases()
    }

    GActionFuture<IndicesAliasesResponse> aliases(Closure c) {
        IndicesAliasesRequest request = new IndicesAliasesRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        aliases(request)
    }

    GActionFuture<IndicesAliasesResponse> aliases(IndicesAliasesRequest request) {
        GActionFuture<IndicesAliasesResponse> future = new GActionFuture<IndicesAliasesResponse>(internalClient.threadPool(), request)
        indicesAdminClient.aliases(request, future)
        return future
    }

    void aliases(IndicesAliasesRequest request, ActionListener<IndicesAliasesResponse> listener) {
        indicesAdminClient.aliases(request, listener)
    }

    void aliases(ClearIndicesCacheRequest request, ActionListener<ClearIndicesCacheResponse> listener) {
        indicesAdminClient.clearCache(request, listener)
    }

    // CLEAR CACHE

    ClearIndicesCacheRequestBuilder prepareClearCache(String... indices) {
        indicesAdminClient.prepareClearCache(indices)
    }

    GActionFuture<ClearIndicesCacheResponse> clearCache(Closure c) {
        ClearIndicesCacheRequest request = new ClearIndicesCacheRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        clearCache(request)
    }

    GActionFuture<ClearIndicesCacheResponse> clearCache(ClearIndicesCacheRequest request) {
        GActionFuture<ClearIndicesCacheResponse> future = new GActionFuture<ClearIndicesCacheResponse>(internalClient.threadPool(), request)
        indicesAdminClient.clearCache(request, future)
        return future
    }

    // UPDATE SETTINGS

    UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices) {
        indicesAdminClient.prepareUpdateSettings(indices)
    }

    GActionFuture<UpdateSettingsResponse> updateSettings(Closure c) {
        UpdateSettingsRequest request = new UpdateSettingsRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        updateSettings(request)
    }

    GActionFuture<UpdateSettingsResponse> updateSettings(UpdateSettingsRequest request) {
        GActionFuture<UpdateSettingsResponse> future = new GActionFuture<UpdateSettingsResponse>(internalClient.threadPool(), request)
        indicesAdminClient.updateSettings(request, future)
        return future
    }

    // ANALYZE

    AnalyzeRequestBuilder prepareAnalyze(String index, String text) {
        indicesAdminClient.prepareAnalyze(index, text)
    }

    GActionFuture<AnalyzeResponse> analyze(Closure c) {
        AnalyzeRequest request = new AnalyzeRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        analyze(request)
    }

    GActionFuture<AnalyzeResponse> analyze(AnalyzeRequest request) {
        GActionFuture<AnalyzeResponse> future = new GActionFuture<AnalyzeResponse>(internalClient.threadPool(), request)
        indicesAdminClient.analyze(request, future)
        return future
    }

    // PUT INDEX TEMPLATE

    PutIndexTemplateRequestBuilder preparePutTemplate(String name) {
        indicesAdminClient.preparePutTemplate(name)
    }

    GActionFuture<PutIndexTemplateResponse> putTemplate(Closure c) {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        putTemplate(request)
    }

    GActionFuture<PutIndexTemplateResponse> putTemplate(PutIndexTemplateRequest request) {
        GActionFuture<PutIndexTemplateResponse> future = new GActionFuture<PutIndexTemplateResponse>(internalClient.threadPool(), request)
        indicesAdminClient.putTemplate(request, future)
        return future
    }

    // DELETE INDEX TEMPLATE

    DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name) {
        indicesAdminClient.prepareDeleteTemplate(name)
    }

    GActionFuture<DeleteIndexTemplateResponse> deleteTemplate(Closure c) {
        DeleteIndexTemplateRequest request = new DeleteIndexTemplateRequest()
        c.setDelegate request
        c.resolveStrategy = gClient.resolveStrategy
        c.call()
        deleteTemplate(request)
    }

    GActionFuture<DeleteIndexTemplateResponse> deleteTemplate(DeleteIndexTemplateRequest request) {
        GActionFuture<DeleteIndexTemplateResponse> future = new GActionFuture<DeleteIndexTemplateResponse>(internalClient.threadPool(), request)
        indicesAdminClient.deleteTemplate(request, future)
        return future
    }
}
