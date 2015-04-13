/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.client.support;

import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.*;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequestBuilder;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerAction;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerRequest;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerRequestBuilder;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersAction;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersRequest;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersRequestBuilder;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerAction;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerRequest;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerRequestBuilder;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.Nullable;

/**
 *
 */
public abstract class AbstractIndicesAdminClient implements IndicesAdminClient {

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, IndicesAdminClient>> RequestBuilder prepareExecute(final Action<Request, Response, RequestBuilder, IndicesAdminClient> action) {
        return action.newRequestBuilder(this);
    }

    @Override
    public ActionFuture<IndicesExistsResponse> exists(final IndicesExistsRequest request) {
        return execute(IndicesExistsAction.INSTANCE, request);
    }

    @Override
    public void exists(final IndicesExistsRequest request, final ActionListener<IndicesExistsResponse> listener) {
        execute(IndicesExistsAction.INSTANCE, request, listener);
    }

    @Override
    public IndicesExistsRequestBuilder prepareExists(String... indices) {
        return new IndicesExistsRequestBuilder(this, indices);
    }

    @Override
    public ActionFuture<TypesExistsResponse> typesExists(TypesExistsRequest request) {
        return execute(TypesExistsAction.INSTANCE, request);
    }

    @Override
    public void typesExists(TypesExistsRequest request, ActionListener<TypesExistsResponse> listener) {
        execute(TypesExistsAction.INSTANCE, request, listener);
    }

    @Override
    public TypesExistsRequestBuilder prepareTypesExists(String... index) {
        return new TypesExistsRequestBuilder(this, index);
    }

    @Override
    public ActionFuture<IndicesAliasesResponse> aliases(final IndicesAliasesRequest request) {
        return execute(IndicesAliasesAction.INSTANCE, request);
    }

    @Override
    public void aliases(final IndicesAliasesRequest request, final ActionListener<IndicesAliasesResponse> listener) {
        execute(IndicesAliasesAction.INSTANCE, request, listener);
    }

    @Override
    public IndicesAliasesRequestBuilder prepareAliases() {
        return new IndicesAliasesRequestBuilder(this);
    }

    @Override
    public ActionFuture<GetAliasesResponse> getAliases(GetAliasesRequest request) {
        return execute(GetAliasesAction.INSTANCE, request);
    }

    @Override
    public void getAliases(GetAliasesRequest request, ActionListener<GetAliasesResponse> listener) {
        execute(GetAliasesAction.INSTANCE, request, listener);
    }

    @Override
    public GetAliasesRequestBuilder prepareGetAliases(String... aliases) {
        return new GetAliasesRequestBuilder(this, aliases);
    }

    @Override
    public ActionFuture<ClearIndicesCacheResponse> clearCache(final ClearIndicesCacheRequest request) {
        return execute(ClearIndicesCacheAction.INSTANCE, request);
    }

    @Override
    public void aliasesExist(GetAliasesRequest request, ActionListener<AliasesExistResponse> listener) {
        execute(AliasesExistAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<AliasesExistResponse> aliasesExist(GetAliasesRequest request) {
        return execute(AliasesExistAction.INSTANCE, request);
    }

    @Override
    public AliasesExistRequestBuilder prepareAliasesExist(String... aliases) {
        return new AliasesExistRequestBuilder(this, aliases);
    }

    @Override
    public ActionFuture<GetIndexResponse> getIndex(GetIndexRequest request) {
        return execute(GetIndexAction.INSTANCE, request);
    }

    @Override
    public void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener) {
        execute(GetIndexAction.INSTANCE, request, listener);
    }

    @Override
    public GetIndexRequestBuilder prepareGetIndex() {
        return new GetIndexRequestBuilder(this);
    }

    @Override
    public void clearCache(final ClearIndicesCacheRequest request, final ActionListener<ClearIndicesCacheResponse> listener) {
        execute(ClearIndicesCacheAction.INSTANCE, request, listener);
    }

    @Override
    public ClearIndicesCacheRequestBuilder prepareClearCache(String... indices) {
        return new ClearIndicesCacheRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<CreateIndexResponse> create(final CreateIndexRequest request) {
        return execute(CreateIndexAction.INSTANCE, request);
    }

    @Override
    public void create(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
        execute(CreateIndexAction.INSTANCE, request, listener);
    }

    @Override
    public CreateIndexRequestBuilder prepareCreate(String index) {
        return new CreateIndexRequestBuilder(this, index);
    }

    @Override
    public ActionFuture<DeleteIndexResponse> delete(final DeleteIndexRequest request) {
        return execute(DeleteIndexAction.INSTANCE, request);
    }

    @Override
    public void delete(final DeleteIndexRequest request, final ActionListener<DeleteIndexResponse> listener) {
        execute(DeleteIndexAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteIndexRequestBuilder prepareDelete(String... indices) {
        return new DeleteIndexRequestBuilder(this, indices);
    }

    @Override
    public ActionFuture<CloseIndexResponse> close(final CloseIndexRequest request) {
        return execute(CloseIndexAction.INSTANCE, request);
    }

    @Override
    public void close(final CloseIndexRequest request, final ActionListener<CloseIndexResponse> listener) {
        execute(CloseIndexAction.INSTANCE, request, listener);
    }

    @Override
    public CloseIndexRequestBuilder prepareClose(String... indices) {
        return new CloseIndexRequestBuilder(this, indices);
    }

    @Override
    public ActionFuture<OpenIndexResponse> open(final OpenIndexRequest request) {
        return execute(OpenIndexAction.INSTANCE, request);
    }

    @Override
    public void open(final OpenIndexRequest request, final ActionListener<OpenIndexResponse> listener) {
        execute(OpenIndexAction.INSTANCE, request, listener);
    }

    @Override
    public OpenIndexRequestBuilder prepareOpen(String... indices) {
        return new OpenIndexRequestBuilder(this, indices);
    }

    @Override
    public ActionFuture<FlushResponse> flush(final FlushRequest request) {
        return execute(FlushAction.INSTANCE, request);
    }

    @Override
    public void flush(final FlushRequest request, final ActionListener<FlushResponse> listener) {
        execute(FlushAction.INSTANCE, request, listener);
    }

    @Override
    public FlushRequestBuilder prepareFlush(String... indices) {
        return new FlushRequestBuilder(this).setIndices(indices);
    }

    @Override
    public void getMappings(GetMappingsRequest request, ActionListener<GetMappingsResponse> listener) {
        execute(GetMappingsAction.INSTANCE, request, listener);
    }

    @Override
    public void getFieldMappings(GetFieldMappingsRequest request, ActionListener<GetFieldMappingsResponse> listener) {
        execute(GetFieldMappingsAction.INSTANCE, request, listener);
    }

    @Override
    public GetMappingsRequestBuilder prepareGetMappings(String... indices) {
        return new GetMappingsRequestBuilder(this, indices);
    }

    @Override
    public ActionFuture<GetMappingsResponse> getMappings(GetMappingsRequest request) {
        return execute(GetMappingsAction.INSTANCE, request);
    }

    @Override
    public GetFieldMappingsRequestBuilder prepareGetFieldMappings(String... indices) {
        return new GetFieldMappingsRequestBuilder(this, indices);
    }

    @Override
    public ActionFuture<GetFieldMappingsResponse> getFieldMappings(GetFieldMappingsRequest request) {
        return execute(GetFieldMappingsAction.INSTANCE, request);
    }

    @Override
    public ActionFuture<PutMappingResponse> putMapping(final PutMappingRequest request) {
        return execute(PutMappingAction.INSTANCE, request);
    }

    @Override
    public void putMapping(final PutMappingRequest request, final ActionListener<PutMappingResponse> listener) {
        execute(PutMappingAction.INSTANCE, request, listener);
    }

    @Override
    public PutMappingRequestBuilder preparePutMapping(String... indices) {
        return new PutMappingRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<OptimizeResponse> optimize(final OptimizeRequest request) {
        return execute(OptimizeAction.INSTANCE, request);
    }

    @Override
    public void optimize(final OptimizeRequest request, final ActionListener<OptimizeResponse> listener) {
        execute(OptimizeAction.INSTANCE, request, listener);
    }

    @Override
    public OptimizeRequestBuilder prepareOptimize(String... indices) {
        return new OptimizeRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<RefreshResponse> refresh(final RefreshRequest request) {
        return execute(RefreshAction.INSTANCE, request);
    }

    @Override
    public void refresh(final RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        execute(RefreshAction.INSTANCE, request, listener);
    }

    @Override
    public RefreshRequestBuilder prepareRefresh(String... indices) {
        return new RefreshRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<IndicesStatsResponse> stats(final IndicesStatsRequest request) {
        return execute(IndicesStatsAction.INSTANCE, request);
    }

    @Override
    public void stats(final IndicesStatsRequest request, final ActionListener<IndicesStatsResponse> listener) {
        execute(IndicesStatsAction.INSTANCE, request, listener);
    }

    @Override
    public IndicesStatsRequestBuilder prepareStats(String... indices) {
        return new IndicesStatsRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<RecoveryResponse> recoveries(final RecoveryRequest request) {
        return execute(RecoveryAction.INSTANCE, request);
    }

    @Override
    public void recoveries(final RecoveryRequest request, final ActionListener<RecoveryResponse> listener) {
        execute(RecoveryAction.INSTANCE, request, listener);
    }

    @Override
    public RecoveryRequestBuilder prepareRecoveries(String... indices) {
        return new RecoveryRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<IndicesSegmentResponse> segments(final IndicesSegmentsRequest request) {
        return execute(IndicesSegmentsAction.INSTANCE, request);
    }

    @Override
    public void segments(final IndicesSegmentsRequest request, final ActionListener<IndicesSegmentResponse> listener) {
        execute(IndicesSegmentsAction.INSTANCE, request, listener);
    }

    @Override
    public IndicesSegmentsRequestBuilder prepareSegments(String... indices) {
        return new IndicesSegmentsRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<UpdateSettingsResponse> updateSettings(final UpdateSettingsRequest request) {
        return execute(UpdateSettingsAction.INSTANCE, request);
    }

    @Override
    public void updateSettings(final UpdateSettingsRequest request, final ActionListener<UpdateSettingsResponse> listener) {
        execute(UpdateSettingsAction.INSTANCE, request, listener);
    }

    @Override
    public UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices) {
        return new UpdateSettingsRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<AnalyzeResponse> analyze(final AnalyzeRequest request) {
        return execute(AnalyzeAction.INSTANCE, request);
    }

    @Override
    public void analyze(final AnalyzeRequest request, final ActionListener<AnalyzeResponse> listener) {
        execute(AnalyzeAction.INSTANCE, request, listener);
    }

    @Override
    public AnalyzeRequestBuilder prepareAnalyze(@Nullable String index, String text) {
        return new AnalyzeRequestBuilder(this, index, text);
    }

    @Override
    public AnalyzeRequestBuilder prepareAnalyze(String text) {
        return new AnalyzeRequestBuilder(this, null, text);
    }

    @Override
    public ActionFuture<PutIndexTemplateResponse> putTemplate(final PutIndexTemplateRequest request) {
        return execute(PutIndexTemplateAction.INSTANCE, request);
    }

    @Override
    public void putTemplate(final PutIndexTemplateRequest request, final ActionListener<PutIndexTemplateResponse> listener) {
        execute(PutIndexTemplateAction.INSTANCE, request, listener);
    }

    @Override
    public PutIndexTemplateRequestBuilder preparePutTemplate(String name) {
        return new PutIndexTemplateRequestBuilder(this, name);
    }

    @Override
    public ActionFuture<GetIndexTemplatesResponse> getTemplates(final GetIndexTemplatesRequest request) {
        return execute(GetIndexTemplatesAction.INSTANCE, request);
    }

    @Override
    public void getTemplates(final GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
        execute(GetIndexTemplatesAction.INSTANCE, request, listener);
    }

    @Override
    public GetIndexTemplatesRequestBuilder prepareGetTemplates(String... names) {
        return new GetIndexTemplatesRequestBuilder(this, names);
    }

    @Override
    public ActionFuture<DeleteIndexTemplateResponse> deleteTemplate(final DeleteIndexTemplateRequest request) {
        return execute(DeleteIndexTemplateAction.INSTANCE, request);
    }

    @Override
    public void deleteTemplate(final DeleteIndexTemplateRequest request, final ActionListener<DeleteIndexTemplateResponse> listener) {
        execute(DeleteIndexTemplateAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name) {
        return new DeleteIndexTemplateRequestBuilder(this, name);
    }

    @Override
    public ActionFuture<ValidateQueryResponse> validateQuery(final ValidateQueryRequest request) {
        return execute(ValidateQueryAction.INSTANCE, request);
    }

    @Override
    public void validateQuery(final ValidateQueryRequest request, final ActionListener<ValidateQueryResponse> listener) {
        execute(ValidateQueryAction.INSTANCE, request, listener);
    }

    @Override
    public ValidateQueryRequestBuilder prepareValidateQuery(String... indices) {
        return new ValidateQueryRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<PutWarmerResponse> putWarmer(PutWarmerRequest request) {
        return execute(PutWarmerAction.INSTANCE, request);
    }

    @Override
    public void putWarmer(PutWarmerRequest request, ActionListener<PutWarmerResponse> listener) {
        execute(PutWarmerAction.INSTANCE, request, listener);
    }

    @Override
    public PutWarmerRequestBuilder preparePutWarmer(String name) {
        return new PutWarmerRequestBuilder(this, name);
    }

    @Override
    public ActionFuture<DeleteWarmerResponse> deleteWarmer(DeleteWarmerRequest request) {
        return execute(DeleteWarmerAction.INSTANCE, request);
    }

    @Override
    public void deleteWarmer(DeleteWarmerRequest request, ActionListener<DeleteWarmerResponse> listener) {
        execute(DeleteWarmerAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteWarmerRequestBuilder prepareDeleteWarmer() {
        return new DeleteWarmerRequestBuilder(this);
    }

    @Override
    public GetWarmersRequestBuilder prepareGetWarmers(String... indices) {
        return new GetWarmersRequestBuilder(this, indices);
    }

    @Override
    public ActionFuture<GetWarmersResponse> getWarmers(GetWarmersRequest request) {
        return execute(GetWarmersAction.INSTANCE, request);
    }

    @Override
    public void getWarmers(GetWarmersRequest request, ActionListener<GetWarmersResponse> listener) {
        execute(GetWarmersAction.INSTANCE, request, listener);
    }

    @Override
    public GetSettingsRequestBuilder prepareGetSettings(String... indices) {
        return new GetSettingsRequestBuilder(this, indices);
    }

    @Override
    public ActionFuture<GetSettingsResponse> getSettings(GetSettingsRequest request) {
        return execute(GetSettingsAction.INSTANCE, request);
    }

    @Override
    public void getSettings(GetSettingsRequest request, ActionListener<GetSettingsResponse> listener) {
        execute(GetSettingsAction.INSTANCE, request, listener);
    }
}
