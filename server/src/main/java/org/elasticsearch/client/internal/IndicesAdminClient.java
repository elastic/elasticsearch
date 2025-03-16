/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.elasticsearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequestBuilder;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequestBuilder;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequestBuilder;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#indices()
 */
public class IndicesAdminClient implements ElasticsearchClient {

    protected final ElasticsearchClient client;

    public IndicesAdminClient(ElasticsearchClient client) {
        this.client = client;
    }

    public ActionFuture<IndicesStatsResponse> stats(final IndicesStatsRequest request) {
        return execute(IndicesStatsAction.INSTANCE, request);
    }

    public void stats(final IndicesStatsRequest request, final ActionListener<IndicesStatsResponse> listener) {
        execute(IndicesStatsAction.INSTANCE, request, listener);
    }

    public IndicesStatsRequestBuilder prepareStats(String... indices) {
        return new IndicesStatsRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<RecoveryResponse> recoveries(final RecoveryRequest request) {
        return execute(RecoveryAction.INSTANCE, request);
    }

    public void recoveries(final RecoveryRequest request, final ActionListener<RecoveryResponse> listener) {
        execute(RecoveryAction.INSTANCE, request, listener);
    }

    public RecoveryRequestBuilder prepareRecoveries(String... indices) {
        return new RecoveryRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<IndicesSegmentResponse> segments(final IndicesSegmentsRequest request) {
        return execute(IndicesSegmentsAction.INSTANCE, request);
    }

    public void segments(final IndicesSegmentsRequest request, final ActionListener<IndicesSegmentResponse> listener) {
        execute(IndicesSegmentsAction.INSTANCE, request, listener);
    }

    public IndicesSegmentsRequestBuilder prepareSegments(String... indices) {
        return new IndicesSegmentsRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<CreateIndexResponse> create(final CreateIndexRequest request) {
        return execute(TransportCreateIndexAction.TYPE, request);
    }

    public void create(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
        execute(TransportCreateIndexAction.TYPE, request, listener);
    }

    public CreateIndexRequestBuilder prepareCreate(String index) {
        return new CreateIndexRequestBuilder(this, index);
    }

    public ActionFuture<AcknowledgedResponse> delete(final DeleteIndexRequest request) {
        return execute(TransportDeleteIndexAction.TYPE, request);
    }

    public void delete(final DeleteIndexRequest request, final ActionListener<AcknowledgedResponse> listener) {
        execute(TransportDeleteIndexAction.TYPE, request, listener);
    }

    public DeleteIndexRequestBuilder prepareDelete(String... indices) {
        return new DeleteIndexRequestBuilder(this, indices);
    }

    public ActionFuture<CloseIndexResponse> close(final CloseIndexRequest request) {
        return execute(TransportCloseIndexAction.TYPE, request);
    }

    public void close(final CloseIndexRequest request, final ActionListener<CloseIndexResponse> listener) {
        execute(TransportCloseIndexAction.TYPE, request, listener);
    }

    public CloseIndexRequestBuilder prepareClose(String... indices) {
        return new CloseIndexRequestBuilder(this, indices);
    }

    public ActionFuture<OpenIndexResponse> open(final OpenIndexRequest request) {
        return execute(OpenIndexAction.INSTANCE, request);
    }

    public void open(final OpenIndexRequest request, final ActionListener<OpenIndexResponse> listener) {
        execute(OpenIndexAction.INSTANCE, request, listener);
    }

    public AddIndexBlockRequestBuilder prepareAddBlock(APIBlock block, String... indices) {
        return new AddIndexBlockRequestBuilder(this, block, indices);
    }

    public void addBlock(AddIndexBlockRequest request, ActionListener<AddIndexBlockResponse> listener) {
        execute(TransportAddIndexBlockAction.TYPE, request, listener);
    }

    public OpenIndexRequestBuilder prepareOpen(String... indices) {
        return new OpenIndexRequestBuilder(this, indices);
    }

    public ActionFuture<BroadcastResponse> refresh(final RefreshRequest request) {
        return execute(RefreshAction.INSTANCE, request);
    }

    public void refresh(final RefreshRequest request, final ActionListener<BroadcastResponse> listener) {
        execute(RefreshAction.INSTANCE, request, listener);
    }

    public RefreshRequestBuilder prepareRefresh(String... indices) {
        return new RefreshRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<BroadcastResponse> flush(final FlushRequest request) {
        return execute(FlushAction.INSTANCE, request);
    }

    public void flush(final FlushRequest request, final ActionListener<BroadcastResponse> listener) {
        execute(FlushAction.INSTANCE, request, listener);
    }

    public FlushRequestBuilder prepareFlush(String... indices) {
        return new FlushRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<BroadcastResponse> forceMerge(final ForceMergeRequest request) {
        return execute(ForceMergeAction.INSTANCE, request);
    }

    public void forceMerge(final ForceMergeRequest request, final ActionListener<BroadcastResponse> listener) {
        execute(ForceMergeAction.INSTANCE, request, listener);
    }

    public ForceMergeRequestBuilder prepareForceMerge(String... indices) {
        return new ForceMergeRequestBuilder(this).setIndices(indices);
    }

    public void getMappings(GetMappingsRequest request, ActionListener<GetMappingsResponse> listener) {
        execute(GetMappingsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetMappingsResponse> getMappings(GetMappingsRequest request) {
        return execute(GetMappingsAction.INSTANCE, request);
    }

    public GetMappingsRequestBuilder prepareGetMappings(TimeValue masterTimeout, String... indices) {
        return new GetMappingsRequestBuilder(this, masterTimeout, indices);
    }

    public void getFieldMappings(GetFieldMappingsRequest request, ActionListener<GetFieldMappingsResponse> listener) {
        execute(GetFieldMappingsAction.INSTANCE, request, listener);
    }

    public GetFieldMappingsRequestBuilder prepareGetFieldMappings(String... indices) {
        return new GetFieldMappingsRequestBuilder(this, indices);
    }

    public ActionFuture<GetFieldMappingsResponse> getFieldMappings(GetFieldMappingsRequest request) {
        return execute(GetFieldMappingsAction.INSTANCE, request);
    }

    public ActionFuture<AcknowledgedResponse> putMapping(final PutMappingRequest request) {
        return execute(TransportPutMappingAction.TYPE, request);
    }

    public void putMapping(final PutMappingRequest request, final ActionListener<AcknowledgedResponse> listener) {
        execute(TransportPutMappingAction.TYPE, request, listener);
    }

    public PutMappingRequestBuilder preparePutMapping(String... indices) {
        return new PutMappingRequestBuilder(this).setIndices(indices);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
        ActionType<Response> action,
        Request request
    ) {
        return client.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        client.execute(action, request, listener);
    }

    @Override
    public ThreadPool threadPool() {
        return client.threadPool();
    }

    public ActionFuture<IndicesAliasesResponse> aliases(final IndicesAliasesRequest request) {
        return execute(TransportIndicesAliasesAction.TYPE, request);
    }

    public void aliases(final IndicesAliasesRequest request, final ActionListener<IndicesAliasesResponse> listener) {
        execute(TransportIndicesAliasesAction.TYPE, request, listener);
    }

    public IndicesAliasesRequestBuilder prepareAliases(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
        return new IndicesAliasesRequestBuilder(this, masterNodeTimeout, ackTimeout);
    }

    public ActionFuture<GetAliasesResponse> getAliases(GetAliasesRequest request) {
        return execute(GetAliasesAction.INSTANCE, request);
    }

    public void getAliases(GetAliasesRequest request, ActionListener<GetAliasesResponse> listener) {
        execute(GetAliasesAction.INSTANCE, request, listener);
    }

    public GetAliasesRequestBuilder prepareGetAliases(TimeValue masterTimeout, String... aliases) {
        return new GetAliasesRequestBuilder(this, masterTimeout, aliases);
    }

    public ActionFuture<GetIndexResponse> getIndex(GetIndexRequest request) {
        return execute(GetIndexAction.INSTANCE, request);
    }

    public void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener) {
        execute(GetIndexAction.INSTANCE, request, listener);
    }

    public GetIndexRequestBuilder prepareGetIndex(TimeValue masterTimeout) {
        return new GetIndexRequestBuilder(this, masterTimeout);
    }

    public ActionFuture<BroadcastResponse> clearCache(final ClearIndicesCacheRequest request) {
        return execute(TransportClearIndicesCacheAction.TYPE, request);
    }

    public void clearCache(final ClearIndicesCacheRequest request, final ActionListener<BroadcastResponse> listener) {
        execute(TransportClearIndicesCacheAction.TYPE, request, listener);
    }

    public ClearIndicesCacheRequestBuilder prepareClearCache(String... indices) {
        return new ClearIndicesCacheRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<AcknowledgedResponse> updateSettings(final UpdateSettingsRequest request) {
        return execute(TransportUpdateSettingsAction.TYPE, request);
    }

    public void updateSettings(final UpdateSettingsRequest request, final ActionListener<AcknowledgedResponse> listener) {
        execute(TransportUpdateSettingsAction.TYPE, request, listener);
    }

    public UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices) {
        return new UpdateSettingsRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<AnalyzeAction.Response> analyze(final AnalyzeAction.Request request) {
        return execute(AnalyzeAction.INSTANCE, request);
    }

    public void analyze(final AnalyzeAction.Request request, final ActionListener<AnalyzeAction.Response> listener) {
        execute(AnalyzeAction.INSTANCE, request, listener);
    }

    public AnalyzeRequestBuilder prepareAnalyze(@Nullable String index, String text) {
        return new AnalyzeRequestBuilder(this, index, text);
    }

    public AnalyzeRequestBuilder prepareAnalyze(String text) {
        return new AnalyzeRequestBuilder(this, null, text);
    }

    public AnalyzeRequestBuilder prepareAnalyze() {
        return new AnalyzeRequestBuilder(this);
    }

    public ActionFuture<AcknowledgedResponse> putTemplate(final PutIndexTemplateRequest request) {
        return execute(TransportPutIndexTemplateAction.TYPE, request);
    }

    public void putTemplate(final PutIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        execute(TransportPutIndexTemplateAction.TYPE, request, listener);
    }

    public PutIndexTemplateRequestBuilder preparePutTemplate(String name) {
        return new PutIndexTemplateRequestBuilder(this, name);
    }

    public void deleteTemplate(final DeleteIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        execute(TransportDeleteIndexTemplateAction.TYPE, request, listener);
    }

    public DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name) {
        return new DeleteIndexTemplateRequestBuilder(this, name);
    }

    public void getTemplates(final GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
        execute(GetIndexTemplatesAction.INSTANCE, request, listener);
    }

    public GetIndexTemplatesRequestBuilder prepareGetTemplates(TimeValue masterTimeout, String... names) {
        return new GetIndexTemplatesRequestBuilder(this, masterTimeout, names);
    }

    public ActionFuture<ValidateQueryResponse> validateQuery(final ValidateQueryRequest request) {
        return execute(ValidateQueryAction.INSTANCE, request);
    }

    public void validateQuery(final ValidateQueryRequest request, final ActionListener<ValidateQueryResponse> listener) {
        execute(ValidateQueryAction.INSTANCE, request, listener);
    }

    public ValidateQueryRequestBuilder prepareValidateQuery(String... indices) {
        return new ValidateQueryRequestBuilder(this).setIndices(indices);
    }

    public void getSettings(GetSettingsRequest request, ActionListener<GetSettingsResponse> listener) {
        execute(GetSettingsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetSettingsResponse> getSettings(GetSettingsRequest request) {
        return execute(GetSettingsAction.INSTANCE, request);
    }

    public GetSettingsRequestBuilder prepareGetSettings(TimeValue masterTimeout, String... indices) {
        return new GetSettingsRequestBuilder(this, masterTimeout, indices);
    }

    public ResizeRequestBuilder prepareResizeIndex(String sourceIndex, String targetIndex) {
        return new ResizeRequestBuilder(this).setSourceIndex(sourceIndex).setTargetIndex(new CreateIndexRequest(targetIndex));
    }

    public void resizeIndex(ResizeRequest request, ActionListener<CreateIndexResponse> listener) {
        execute(ResizeAction.INSTANCE, request, listener);
    }

    public RolloverRequestBuilder prepareRolloverIndex(String alias) {
        return new RolloverRequestBuilder(this).setRolloverTarget(alias);
    }

    public ActionFuture<RolloverResponse> rolloverIndex(RolloverRequest request) {
        return execute(RolloverAction.INSTANCE, request);
    }

    public void rolloverIndex(RolloverRequest request, ActionListener<RolloverResponse> listener) {
        execute(RolloverAction.INSTANCE, request, listener);
    }

    public void resolveIndex(ResolveIndexAction.Request request, ActionListener<ResolveIndexAction.Response> listener) {
        execute(ResolveIndexAction.INSTANCE, request, listener);
    }

    public ActionFuture<ResolveIndexAction.Response> resolveIndex(ResolveIndexAction.Request request) {
        return execute(ResolveIndexAction.INSTANCE, request);
    }
}
