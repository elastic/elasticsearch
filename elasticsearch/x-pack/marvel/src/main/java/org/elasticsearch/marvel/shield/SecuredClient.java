/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequestBuilder;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.cluster.validate.template.RenderSearchTemplateAction;
import org.elasticsearch.action.admin.cluster.validate.template.RenderSearchTemplateRequest;
import org.elasticsearch.action.admin.cluster.validate.template.RenderSearchTemplateRequestBuilder;
import org.elasticsearch.action.admin.cluster.validate.template.RenderSearchTemplateResponse;
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
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
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
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoreRequestBuilder;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
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
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusRequestBuilder;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequestBuilder;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeResponse;
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
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldstats.FieldStatsAction;
import org.elasticsearch.action.fieldstats.FieldStatsRequest;
import org.elasticsearch.action.fieldstats.FieldStatsRequestBuilder;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.percolate.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.termvectors.*;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 *
 */
public class SecuredClient implements Client {

    private final Client client;
    private final Admin admin;
    private MarvelShieldIntegration shieldIntegration;

    @Inject
    public SecuredClient(Client client, MarvelShieldIntegration shieldIntegration) {
        this.client = client;
        this.shieldIntegration = shieldIntegration;
        this.admin = new Admin(this.client, this.shieldIntegration);
    }

    @Override
    public AdminClient admin() {
        return admin;
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public ThreadPool threadPool() {
        return client.threadPool();
    }

    @Override
    public Headers headers() {
        return client.headers();
    }

    @Override
    public Settings settings() {
        return client.settings();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
        shieldIntegration.bindInternalMarvelUser(request);
        return client.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> actionListener) {
        shieldIntegration.bindInternalMarvelUser(request);
        client.execute(action, request, actionListener);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
        return action.newRequestBuilder(this);
    }

    public ActionFuture<IndexResponse> index(IndexRequest request) {
        return this.execute(IndexAction.INSTANCE, request);
    }

    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        this.execute(IndexAction.INSTANCE, request, listener);
    }

    public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, IndexAction.INSTANCE, null);
    }

    public IndexRequestBuilder prepareIndex(String index, String type) {
        return this.prepareIndex(index, type, null);
    }

    public IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id) {
        return this.prepareIndex().setIndex(index).setType(type).setId(id);
    }

    public ActionFuture<UpdateResponse> update(UpdateRequest request) {
        return this.execute(UpdateAction.INSTANCE, request);
    }

    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        this.execute(UpdateAction.INSTANCE, request, listener);
    }

    public UpdateRequestBuilder prepareUpdate() {
        return new UpdateRequestBuilder(this, UpdateAction.INSTANCE, null, null, null);
    }

    public UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return new UpdateRequestBuilder(this, UpdateAction.INSTANCE, index, type, id);
    }

    public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return this.execute(DeleteAction.INSTANCE, request);
    }

    public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        this.execute(DeleteAction.INSTANCE, request, listener);
    }

    public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this, DeleteAction.INSTANCE, null);
    }

    public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return this.prepareDelete().setIndex(index).setType(type).setId(id);
    }

    public ActionFuture<BulkResponse> bulk(BulkRequest request) {
        return this.execute(BulkAction.INSTANCE, request);
    }

    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        this.execute(BulkAction.INSTANCE, request, listener);
    }

    public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this, BulkAction.INSTANCE);
    }

    public ActionFuture<GetResponse> get(GetRequest request) {
        return this.execute(GetAction.INSTANCE, request);
    }

    public void get(GetRequest request, ActionListener<GetResponse> listener) {
        this.execute(GetAction.INSTANCE, request, listener);
    }

    public GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this, GetAction.INSTANCE, null);
    }

    public GetRequestBuilder prepareGet(String index, String type, String id) {
        return this.prepareGet().setIndex(index).setType(type).setId(id);
    }

    public ActionFuture<GetIndexedScriptResponse> getIndexedScript(GetIndexedScriptRequest request) {
        return this.execute(GetIndexedScriptAction.INSTANCE, request);
    }

    public void getIndexedScript(GetIndexedScriptRequest request, ActionListener<GetIndexedScriptResponse> listener) {
        this.execute(GetIndexedScriptAction.INSTANCE, request, listener);
    }

    public GetIndexedScriptRequestBuilder prepareGetIndexedScript() {
        return new GetIndexedScriptRequestBuilder(this, GetIndexedScriptAction.INSTANCE);
    }

    public GetIndexedScriptRequestBuilder prepareGetIndexedScript(String scriptLang, String id) {
        return this.prepareGetIndexedScript().setScriptLang(scriptLang).setId(id);
    }

    public PutIndexedScriptRequestBuilder preparePutIndexedScript() {
        return new PutIndexedScriptRequestBuilder(this, PutIndexedScriptAction.INSTANCE);
    }

    public PutIndexedScriptRequestBuilder preparePutIndexedScript(@Nullable String scriptLang, String id, String source) {
        return PutIndexedScriptAction.INSTANCE.newRequestBuilder(this).setScriptLang(scriptLang).setId(id).setSource(source);
    }

    public void putIndexedScript(PutIndexedScriptRequest request, ActionListener<PutIndexedScriptResponse> listener) {
        this.execute(PutIndexedScriptAction.INSTANCE, request, listener);
    }

    public ActionFuture<PutIndexedScriptResponse> putIndexedScript(PutIndexedScriptRequest request) {
        return this.execute(PutIndexedScriptAction.INSTANCE, request);
    }

    public void deleteIndexedScript(DeleteIndexedScriptRequest request, ActionListener<DeleteIndexedScriptResponse> listener) {
        this.execute(DeleteIndexedScriptAction.INSTANCE, request, listener);
    }

    public ActionFuture<DeleteIndexedScriptResponse> deleteIndexedScript(DeleteIndexedScriptRequest request) {
        return this.execute(DeleteIndexedScriptAction.INSTANCE, request);
    }

    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript() {
        return DeleteIndexedScriptAction.INSTANCE.newRequestBuilder(this);
    }

    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript(@Nullable String scriptLang, String id) {
        return this.prepareDeleteIndexedScript().setScriptLang(scriptLang).setId(id);
    }

    public ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request) {
        return this.execute(MultiGetAction.INSTANCE, request);
    }

    public void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        this.execute(MultiGetAction.INSTANCE, request, listener);
    }

    public MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this, MultiGetAction.INSTANCE);
    }

    public ActionFuture<SearchResponse> search(SearchRequest request) {
        return this.execute(SearchAction.INSTANCE, request);
    }

    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        this.execute(SearchAction.INSTANCE, request, listener);
    }

    public SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(this, SearchAction.INSTANCE).setIndices(indices);
    }

    public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return this.execute(SearchScrollAction.INSTANCE, request);
    }

    public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        this.execute(SearchScrollAction.INSTANCE, request, listener);
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return new SearchScrollRequestBuilder(this, SearchScrollAction.INSTANCE, scrollId);
    }

    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return this.execute(MultiSearchAction.INSTANCE, request);
    }

    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        this.execute(MultiSearchAction.INSTANCE, request, listener);
    }

    public MultiSearchRequestBuilder prepareMultiSearch() {
        return new MultiSearchRequestBuilder(this, MultiSearchAction.INSTANCE);
    }

    public ActionFuture<SuggestResponse> suggest(SuggestRequest request) {
        return this.execute(SuggestAction.INSTANCE, request);
    }

    public void suggest(SuggestRequest request, ActionListener<SuggestResponse> listener) {
        this.execute(SuggestAction.INSTANCE, request, listener);
    }

    public SuggestRequestBuilder prepareSuggest(String... indices) {
        return new SuggestRequestBuilder(this, SuggestAction.INSTANCE).setIndices(indices);
    }

    public ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest request) {
        return this.execute(TermVectorsAction.INSTANCE, request);
    }

    public void termVectors(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        this.execute(TermVectorsAction.INSTANCE, request, listener);
    }

    public TermVectorsRequestBuilder prepareTermVectors() {
        return new TermVectorsRequestBuilder(this, TermVectorsAction.INSTANCE);
    }

    public TermVectorsRequestBuilder prepareTermVectors(String index, String type, String id) {
        return new TermVectorsRequestBuilder(this, TermVectorsAction.INSTANCE, index, type, id);
    }

    /** @deprecated */
    @Deprecated
    public ActionFuture<TermVectorsResponse> termVector(TermVectorsRequest request) {
        return this.termVectors(request);
    }

    /** @deprecated */
    @Deprecated
    public void termVector(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        this.termVectors(request, listener);
    }

    /** @deprecated */
    @Deprecated
    public TermVectorsRequestBuilder prepareTermVector() {
        return this.prepareTermVectors();
    }

    /** @deprecated */
    @Deprecated
    public TermVectorsRequestBuilder prepareTermVector(String index, String type, String id) {
        return this.prepareTermVectors(index, type, id);
    }

    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest request) {
        return this.execute(MultiTermVectorsAction.INSTANCE, request);
    }

    public void multiTermVectors(MultiTermVectorsRequest request, ActionListener<MultiTermVectorsResponse> listener) {
        this.execute(MultiTermVectorsAction.INSTANCE, request, listener);
    }

    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return new MultiTermVectorsRequestBuilder(this, MultiTermVectorsAction.INSTANCE);
    }

    public ActionFuture<PercolateResponse> percolate(PercolateRequest request) {
        return this.execute(PercolateAction.INSTANCE, request);
    }

    public void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        this.execute(PercolateAction.INSTANCE, request, listener);
    }

    public PercolateRequestBuilder preparePercolate() {
        return new PercolateRequestBuilder(this, PercolateAction.INSTANCE);
    }

    public MultiPercolateRequestBuilder prepareMultiPercolate() {
        return new MultiPercolateRequestBuilder(this, MultiPercolateAction.INSTANCE);
    }

    public void multiPercolate(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener) {
        this.execute(MultiPercolateAction.INSTANCE, request, listener);
    }

    public ActionFuture<MultiPercolateResponse> multiPercolate(MultiPercolateRequest request) {
        return this.execute(MultiPercolateAction.INSTANCE, request);
    }

    public ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return new ExplainRequestBuilder(this, ExplainAction.INSTANCE, index, type, id);
    }

    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return this.execute(ExplainAction.INSTANCE, request);
    }

    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        this.execute(ExplainAction.INSTANCE, request, listener);
    }

    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        this.execute(ClearScrollAction.INSTANCE, request, listener);
    }

    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return this.execute(ClearScrollAction.INSTANCE, request);
    }

    public ClearScrollRequestBuilder prepareClearScroll() {
        return new ClearScrollRequestBuilder(this, ClearScrollAction.INSTANCE);
    }

    public void fieldStats(FieldStatsRequest request, ActionListener<FieldStatsResponse> listener) {
        this.execute(FieldStatsAction.INSTANCE, request, listener);
    }

    public ActionFuture<FieldStatsResponse> fieldStats(FieldStatsRequest request) {
        return this.execute(FieldStatsAction.INSTANCE, request);
    }

    public FieldStatsRequestBuilder prepareFieldStats() {
        return new FieldStatsRequestBuilder(this, FieldStatsAction.INSTANCE);
    }

    static class IndicesAdmin implements IndicesAdminClient {
        private final ElasticsearchClient client;
        private final MarvelShieldIntegration shieldIntegration;

        public IndicesAdmin(ElasticsearchClient client, MarvelShieldIntegration shieldIntegration) {
            this.client = client;
            this.shieldIntegration = shieldIntegration;
        }

        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
            shieldIntegration.bindInternalMarvelUser(request);
            return this.client.execute(action, request);
        }

        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            shieldIntegration.bindInternalMarvelUser(request);
            this.client.execute(action, request, listener);
        }

        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
            return this.client.prepareExecute(action);
        }

        public ThreadPool threadPool() {
            return this.client.threadPool();
        }

        public ActionFuture<IndicesExistsResponse> exists(IndicesExistsRequest request) {
            return this.execute(IndicesExistsAction.INSTANCE, request);
        }

        public void exists(IndicesExistsRequest request, ActionListener<IndicesExistsResponse> listener) {
            this.execute(IndicesExistsAction.INSTANCE, request, listener);
        }

        public IndicesExistsRequestBuilder prepareExists(String... indices) {
            return new IndicesExistsRequestBuilder(this, IndicesExistsAction.INSTANCE, indices);
        }

        public ActionFuture<TypesExistsResponse> typesExists(TypesExistsRequest request) {
            return this.execute(TypesExistsAction.INSTANCE, request);
        }

        public void typesExists(TypesExistsRequest request, ActionListener<TypesExistsResponse> listener) {
            this.execute(TypesExistsAction.INSTANCE, request, listener);
        }

        public TypesExistsRequestBuilder prepareTypesExists(String... index) {
            return new TypesExistsRequestBuilder(this, TypesExistsAction.INSTANCE, index);
        }

        public ActionFuture<IndicesAliasesResponse> aliases(IndicesAliasesRequest request) {
            return this.execute(IndicesAliasesAction.INSTANCE, request);
        }

        public void aliases(IndicesAliasesRequest request, ActionListener<IndicesAliasesResponse> listener) {
            this.execute(IndicesAliasesAction.INSTANCE, request, listener);
        }

        public IndicesAliasesRequestBuilder prepareAliases() {
            return new IndicesAliasesRequestBuilder(this, IndicesAliasesAction.INSTANCE);
        }

        public ActionFuture<GetAliasesResponse> getAliases(GetAliasesRequest request) {
            return this.execute(GetAliasesAction.INSTANCE, request);
        }

        public void getAliases(GetAliasesRequest request, ActionListener<GetAliasesResponse> listener) {
            this.execute(GetAliasesAction.INSTANCE, request, listener);
        }

        public GetAliasesRequestBuilder prepareGetAliases(String... aliases) {
            return new GetAliasesRequestBuilder(this, GetAliasesAction.INSTANCE, aliases);
        }

        public ActionFuture<ClearIndicesCacheResponse> clearCache(ClearIndicesCacheRequest request) {
            return this.execute(ClearIndicesCacheAction.INSTANCE, request);
        }

        public void aliasesExist(GetAliasesRequest request, ActionListener<AliasesExistResponse> listener) {
            this.execute(AliasesExistAction.INSTANCE, request, listener);
        }

        public ActionFuture<AliasesExistResponse> aliasesExist(GetAliasesRequest request) {
            return this.execute(AliasesExistAction.INSTANCE, request);
        }

        public AliasesExistRequestBuilder prepareAliasesExist(String... aliases) {
            return new AliasesExistRequestBuilder(this, AliasesExistAction.INSTANCE, aliases);
        }

        public ActionFuture<GetIndexResponse> getIndex(GetIndexRequest request) {
            return this.execute(GetIndexAction.INSTANCE, request);
        }

        public void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener) {
            this.execute(GetIndexAction.INSTANCE, request, listener);
        }

        public GetIndexRequestBuilder prepareGetIndex() {
            return new GetIndexRequestBuilder(this, GetIndexAction.INSTANCE, Strings.EMPTY_ARRAY);
        }

        public void clearCache(ClearIndicesCacheRequest request, ActionListener<ClearIndicesCacheResponse> listener) {
            this.execute(ClearIndicesCacheAction.INSTANCE, request, listener);
        }

        public ClearIndicesCacheRequestBuilder prepareClearCache(String... indices) {
            return new ClearIndicesCacheRequestBuilder(this, ClearIndicesCacheAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<CreateIndexResponse> create(CreateIndexRequest request) {
            return this.execute(CreateIndexAction.INSTANCE, request);
        }

        public void create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
            this.execute(CreateIndexAction.INSTANCE, request, listener);
        }

        public CreateIndexRequestBuilder prepareCreate(String index) {
            return new CreateIndexRequestBuilder(this, CreateIndexAction.INSTANCE, index);
        }

        public ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request) {
            return this.execute(DeleteIndexAction.INSTANCE, request);
        }

        public void delete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
            this.execute(DeleteIndexAction.INSTANCE, request, listener);
        }

        public DeleteIndexRequestBuilder prepareDelete(String... indices) {
            return new DeleteIndexRequestBuilder(this, DeleteIndexAction.INSTANCE, indices);
        }

        public ActionFuture<CloseIndexResponse> close(CloseIndexRequest request) {
            return this.execute(CloseIndexAction.INSTANCE, request);
        }

        public void close(CloseIndexRequest request, ActionListener<CloseIndexResponse> listener) {
            this.execute(CloseIndexAction.INSTANCE, request, listener);
        }

        public CloseIndexRequestBuilder prepareClose(String... indices) {
            return new CloseIndexRequestBuilder(this, CloseIndexAction.INSTANCE, indices);
        }

        public ActionFuture<OpenIndexResponse> open(OpenIndexRequest request) {
            return this.execute(OpenIndexAction.INSTANCE, request);
        }

        public void open(OpenIndexRequest request, ActionListener<OpenIndexResponse> listener) {
            this.execute(OpenIndexAction.INSTANCE, request, listener);
        }

        public OpenIndexRequestBuilder prepareOpen(String... indices) {
            return new OpenIndexRequestBuilder(this, OpenIndexAction.INSTANCE, indices);
        }

        public ActionFuture<FlushResponse> flush(FlushRequest request) {
            return this.execute(FlushAction.INSTANCE, request);
        }

        public void flush(FlushRequest request, ActionListener<FlushResponse> listener) {
            this.execute(FlushAction.INSTANCE, request, listener);
        }

        public FlushRequestBuilder prepareFlush(String... indices) {
            return (new FlushRequestBuilder(this, FlushAction.INSTANCE)).setIndices(indices);
        }

        public void getMappings(GetMappingsRequest request, ActionListener<GetMappingsResponse> listener) {
            this.execute(GetMappingsAction.INSTANCE, request, listener);
        }

        public void getFieldMappings(GetFieldMappingsRequest request, ActionListener<GetFieldMappingsResponse> listener) {
            this.execute(GetFieldMappingsAction.INSTANCE, request, listener);
        }

        public GetMappingsRequestBuilder prepareGetMappings(String... indices) {
            return new GetMappingsRequestBuilder(this, GetMappingsAction.INSTANCE, indices);
        }

        public ActionFuture<GetMappingsResponse> getMappings(GetMappingsRequest request) {
            return this.execute(GetMappingsAction.INSTANCE, request);
        }

        public GetFieldMappingsRequestBuilder prepareGetFieldMappings(String... indices) {
            return new GetFieldMappingsRequestBuilder(this, GetFieldMappingsAction.INSTANCE, indices);
        }

        public ActionFuture<GetFieldMappingsResponse> getFieldMappings(GetFieldMappingsRequest request) {
            return this.execute(GetFieldMappingsAction.INSTANCE, request);
        }

        public ActionFuture<PutMappingResponse> putMapping(PutMappingRequest request) {
            return this.execute(PutMappingAction.INSTANCE, request);
        }

        public void putMapping(PutMappingRequest request, ActionListener<PutMappingResponse> listener) {
            this.execute(PutMappingAction.INSTANCE, request, listener);
        }

        public PutMappingRequestBuilder preparePutMapping(String... indices) {
            return new PutMappingRequestBuilder(this, PutMappingAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<ForceMergeResponse> forceMerge(ForceMergeRequest request) {
            return this.execute(ForceMergeAction.INSTANCE, request);
        }

        @Override
        public void forceMerge(ForceMergeRequest request, ActionListener<ForceMergeResponse> listener) {
            this.execute(ForceMergeAction.INSTANCE, request, listener);
        }

        @Override
        public ForceMergeRequestBuilder prepareForceMerge(String... indices) {
            return (new ForceMergeRequestBuilder(this, ForceMergeAction.INSTANCE)).setIndices(indices);
        }

        public ActionFuture<UpgradeResponse> upgrade(UpgradeRequest request) {
            return this.execute(UpgradeAction.INSTANCE, request);
        }

        public void upgrade(UpgradeRequest request, ActionListener<UpgradeResponse> listener) {
            this.execute(UpgradeAction.INSTANCE, request, listener);
        }

        public UpgradeRequestBuilder prepareUpgrade(String... indices) {
            return new UpgradeRequestBuilder(this, UpgradeAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<UpgradeStatusResponse> upgradeStatus(UpgradeStatusRequest request) {
            return this.execute(UpgradeStatusAction.INSTANCE, request);
        }

        public void upgradeStatus(UpgradeStatusRequest request, ActionListener<UpgradeStatusResponse> listener) {
            this.execute(UpgradeStatusAction.INSTANCE, request, listener);
        }

        public UpgradeStatusRequestBuilder prepareUpgradeStatus(String... indices) {
            return new UpgradeStatusRequestBuilder(this, UpgradeStatusAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<RefreshResponse> refresh(RefreshRequest request) {
            return this.execute(RefreshAction.INSTANCE, request);
        }

        public void refresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
            this.execute(RefreshAction.INSTANCE, request, listener);
        }

        public RefreshRequestBuilder prepareRefresh(String... indices) {
            return new RefreshRequestBuilder(this, RefreshAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<IndicesStatsResponse> stats(IndicesStatsRequest request) {
            return this.execute(IndicesStatsAction.INSTANCE, request);
        }

        public void stats(IndicesStatsRequest request, ActionListener<IndicesStatsResponse> listener) {
            this.execute(IndicesStatsAction.INSTANCE, request, listener);
        }

        public IndicesStatsRequestBuilder prepareStats(String... indices) {
            return new IndicesStatsRequestBuilder(this, IndicesStatsAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<RecoveryResponse> recoveries(RecoveryRequest request) {
            return this.execute(RecoveryAction.INSTANCE, request);
        }

        public void recoveries(RecoveryRequest request, ActionListener<RecoveryResponse> listener) {
            this.execute(RecoveryAction.INSTANCE, request, listener);
        }

        public RecoveryRequestBuilder prepareRecoveries(String... indices) {
            return new RecoveryRequestBuilder(this, RecoveryAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<IndicesSegmentResponse> segments(IndicesSegmentsRequest request) {
            return this.execute(IndicesSegmentsAction.INSTANCE, request);
        }

        public void segments(IndicesSegmentsRequest request, ActionListener<IndicesSegmentResponse> listener) {
            this.execute(IndicesSegmentsAction.INSTANCE, request, listener);
        }

        public IndicesSegmentsRequestBuilder prepareSegments(String... indices) {
            return new IndicesSegmentsRequestBuilder(this, IndicesSegmentsAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<IndicesShardStoresResponse> shardStores(IndicesShardStoresRequest request) {
            return this.execute(IndicesShardStoresAction.INSTANCE, request);
        }

        public void shardStores(IndicesShardStoresRequest request, ActionListener<IndicesShardStoresResponse> listener) {
            this.execute(IndicesShardStoresAction.INSTANCE, request, listener);
        }

        public IndicesShardStoreRequestBuilder prepareShardStores(String... indices) {
            return new IndicesShardStoreRequestBuilder(this, IndicesShardStoresAction.INSTANCE, indices);
        }

        public ActionFuture<UpdateSettingsResponse> updateSettings(UpdateSettingsRequest request) {
            return this.execute(UpdateSettingsAction.INSTANCE, request);
        }

        public void updateSettings(UpdateSettingsRequest request, ActionListener<UpdateSettingsResponse> listener) {
            this.execute(UpdateSettingsAction.INSTANCE, request, listener);
        }

        public UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices) {
            return new UpdateSettingsRequestBuilder(this, UpdateSettingsAction.INSTANCE, Strings.EMPTY_ARRAY).setIndices(indices);
        }

        public ActionFuture<AnalyzeResponse> analyze(AnalyzeRequest request) {
            return this.execute(AnalyzeAction.INSTANCE, request);
        }

        public void analyze(AnalyzeRequest request, ActionListener<AnalyzeResponse> listener) {
            this.execute(AnalyzeAction.INSTANCE, request, listener);
        }

        public AnalyzeRequestBuilder prepareAnalyze(@Nullable String index, String text) {
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE, index, text);
        }

        public AnalyzeRequestBuilder prepareAnalyze(String text) {
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE, null, text);
        }

        public AnalyzeRequestBuilder prepareAnalyze() {
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE);
        }

        public ActionFuture<PutIndexTemplateResponse> putTemplate(PutIndexTemplateRequest request) {
            return this.execute(PutIndexTemplateAction.INSTANCE, request);
        }

        public void putTemplate(PutIndexTemplateRequest request, ActionListener<PutIndexTemplateResponse> listener) {
            this.execute(PutIndexTemplateAction.INSTANCE, request, listener);
        }

        public PutIndexTemplateRequestBuilder preparePutTemplate(String name) {
            return new PutIndexTemplateRequestBuilder(this, PutIndexTemplateAction.INSTANCE, name);
        }

        public ActionFuture<GetIndexTemplatesResponse> getTemplates(GetIndexTemplatesRequest request) {
            return this.execute(GetIndexTemplatesAction.INSTANCE, request);
        }

        public void getTemplates(GetIndexTemplatesRequest request, ActionListener<GetIndexTemplatesResponse> listener) {
            this.execute(GetIndexTemplatesAction.INSTANCE, request, listener);
        }

        public GetIndexTemplatesRequestBuilder prepareGetTemplates(String... names) {
            return new GetIndexTemplatesRequestBuilder(this, GetIndexTemplatesAction.INSTANCE, names);
        }

        public ActionFuture<DeleteIndexTemplateResponse> deleteTemplate(DeleteIndexTemplateRequest request) {
            return this.execute(DeleteIndexTemplateAction.INSTANCE, request);
        }

        public void deleteTemplate(DeleteIndexTemplateRequest request, ActionListener<DeleteIndexTemplateResponse> listener) {
            this.execute(DeleteIndexTemplateAction.INSTANCE, request, listener);
        }

        public DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name) {
            return new DeleteIndexTemplateRequestBuilder(this, DeleteIndexTemplateAction.INSTANCE, name);
        }

        public ActionFuture<ValidateQueryResponse> validateQuery(ValidateQueryRequest request) {
            return this.execute(ValidateQueryAction.INSTANCE, request);
        }

        public void validateQuery(ValidateQueryRequest request, ActionListener<ValidateQueryResponse> listener) {
            this.execute(ValidateQueryAction.INSTANCE, request, listener);
        }

        public ValidateQueryRequestBuilder prepareValidateQuery(String... indices) {
            return new ValidateQueryRequestBuilder(this, ValidateQueryAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<PutWarmerResponse> putWarmer(PutWarmerRequest request) {
            return this.execute(PutWarmerAction.INSTANCE, request);
        }

        public void putWarmer(PutWarmerRequest request, ActionListener<PutWarmerResponse> listener) {
            this.execute(PutWarmerAction.INSTANCE, request, listener);
        }

        public PutWarmerRequestBuilder preparePutWarmer(String name) {
            return new PutWarmerRequestBuilder(this, PutWarmerAction.INSTANCE, name);
        }

        public ActionFuture<DeleteWarmerResponse> deleteWarmer(DeleteWarmerRequest request) {
            return this.execute(DeleteWarmerAction.INSTANCE, request);
        }

        public void deleteWarmer(DeleteWarmerRequest request, ActionListener<DeleteWarmerResponse> listener) {
            this.execute(DeleteWarmerAction.INSTANCE, request, listener);
        }

        public DeleteWarmerRequestBuilder prepareDeleteWarmer() {
            return new DeleteWarmerRequestBuilder(this, DeleteWarmerAction.INSTANCE);
        }

        public GetWarmersRequestBuilder prepareGetWarmers(String... indices) {
            return new GetWarmersRequestBuilder(this, GetWarmersAction.INSTANCE, indices);
        }

        public ActionFuture<GetWarmersResponse> getWarmers(GetWarmersRequest request) {
            return this.execute(GetWarmersAction.INSTANCE, request);
        }

        public void getWarmers(GetWarmersRequest request, ActionListener<GetWarmersResponse> listener) {
            this.execute(GetWarmersAction.INSTANCE, request, listener);
        }

        public GetSettingsRequestBuilder prepareGetSettings(String... indices) {
            return new GetSettingsRequestBuilder(this, GetSettingsAction.INSTANCE, indices);
        }

        public ActionFuture<GetSettingsResponse> getSettings(GetSettingsRequest request) {
            return this.execute(GetSettingsAction.INSTANCE, request);
        }

        public void getSettings(GetSettingsRequest request, ActionListener<GetSettingsResponse> listener) {
            this.execute(GetSettingsAction.INSTANCE, request, listener);
        }
    }

    static class ClusterAdmin implements ClusterAdminClient {
        private final ElasticsearchClient client;
        private final MarvelShieldIntegration shieldIntegration;

        public ClusterAdmin(ElasticsearchClient client, MarvelShieldIntegration shieldIntegration) {
            this.client = client;
            this.shieldIntegration = shieldIntegration;
        }

        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
            shieldIntegration.bindInternalMarvelUser(request);
            return this.client.execute(action, request);
        }

        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            shieldIntegration.bindInternalMarvelUser(request);
            this.client.execute(action, request, listener);
        }

        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
            return this.client.prepareExecute(action);
        }

        public ThreadPool threadPool() {
            return this.client.threadPool();
        }

        public ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request) {
            return this.execute(ClusterHealthAction.INSTANCE, request);
        }

        public void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener) {
            this.execute(ClusterHealthAction.INSTANCE, request, listener);
        }

        public ClusterHealthRequestBuilder prepareHealth(String... indices) {
            return new ClusterHealthRequestBuilder(this, ClusterHealthAction.INSTANCE).setIndices(indices);
        }

        public ActionFuture<ClusterStateResponse> state(ClusterStateRequest request) {
            return this.execute(ClusterStateAction.INSTANCE, request);
        }

        public void state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener) {
            this.execute(ClusterStateAction.INSTANCE, request, listener);
        }

        public ClusterStateRequestBuilder prepareState() {
            return new ClusterStateRequestBuilder(this, ClusterStateAction.INSTANCE);
        }

        public ActionFuture<ClusterRerouteResponse> reroute(ClusterRerouteRequest request) {
            return this.execute(ClusterRerouteAction.INSTANCE, request);
        }

        public void reroute(ClusterRerouteRequest request, ActionListener<ClusterRerouteResponse> listener) {
            this.execute(ClusterRerouteAction.INSTANCE, request, listener);
        }

        public ClusterRerouteRequestBuilder prepareReroute() {
            return new ClusterRerouteRequestBuilder(this, ClusterRerouteAction.INSTANCE);
        }

        public ActionFuture<ClusterUpdateSettingsResponse> updateSettings(ClusterUpdateSettingsRequest request) {
            return this.execute(ClusterUpdateSettingsAction.INSTANCE, request);
        }

        public void updateSettings(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener) {
            this.execute(ClusterUpdateSettingsAction.INSTANCE, request, listener);
        }

        public ClusterUpdateSettingsRequestBuilder prepareUpdateSettings() {
            return new ClusterUpdateSettingsRequestBuilder(this, ClusterUpdateSettingsAction.INSTANCE);
        }

        public ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request) {
            return this.execute(NodesInfoAction.INSTANCE, request);
        }

        public void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener) {
            this.execute(NodesInfoAction.INSTANCE, request, listener);
        }

        public NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds) {
            return new NodesInfoRequestBuilder(this, NodesInfoAction.INSTANCE).setNodesIds(nodesIds);
        }

        public ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request) {
            return this.execute(NodesStatsAction.INSTANCE, request);
        }

        public void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener) {
            this.execute(NodesStatsAction.INSTANCE, request, listener);
        }

        public NodesStatsRequestBuilder prepareNodesStats(String... nodesIds) {
            return new NodesStatsRequestBuilder(this, NodesStatsAction.INSTANCE).setNodesIds(nodesIds);
        }

        public ActionFuture<ClusterStatsResponse> clusterStats(ClusterStatsRequest request) {
            return this.execute(ClusterStatsAction.INSTANCE, request);
        }

        public void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener) {
            this.execute(ClusterStatsAction.INSTANCE, request, listener);
        }

        public ClusterStatsRequestBuilder prepareClusterStats() {
            return new ClusterStatsRequestBuilder(this, ClusterStatsAction.INSTANCE);
        }

        public ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request) {
            return this.execute(NodesHotThreadsAction.INSTANCE, request);
        }

        public void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener) {
            this.execute(NodesHotThreadsAction.INSTANCE, request, listener);
        }

        public NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds) {
            return new NodesHotThreadsRequestBuilder(this, NodesHotThreadsAction.INSTANCE).setNodesIds(nodesIds);
        }

        public ActionFuture<ClusterSearchShardsResponse> searchShards(ClusterSearchShardsRequest request) {
            return this.execute(ClusterSearchShardsAction.INSTANCE, request);
        }

        public void searchShards(ClusterSearchShardsRequest request, ActionListener<ClusterSearchShardsResponse> listener) {
            this.execute(ClusterSearchShardsAction.INSTANCE, request, listener);
        }

        public ClusterSearchShardsRequestBuilder prepareSearchShards() {
            return new ClusterSearchShardsRequestBuilder(this, ClusterSearchShardsAction.INSTANCE);
        }

        public ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices) {
            return new ClusterSearchShardsRequestBuilder(this, ClusterSearchShardsAction.INSTANCE).setIndices(indices);
        }

        public PendingClusterTasksRequestBuilder preparePendingClusterTasks() {
            return new PendingClusterTasksRequestBuilder(this, PendingClusterTasksAction.INSTANCE);
        }

        public ActionFuture<PendingClusterTasksResponse> pendingClusterTasks(PendingClusterTasksRequest request) {
            return this.execute(PendingClusterTasksAction.INSTANCE, request);
        }

        public void pendingClusterTasks(PendingClusterTasksRequest request, ActionListener<PendingClusterTasksResponse> listener) {
            this.execute(PendingClusterTasksAction.INSTANCE, request, listener);
        }

        public ActionFuture<PutRepositoryResponse> putRepository(PutRepositoryRequest request) {
            return this.execute(PutRepositoryAction.INSTANCE, request);
        }

        public void putRepository(PutRepositoryRequest request, ActionListener<PutRepositoryResponse> listener) {
            this.execute(PutRepositoryAction.INSTANCE, request, listener);
        }

        public PutRepositoryRequestBuilder preparePutRepository(String name) {
            return new PutRepositoryRequestBuilder(this, PutRepositoryAction.INSTANCE, name);
        }

        public ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request) {
            return this.execute(CreateSnapshotAction.INSTANCE, request);
        }

        public void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener) {
            this.execute(CreateSnapshotAction.INSTANCE, request, listener);
        }

        public CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name) {
            return new CreateSnapshotRequestBuilder(this, CreateSnapshotAction.INSTANCE, repository, name);
        }

        public ActionFuture<GetSnapshotsResponse> getSnapshots(GetSnapshotsRequest request) {
            return this.execute(GetSnapshotsAction.INSTANCE, request);
        }

        public void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener) {
            this.execute(GetSnapshotsAction.INSTANCE, request, listener);
        }

        public GetSnapshotsRequestBuilder prepareGetSnapshots(String repository) {
            return new GetSnapshotsRequestBuilder(this, GetSnapshotsAction.INSTANCE, repository);
        }

        public ActionFuture<DeleteSnapshotResponse> deleteSnapshot(DeleteSnapshotRequest request) {
            return this.execute(DeleteSnapshotAction.INSTANCE, request);
        }

        public void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<DeleteSnapshotResponse> listener) {
            this.execute(DeleteSnapshotAction.INSTANCE, request, listener);
        }

        public DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String name) {
            return new DeleteSnapshotRequestBuilder(this, DeleteSnapshotAction.INSTANCE, repository, name);
        }

        public ActionFuture<DeleteRepositoryResponse> deleteRepository(DeleteRepositoryRequest request) {
            return this.execute(DeleteRepositoryAction.INSTANCE, request);
        }

        public void deleteRepository(DeleteRepositoryRequest request, ActionListener<DeleteRepositoryResponse> listener) {
            this.execute(DeleteRepositoryAction.INSTANCE, request, listener);
        }

        public DeleteRepositoryRequestBuilder prepareDeleteRepository(String name) {
            return new DeleteRepositoryRequestBuilder(this, DeleteRepositoryAction.INSTANCE, name);
        }

        public ActionFuture<VerifyRepositoryResponse> verifyRepository(VerifyRepositoryRequest request) {
            return this.execute(VerifyRepositoryAction.INSTANCE, request);
        }

        public void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener) {
            this.execute(VerifyRepositoryAction.INSTANCE, request, listener);
        }

        public VerifyRepositoryRequestBuilder prepareVerifyRepository(String name) {
            return new VerifyRepositoryRequestBuilder(this, VerifyRepositoryAction.INSTANCE, name);
        }

        public ActionFuture<GetRepositoriesResponse> getRepositories(GetRepositoriesRequest request) {
            return this.execute(GetRepositoriesAction.INSTANCE, request);
        }

        public void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener) {
            this.execute(GetRepositoriesAction.INSTANCE, request, listener);
        }

        public GetRepositoriesRequestBuilder prepareGetRepositories(String... name) {
            return new GetRepositoriesRequestBuilder(this, GetRepositoriesAction.INSTANCE, name);
        }

        public ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request) {
            return this.execute(RestoreSnapshotAction.INSTANCE, request);
        }

        public void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener) {
            this.execute(RestoreSnapshotAction.INSTANCE, request, listener);
        }

        public RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot) {
            return new RestoreSnapshotRequestBuilder(this, RestoreSnapshotAction.INSTANCE, repository, snapshot);
        }

        public ActionFuture<SnapshotsStatusResponse> snapshotsStatus(SnapshotsStatusRequest request) {
            return this.execute(SnapshotsStatusAction.INSTANCE, request);
        }

        public void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener) {
            this.execute(SnapshotsStatusAction.INSTANCE, request, listener);
        }

        public SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository) {
            return new SnapshotsStatusRequestBuilder(this, SnapshotsStatusAction.INSTANCE, repository);
        }

        public SnapshotsStatusRequestBuilder prepareSnapshotStatus() {
            return new SnapshotsStatusRequestBuilder(this, SnapshotsStatusAction.INSTANCE);
        }

        public ActionFuture<RenderSearchTemplateResponse> renderSearchTemplate(RenderSearchTemplateRequest request) {
            return this.execute(RenderSearchTemplateAction.INSTANCE, request);
        }

        public void renderSearchTemplate(RenderSearchTemplateRequest request, ActionListener<RenderSearchTemplateResponse> listener) {
            this.execute(RenderSearchTemplateAction.INSTANCE, request, listener);
        }

        public RenderSearchTemplateRequestBuilder prepareRenderSearchTemplate() {
            return new RenderSearchTemplateRequestBuilder(this, RenderSearchTemplateAction.INSTANCE);
        }
    }

    static class Admin implements AdminClient {

        private final ClusterAdmin clusterAdmin;
        private final IndicesAdmin indicesAdmin;

        public Admin(ElasticsearchClient client, MarvelShieldIntegration shieldIntegration) {
            this.clusterAdmin = new ClusterAdmin(client, shieldIntegration);
            this.indicesAdmin = new IndicesAdmin(client, shieldIntegration);
        }

        public ClusterAdminClient cluster() {
            return this.clusterAdmin;
        }

        public IndicesAdminClient indices() {
            return this.indicesAdmin;
        }
    }
}
