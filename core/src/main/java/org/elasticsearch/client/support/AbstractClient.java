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
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoreRequestBuilder;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
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
import org.elasticsearch.action.admin.indices.validate.template.RenderSearchTemplateAction;
import org.elasticsearch.action.admin.indices.validate.template.RenderSearchTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.template.RenderSearchTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.validate.template.RenderSearchTemplateResponse;
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
import org.elasticsearch.action.count.CountAction;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.exists.ExistsAction;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsRequestBuilder;
import org.elasticsearch.action.exists.ExistsResponse;
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
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.action.support.DelegatingActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.termvectors.*;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 *
 */
public abstract class AbstractClient extends AbstractComponent implements Client {

    private final ThreadPool threadPool;
    private final Admin admin;

    private final Headers headers;
    private final ThreadedActionListener.Wrapper threadedWrapper;

    public AbstractClient(Settings settings, ThreadPool threadPool, Headers headers) {
        super(settings);
        this.threadPool = threadPool;
        this.headers = headers;
        this.admin = new Admin(this);
        this.threadedWrapper = new ThreadedActionListener.Wrapper(logger, settings, threadPool);
    }

    @Override
    public Headers headers() {
        return this.headers;
    }

    @Override
    public final Settings settings() {
        return this.settings;
    }

    @Override
    public final ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override
    public final AdminClient admin() {
        return admin;
    }

    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(final Action<Request, Response, RequestBuilder> action) {
        return action.newRequestBuilder(this);
    }

    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    /**
     * This is the single execution point of *all* clients.
     */
    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        headers.applyTo(request);
        listener = threadedWrapper.wrap(listener);
        doExecute(action, request, listener);
    }

    protected abstract <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(final Action<Request, Response, RequestBuilder> action, final Request request, ActionListener<Response> listener);

    @Override
    public ActionFuture<IndexResponse> index(final IndexRequest request) {
        return execute(IndexAction.INSTANCE, request);
    }

    @Override
    public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        execute(IndexAction.INSTANCE, request, listener);
    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, IndexAction.INSTANCE, null);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type) {
        return prepareIndex(index, type, null);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id) {
        return prepareIndex().setIndex(index).setType(type).setId(id);
    }

    @Override
    public ActionFuture<UpdateResponse> update(final UpdateRequest request) {
        return execute(UpdateAction.INSTANCE, request);
    }

    @Override
    public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        execute(UpdateAction.INSTANCE, request, listener);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return new UpdateRequestBuilder(this, UpdateAction.INSTANCE, null, null, null);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return new UpdateRequestBuilder(this, UpdateAction.INSTANCE, index, type, id);
    }

    @Override
    public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        return execute(DeleteAction.INSTANCE, request);
    }

    @Override
    public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        execute(DeleteAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this, DeleteAction.INSTANCE, null);
    }

    @Override
    public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return prepareDelete().setIndex(index).setType(type).setId(id);
    }

    @Override
    public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        return execute(BulkAction.INSTANCE, request);
    }

    @Override
    public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        execute(BulkAction.INSTANCE, request, listener);
    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this, BulkAction.INSTANCE);
    }

    @Override
    public ActionFuture<GetResponse> get(final GetRequest request) {
        return execute(GetAction.INSTANCE, request);
    }

    @Override
    public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        execute(GetAction.INSTANCE, request, listener);
    }

    @Override
    public GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this, GetAction.INSTANCE, null);
    }

    @Override
    public GetRequestBuilder prepareGet(String index, String type, String id) {
        return prepareGet().setIndex(index).setType(type).setId(id);
    }


    @Override
    public ActionFuture<GetIndexedScriptResponse> getIndexedScript(final GetIndexedScriptRequest request) {
        return execute(GetIndexedScriptAction.INSTANCE, request);
    }

    @Override
    public void getIndexedScript(final GetIndexedScriptRequest request, final ActionListener<GetIndexedScriptResponse> listener) {
        execute(GetIndexedScriptAction.INSTANCE, request, listener);
    }

    @Override
    public GetIndexedScriptRequestBuilder prepareGetIndexedScript() {
        return new GetIndexedScriptRequestBuilder(this, GetIndexedScriptAction.INSTANCE);
    }

    @Override
    public GetIndexedScriptRequestBuilder prepareGetIndexedScript(String scriptLang, String id) {
        return prepareGetIndexedScript().setScriptLang(scriptLang).setId(id);
    }


    /**
     * Put an indexed script
     */
    @Override
    public PutIndexedScriptRequestBuilder preparePutIndexedScript() {
        return new PutIndexedScriptRequestBuilder(this, PutIndexedScriptAction.INSTANCE);
    }

    /**
     * Put the indexed script
     */
    @Override
    public PutIndexedScriptRequestBuilder preparePutIndexedScript(@Nullable String scriptLang, String id, String source){
        return PutIndexedScriptAction.INSTANCE.newRequestBuilder(this).setScriptLang(scriptLang).setId(id).setSource(source);
    }

    /**
     * Put an indexed script
     */
    @Override
    public void putIndexedScript(final PutIndexedScriptRequest request, ActionListener<PutIndexedScriptResponse> listener){
        execute(PutIndexedScriptAction.INSTANCE, request, listener);

    }

    /**
     * Put an indexed script
     *
     * @param request The put request
     * @return The result future
     */
    @Override
    public ActionFuture<PutIndexedScriptResponse> putIndexedScript(final PutIndexedScriptRequest request){
        return execute(PutIndexedScriptAction.INSTANCE, request);
    }

    /**
     * delete an indexed script
     */
    @Override
    public void deleteIndexedScript(DeleteIndexedScriptRequest request, ActionListener<DeleteIndexedScriptResponse> listener){
        execute(DeleteIndexedScriptAction.INSTANCE, request, listener);
    }

    /**
     * Delete an indexed script
     *
     * @param request The put request
     * @return The result future
     */
    @Override
    public ActionFuture<DeleteIndexedScriptResponse> deleteIndexedScript(DeleteIndexedScriptRequest request){
        return execute(DeleteIndexedScriptAction.INSTANCE, request);
    }


    /**
     * Delete an indexed script
     */
    @Override
    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript(){
        return DeleteIndexedScriptAction.INSTANCE.newRequestBuilder(this);
    }

    /**
     * Delete an indexed script
     */
    @Override
    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript(@Nullable String scriptLang, String id){
        return prepareDeleteIndexedScript().setScriptLang(scriptLang).setId(id);
    }



    @Override
    public ActionFuture<MultiGetResponse> multiGet(final MultiGetRequest request) {
        return execute(MultiGetAction.INSTANCE, request);
    }

    @Override
    public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        execute(MultiGetAction.INSTANCE, request, listener);
    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this, MultiGetAction.INSTANCE);
    }

    @Override
    public ActionFuture<SearchResponse> search(final SearchRequest request) {
        return execute(SearchAction.INSTANCE, request);
    }

    @Override
    public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        execute(SearchAction.INSTANCE, request, listener);
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(this, SearchAction.INSTANCE).setIndices(indices);
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        return execute(SearchScrollAction.INSTANCE, request);
    }

    @Override
    public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        execute(SearchScrollAction.INSTANCE, request, listener);
    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return new SearchScrollRequestBuilder(this, SearchScrollAction.INSTANCE, scrollId);
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return execute(MultiSearchAction.INSTANCE, request);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        execute(MultiSearchAction.INSTANCE, request, listener);
    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return new MultiSearchRequestBuilder(this, MultiSearchAction.INSTANCE);
    }

    @Override
    public ActionFuture<CountResponse> count(final CountRequest request) {
        AdapterActionFuture<CountResponse, SearchResponse> actionFuture = new AdapterActionFuture<CountResponse, SearchResponse>() {
            @Override
            protected CountResponse convert(SearchResponse listenerResponse) {
                return new CountResponse(listenerResponse);
            }
        };
        execute(SearchAction.INSTANCE, request.toSearchRequest(), actionFuture);
        return actionFuture;
    }

    @Override
    public void count(final CountRequest request, final ActionListener<CountResponse> listener) {
        execute(SearchAction.INSTANCE, request.toSearchRequest(), new DelegatingActionListener<SearchResponse, CountResponse>(listener) {
            @Override
            protected CountResponse getDelegatedFromInstigator(SearchResponse response) {
                return new CountResponse(response);
            }
        });
    }

    @Override
    public CountRequestBuilder prepareCount(String... indices) {
        return new CountRequestBuilder(this, CountAction.INSTANCE).setIndices(indices);
    }

    @Override
    public ActionFuture<ExistsResponse> exists(final ExistsRequest request) {
        return execute(ExistsAction.INSTANCE, request);
    }

    @Override
    public void exists(final ExistsRequest request, final ActionListener<ExistsResponse> listener) {
        execute(ExistsAction.INSTANCE, request, listener);
    }

    @Override
    public ExistsRequestBuilder prepareExists(String... indices) {
        return new ExistsRequestBuilder(this, ExistsAction.INSTANCE).setIndices(indices);
    }

    @Override
    public ActionFuture<SuggestResponse> suggest(final SuggestRequest request) {
        return execute(SuggestAction.INSTANCE, request);
    }

    @Override
    public void suggest(final SuggestRequest request, final ActionListener<SuggestResponse> listener) {
        execute(SuggestAction.INSTANCE, request, listener);
    }

    @Override
    public SuggestRequestBuilder prepareSuggest(String... indices) {
        return new SuggestRequestBuilder(this, SuggestAction.INSTANCE).setIndices(indices);
    }

    @Override
    public ActionFuture<TermVectorsResponse> termVectors(final TermVectorsRequest request) {
        return execute(TermVectorsAction.INSTANCE, request);
    }

    @Override
    public void termVectors(final TermVectorsRequest request, final ActionListener<TermVectorsResponse> listener) {
        execute(TermVectorsAction.INSTANCE, request, listener);
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors() {
        return new TermVectorsRequestBuilder(this, TermVectorsAction.INSTANCE);
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors(String index, String type, String id) {
        return new TermVectorsRequestBuilder(this, TermVectorsAction.INSTANCE, index, type, id);
    }

    @Deprecated
    @Override
    public ActionFuture<TermVectorsResponse> termVector(final TermVectorsRequest request) {
        return termVectors(request);
    }

    @Deprecated
    @Override
    public void termVector(final TermVectorsRequest request, final ActionListener<TermVectorsResponse> listener) {
        termVectors(request, listener);
    }

    @Deprecated
    @Override
    public TermVectorsRequestBuilder prepareTermVector() {
        return prepareTermVectors();
    }

    @Deprecated
    @Override
    public TermVectorsRequestBuilder prepareTermVector(String index, String type, String id) {
        return prepareTermVectors(index, type, id);
    }

    @Override
    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(final MultiTermVectorsRequest request) {
        return execute(MultiTermVectorsAction.INSTANCE, request);
    }

    @Override
    public void multiTermVectors(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        execute(MultiTermVectorsAction.INSTANCE, request, listener);
    }

    @Override
    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return new MultiTermVectorsRequestBuilder(this, MultiTermVectorsAction.INSTANCE);
    }

    @Override
    public ActionFuture<PercolateResponse> percolate(final PercolateRequest request) {
        return execute(PercolateAction.INSTANCE, request);
    }

    @Override
    public void percolate(final PercolateRequest request, final ActionListener<PercolateResponse> listener) {
        execute(PercolateAction.INSTANCE, request, listener);
    }

    @Override
    public PercolateRequestBuilder preparePercolate() {
        return new PercolateRequestBuilder(this, PercolateAction.INSTANCE);
    }

    @Override
    public MultiPercolateRequestBuilder prepareMultiPercolate() {
        return new MultiPercolateRequestBuilder(this, MultiPercolateAction.INSTANCE);
    }

    @Override
    public void multiPercolate(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener) {
        execute(MultiPercolateAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<MultiPercolateResponse> multiPercolate(MultiPercolateRequest request) {
        return execute(MultiPercolateAction.INSTANCE, request);
    }

    @Override
    public ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return new ExplainRequestBuilder(this, ExplainAction.INSTANCE, index, type, id);
    }

    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return execute(ExplainAction.INSTANCE, request);
    }

    @Override
    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        execute(ExplainAction.INSTANCE, request, listener);
    }

    @Override
    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        execute(ClearScrollAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return execute(ClearScrollAction.INSTANCE, request);
    }

    @Override
    public ClearScrollRequestBuilder prepareClearScroll() {
        return new ClearScrollRequestBuilder(this, ClearScrollAction.INSTANCE);
    }

    @Override
    public void fieldStats(FieldStatsRequest request, ActionListener<FieldStatsResponse> listener) {
        execute(FieldStatsAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<FieldStatsResponse> fieldStats(FieldStatsRequest request) {
        return execute(FieldStatsAction.INSTANCE, request);
    }

    @Override
    public FieldStatsRequestBuilder prepareFieldStats() {
        return new FieldStatsRequestBuilder(this, FieldStatsAction.INSTANCE);
    }

    static class Admin implements AdminClient {

        private final ClusterAdmin clusterAdmin;
        private final IndicesAdmin indicesAdmin;

        public Admin(ElasticsearchClient client) {
            this.clusterAdmin = new ClusterAdmin(client);
            this.indicesAdmin = new IndicesAdmin(client);
        }

        @Override
        public ClusterAdminClient cluster() {
            return clusterAdmin;
        }

        @Override
        public IndicesAdminClient indices() {
            return indicesAdmin;
        }
    }

    static class ClusterAdmin implements ClusterAdminClient {

        private final ElasticsearchClient client;

        public ClusterAdmin(ElasticsearchClient client) {
            this.client = client;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
            return client.execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            client.execute(action, request, listener);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
            return client.prepareExecute(action);
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
        }

        @Override
        public ActionFuture<ClusterHealthResponse> health(final ClusterHealthRequest request) {
            return execute(ClusterHealthAction.INSTANCE, request);
        }

        @Override
        public void health(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
            execute(ClusterHealthAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterHealthRequestBuilder prepareHealth(String... indices) {
            return new ClusterHealthRequestBuilder(this, ClusterHealthAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
            return execute(ClusterStateAction.INSTANCE, request);
        }

        @Override
        public void state(final ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
            execute(ClusterStateAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterStateRequestBuilder prepareState() {
            return new ClusterStateRequestBuilder(this, ClusterStateAction.INSTANCE);
        }

        @Override
        public ActionFuture<ClusterRerouteResponse> reroute(final ClusterRerouteRequest request) {
            return execute(ClusterRerouteAction.INSTANCE, request);
        }

        @Override
        public void reroute(final ClusterRerouteRequest request, final ActionListener<ClusterRerouteResponse> listener) {
            execute(ClusterRerouteAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterRerouteRequestBuilder prepareReroute() {
            return new ClusterRerouteRequestBuilder(this, ClusterRerouteAction.INSTANCE);
        }

        @Override
        public ActionFuture<ClusterUpdateSettingsResponse> updateSettings(final ClusterUpdateSettingsRequest request) {
            return execute(ClusterUpdateSettingsAction.INSTANCE, request);
        }

        @Override
        public void updateSettings(final ClusterUpdateSettingsRequest request, final ActionListener<ClusterUpdateSettingsResponse> listener) {
            execute(ClusterUpdateSettingsAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterUpdateSettingsRequestBuilder prepareUpdateSettings() {
            return new ClusterUpdateSettingsRequestBuilder(this, ClusterUpdateSettingsAction.INSTANCE);
        }

        @Override
        public ActionFuture<NodesInfoResponse> nodesInfo(final NodesInfoRequest request) {
            return execute(NodesInfoAction.INSTANCE, request);
        }

        @Override
        public void nodesInfo(final NodesInfoRequest request, final ActionListener<NodesInfoResponse> listener) {
            execute(NodesInfoAction.INSTANCE, request, listener);
        }

        @Override
        public NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds) {
            return new NodesInfoRequestBuilder(this, NodesInfoAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public ActionFuture<NodesStatsResponse> nodesStats(final NodesStatsRequest request) {
            return execute(NodesStatsAction.INSTANCE, request);
        }

        @Override
        public void nodesStats(final NodesStatsRequest request, final ActionListener<NodesStatsResponse> listener) {
            execute(NodesStatsAction.INSTANCE, request, listener);
        }

        @Override
        public NodesStatsRequestBuilder prepareNodesStats(String... nodesIds) {
            return new NodesStatsRequestBuilder(this, NodesStatsAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public ActionFuture<ClusterStatsResponse> clusterStats(ClusterStatsRequest request) {
            return execute(ClusterStatsAction.INSTANCE, request);
        }

        @Override
        public void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener) {
            execute(ClusterStatsAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterStatsRequestBuilder prepareClusterStats() {
            return new ClusterStatsRequestBuilder(this, ClusterStatsAction.INSTANCE);
        }

        @Override
        public ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request) {
            return execute(NodesHotThreadsAction.INSTANCE, request);
        }

        @Override
        public void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener) {
            execute(NodesHotThreadsAction.INSTANCE, request, listener);
        }

        @Override
        public NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds) {
            return new NodesHotThreadsRequestBuilder(this, NodesHotThreadsAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public ActionFuture<ClusterSearchShardsResponse> searchShards(final ClusterSearchShardsRequest request) {
            return execute(ClusterSearchShardsAction.INSTANCE, request);
        }

        @Override
        public void searchShards(final ClusterSearchShardsRequest request, final ActionListener<ClusterSearchShardsResponse> listener) {
            execute(ClusterSearchShardsAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterSearchShardsRequestBuilder prepareSearchShards() {
            return new ClusterSearchShardsRequestBuilder(this, ClusterSearchShardsAction.INSTANCE);
        }

        @Override
        public ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices) {
            return new ClusterSearchShardsRequestBuilder(this, ClusterSearchShardsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public PendingClusterTasksRequestBuilder preparePendingClusterTasks() {
            return new PendingClusterTasksRequestBuilder(this, PendingClusterTasksAction.INSTANCE);
        }

        @Override
        public ActionFuture<PendingClusterTasksResponse> pendingClusterTasks(PendingClusterTasksRequest request) {
            return execute(PendingClusterTasksAction.INSTANCE, request);
        }

        @Override
        public void pendingClusterTasks(PendingClusterTasksRequest request, ActionListener<PendingClusterTasksResponse> listener) {
            execute(PendingClusterTasksAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<PutRepositoryResponse> putRepository(PutRepositoryRequest request) {
            return execute(PutRepositoryAction.INSTANCE, request);
        }

        @Override
        public void putRepository(PutRepositoryRequest request, ActionListener<PutRepositoryResponse> listener) {
            execute(PutRepositoryAction.INSTANCE, request, listener);
        }

        @Override
        public PutRepositoryRequestBuilder preparePutRepository(String name) {
            return new PutRepositoryRequestBuilder(this, PutRepositoryAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request) {
            return execute(CreateSnapshotAction.INSTANCE, request);
        }

        @Override
        public void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener) {
            execute(CreateSnapshotAction.INSTANCE, request, listener);
        }

        @Override
        public CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name) {
            return new CreateSnapshotRequestBuilder(this, CreateSnapshotAction.INSTANCE, repository, name);
        }

        @Override
        public ActionFuture<GetSnapshotsResponse> getSnapshots(GetSnapshotsRequest request) {
            return execute(GetSnapshotsAction.INSTANCE, request);
        }

        @Override
        public void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener) {
            execute(GetSnapshotsAction.INSTANCE, request, listener);
        }

        @Override
        public GetSnapshotsRequestBuilder prepareGetSnapshots(String repository) {
            return new GetSnapshotsRequestBuilder(this, GetSnapshotsAction.INSTANCE, repository);
        }


        @Override
        public ActionFuture<DeleteSnapshotResponse> deleteSnapshot(DeleteSnapshotRequest request) {
            return execute(DeleteSnapshotAction.INSTANCE, request);
        }

        @Override
        public void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<DeleteSnapshotResponse> listener) {
            execute(DeleteSnapshotAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String name) {
            return new DeleteSnapshotRequestBuilder(this, DeleteSnapshotAction.INSTANCE, repository, name);
        }


        @Override
        public ActionFuture<DeleteRepositoryResponse> deleteRepository(DeleteRepositoryRequest request) {
            return execute(DeleteRepositoryAction.INSTANCE, request);
        }

        @Override
        public void deleteRepository(DeleteRepositoryRequest request, ActionListener<DeleteRepositoryResponse> listener) {
            execute(DeleteRepositoryAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteRepositoryRequestBuilder prepareDeleteRepository(String name) {
            return new DeleteRepositoryRequestBuilder(this, DeleteRepositoryAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<VerifyRepositoryResponse> verifyRepository(VerifyRepositoryRequest request) {
            return execute(VerifyRepositoryAction.INSTANCE, request);
        }

        @Override
        public void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener) {
            execute(VerifyRepositoryAction.INSTANCE, request, listener);
        }

        @Override
        public VerifyRepositoryRequestBuilder prepareVerifyRepository(String name) {
            return new VerifyRepositoryRequestBuilder(this, VerifyRepositoryAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<GetRepositoriesResponse> getRepositories(GetRepositoriesRequest request) {
            return execute(GetRepositoriesAction.INSTANCE, request);
        }

        @Override
        public void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener) {
            execute(GetRepositoriesAction.INSTANCE, request, listener);
        }

        @Override
        public GetRepositoriesRequestBuilder prepareGetRepositories(String... name) {
            return new GetRepositoriesRequestBuilder(this, GetRepositoriesAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request) {
            return execute(RestoreSnapshotAction.INSTANCE, request);
        }

        @Override
        public void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener) {
            execute(RestoreSnapshotAction.INSTANCE, request, listener);
        }

        @Override
        public RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot) {
            return new RestoreSnapshotRequestBuilder(this, RestoreSnapshotAction.INSTANCE, repository, snapshot);
        }


        @Override
        public ActionFuture<SnapshotsStatusResponse> snapshotsStatus(SnapshotsStatusRequest request) {
            return execute(SnapshotsStatusAction.INSTANCE, request);
        }

        @Override
        public void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener) {
            execute(SnapshotsStatusAction.INSTANCE, request, listener);
        }

        @Override
        public SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository) {
            return new SnapshotsStatusRequestBuilder(this, SnapshotsStatusAction.INSTANCE, repository);
        }

        @Override
        public SnapshotsStatusRequestBuilder prepareSnapshotStatus() {
            return new SnapshotsStatusRequestBuilder(this, SnapshotsStatusAction.INSTANCE);
        }
    }

    static class IndicesAdmin implements IndicesAdminClient {

        private final ElasticsearchClient client;

        public IndicesAdmin(ElasticsearchClient client) {
            this.client = client;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
            return client.execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            client.execute(action, request, listener);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
            return client.prepareExecute(action);
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
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
            return new IndicesExistsRequestBuilder(this, IndicesExistsAction.INSTANCE, indices);
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
            return new TypesExistsRequestBuilder(this, TypesExistsAction.INSTANCE, index);
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
            return new IndicesAliasesRequestBuilder(this, IndicesAliasesAction.INSTANCE);
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
            return new GetAliasesRequestBuilder(this, GetAliasesAction.INSTANCE, aliases);
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
            return new AliasesExistRequestBuilder(this, AliasesExistAction.INSTANCE, aliases);
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
            return new GetIndexRequestBuilder(this, GetIndexAction.INSTANCE);
        }

        @Override
        public void clearCache(final ClearIndicesCacheRequest request, final ActionListener<ClearIndicesCacheResponse> listener) {
            execute(ClearIndicesCacheAction.INSTANCE, request, listener);
        }

        @Override
        public ClearIndicesCacheRequestBuilder prepareClearCache(String... indices) {
            return new ClearIndicesCacheRequestBuilder(this, ClearIndicesCacheAction.INSTANCE).setIndices(indices);
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
            return new CreateIndexRequestBuilder(this, CreateIndexAction.INSTANCE, index);
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
            return new DeleteIndexRequestBuilder(this, DeleteIndexAction.INSTANCE, indices);
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
            return new CloseIndexRequestBuilder(this, CloseIndexAction.INSTANCE, indices);
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
            return new OpenIndexRequestBuilder(this, OpenIndexAction.INSTANCE, indices);
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
            return new FlushRequestBuilder(this, FlushAction.INSTANCE).setIndices(indices);
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
            return new GetMappingsRequestBuilder(this, GetMappingsAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<GetMappingsResponse> getMappings(GetMappingsRequest request) {
            return execute(GetMappingsAction.INSTANCE, request);
        }

        @Override
        public GetFieldMappingsRequestBuilder prepareGetFieldMappings(String... indices) {
            return new GetFieldMappingsRequestBuilder(this, GetFieldMappingsAction.INSTANCE, indices);
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
            return new PutMappingRequestBuilder(this, PutMappingAction.INSTANCE).setIndices(indices);
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
            return new OptimizeRequestBuilder(this, OptimizeAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<UpgradeResponse> upgrade(final UpgradeRequest request) {
            return execute(UpgradeAction.INSTANCE, request);
        }

        @Override
        public void upgrade(final UpgradeRequest request, final ActionListener<UpgradeResponse> listener) {
            execute(UpgradeAction.INSTANCE, request, listener);
        }

        @Override
        public UpgradeRequestBuilder prepareUpgrade(String... indices) {
            return new UpgradeRequestBuilder(this, UpgradeAction.INSTANCE).setIndices(indices);
        }


        @Override
        public ActionFuture<UpgradeStatusResponse> upgradeStatus(final UpgradeStatusRequest request) {
            return execute(UpgradeStatusAction.INSTANCE, request);
        }

        @Override
        public void upgradeStatus(final UpgradeStatusRequest request, final ActionListener<UpgradeStatusResponse> listener) {
            execute(UpgradeStatusAction.INSTANCE, request, listener);
        }

        @Override
        public UpgradeStatusRequestBuilder prepareUpgradeStatus(String... indices) {
            return new UpgradeStatusRequestBuilder(this, UpgradeStatusAction.INSTANCE).setIndices(indices);
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
            return new RefreshRequestBuilder(this, RefreshAction.INSTANCE).setIndices(indices);
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
            return new IndicesStatsRequestBuilder(this, IndicesStatsAction.INSTANCE).setIndices(indices);
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
            return new RecoveryRequestBuilder(this, RecoveryAction.INSTANCE).setIndices(indices);
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
            return new IndicesSegmentsRequestBuilder(this, IndicesSegmentsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<IndicesShardStoresResponse> shardStores(IndicesShardStoresRequest request) {
            return execute(IndicesShardStoresAction.INSTANCE, request);
        }

        @Override
        public void shardStores(IndicesShardStoresRequest request, ActionListener<IndicesShardStoresResponse> listener) {
            execute(IndicesShardStoresAction.INSTANCE, request, listener);
        }

        @Override
        public IndicesShardStoreRequestBuilder prepareShardStores(String... indices) {
            return new IndicesShardStoreRequestBuilder(this, IndicesShardStoresAction.INSTANCE, indices);
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
            return new UpdateSettingsRequestBuilder(this, UpdateSettingsAction.INSTANCE).setIndices(indices);
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
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE, index, text);
        }

        @Override
        public AnalyzeRequestBuilder prepareAnalyze(String text) {
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE, null, text);
        }

        @Override
        public AnalyzeRequestBuilder prepareAnalyze() {
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE);
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
            return new PutIndexTemplateRequestBuilder(this, PutIndexTemplateAction.INSTANCE, name);
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
            return new GetIndexTemplatesRequestBuilder(this, GetIndexTemplatesAction.INSTANCE, names);
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
            return new DeleteIndexTemplateRequestBuilder(this, DeleteIndexTemplateAction.INSTANCE, name);
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
            return new ValidateQueryRequestBuilder(this, ValidateQueryAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<RenderSearchTemplateResponse> renderSearchTemplate(final RenderSearchTemplateRequest request) {
            return execute(RenderSearchTemplateAction.INSTANCE, request);
        }

        @Override
        public void renderSearchTemplate(final RenderSearchTemplateRequest request, final ActionListener<RenderSearchTemplateResponse> listener) {
            execute(RenderSearchTemplateAction.INSTANCE, request, listener);
        }

        @Override
        public RenderSearchTemplateRequestBuilder prepareRenderSearchTemplate() {
            return new RenderSearchTemplateRequestBuilder(this, RenderSearchTemplateAction.INSTANCE);
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
            return new PutWarmerRequestBuilder(this, PutWarmerAction.INSTANCE, name);
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
            return new DeleteWarmerRequestBuilder(this, DeleteWarmerAction.INSTANCE);
        }

        @Override
        public GetWarmersRequestBuilder prepareGetWarmers(String... indices) {
            return new GetWarmersRequestBuilder(this, GetWarmersAction.INSTANCE, indices);
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
            return new GetSettingsRequestBuilder(this, GetSettingsAction.INSTANCE, indices);
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
}
