/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequestBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequestBuilder;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequestBuilder;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A client provides a one stop interface for performing actions/operations against the cluster.
 * <p>
 * All operations performed are asynchronous by nature. Each action/operation has two flavors, the first
 * simply returns an {@link org.elasticsearch.action.ActionFuture}, while the second accepts an
 * {@link org.elasticsearch.action.ActionListener}.
 * <p>
 * A client can be retrieved from a started {@link org.elasticsearch.node.Node}.
 *
 * @see org.elasticsearch.node.Node#client()
 */
public abstract class Client implements ElasticsearchClient {

    @UpdateForV9 // Note: This setting is registered only for bwc. The value is never read.
    public static final Setting<String> CLIENT_TYPE_SETTING_S = new Setting<>("client.type", "node", s -> switch (s) {
        case "node", "transport" -> s;
        default -> throw new IllegalArgumentException("Can't parse [client.type] must be one of [node, transport]");
    }, Property.NodeScope, Property.Deprecated);

    protected final Logger logger;
    protected final Settings settings;
    protected final ThreadPool threadPool;
    protected final AdminClient admin;

    public Client(Settings settings, ThreadPool threadPool) {
        this.logger = LogManager.getLogger(this.getClass());
        this.settings = settings;
        this.threadPool = threadPool;
        this.admin = new AdminClient(this);
    }

    @Override
    public final ThreadPool threadPool() {
        return this.threadPool;
    }

    public final AdminClient admin() {
        return admin;
    }

    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
        ActionType<Response> action,
        Request request
    ) {
        PlainActionFuture<Response> actionFuture = new Client.RefCountedFuture<>();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    /**
     * This is the single execution point of *all* clients.
     */
    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        try {
            doExecute(action, request, listener);
        } catch (Exception e) {
            assert false : new AssertionError(e);
            listener.onFailure(e);
        }
    }

    protected abstract <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );

    public ActionFuture<DocWriteResponse> index(final IndexRequest request) {
        return execute(TransportIndexAction.TYPE, request);
    }

    public void index(final IndexRequest request, final ActionListener<DocWriteResponse> listener) {
        execute(TransportIndexAction.TYPE, request, listener);
    }

    public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, null);
    }

    public IndexRequestBuilder prepareIndex(String index) {
        return new IndexRequestBuilder(this, index);
    }

    public ActionFuture<UpdateResponse> update(final UpdateRequest request) {
        return execute(TransportUpdateAction.TYPE, request);
    }

    public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        execute(TransportUpdateAction.TYPE, request, listener);
    }

    public UpdateRequestBuilder prepareUpdate() {
        return new UpdateRequestBuilder(this, null, null);
    }

    public UpdateRequestBuilder prepareUpdate(String index, String id) {
        return new UpdateRequestBuilder(this, index, id);
    }

    public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        return execute(TransportDeleteAction.TYPE, request);
    }

    public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        execute(TransportDeleteAction.TYPE, request, listener);
    }

    public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this, null);
    }

    public DeleteRequestBuilder prepareDelete(String index, String id) {
        return prepareDelete().setIndex(index).setId(id);
    }

    public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        return execute(TransportBulkAction.TYPE, request);
    }

    public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        execute(TransportBulkAction.TYPE, request, listener);
    }

    public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this);
    }

    public BulkRequestBuilder prepareBulk(@Nullable String globalIndex) {
        return new BulkRequestBuilder(this, globalIndex);
    }

    public ActionFuture<GetResponse> get(final GetRequest request) {
        return execute(TransportGetAction.TYPE, request);
    }

    public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        execute(TransportGetAction.TYPE, request, listener);
    }

    public GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this, null);
    }

    public GetRequestBuilder prepareGet(String index, String id) {
        return prepareGet().setIndex(index).setId(id);
    }

    public ActionFuture<MultiGetResponse> multiGet(final MultiGetRequest request) {
        return execute(TransportMultiGetAction.TYPE, request);
    }

    public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        execute(TransportMultiGetAction.TYPE, request, listener);
    }

    public MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this);
    }

    public ActionFuture<SearchResponse> search(final SearchRequest request) {
        return execute(TransportSearchAction.TYPE, request);
    }

    public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        execute(TransportSearchAction.TYPE, request, listener);
    }

    public SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        return execute(TransportSearchScrollAction.TYPE, request);
    }

    public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        execute(TransportSearchScrollAction.TYPE, request, listener);
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return new SearchScrollRequestBuilder(this, scrollId);
    }

    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return execute(TransportMultiSearchAction.TYPE, request);
    }

    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        execute(TransportMultiSearchAction.TYPE, request, listener);
    }

    public MultiSearchRequestBuilder prepareMultiSearch() {
        return new MultiSearchRequestBuilder(this);
    }

    public ActionFuture<TermVectorsResponse> termVectors(final TermVectorsRequest request) {
        return execute(TermVectorsAction.INSTANCE, request);
    }

    public void termVectors(final TermVectorsRequest request, final ActionListener<TermVectorsResponse> listener) {
        execute(TermVectorsAction.INSTANCE, request, listener);
    }

    public TermVectorsRequestBuilder prepareTermVectors() {
        return new TermVectorsRequestBuilder(this);
    }

    public TermVectorsRequestBuilder prepareTermVectors(String index, String id) {
        return new TermVectorsRequestBuilder(this, index, id);
    }

    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(final MultiTermVectorsRequest request) {
        return execute(MultiTermVectorsAction.INSTANCE, request);
    }

    public void multiTermVectors(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        execute(MultiTermVectorsAction.INSTANCE, request, listener);
    }

    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return new MultiTermVectorsRequestBuilder(this);
    }

    public ExplainRequestBuilder prepareExplain(String index, String id) {
        return new ExplainRequestBuilder(this, index, id);
    }

    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return execute(TransportExplainAction.TYPE, request);
    }

    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        execute(TransportExplainAction.TYPE, request, listener);
    }

    public ClearScrollRequestBuilder prepareClearScroll() {
        return new ClearScrollRequestBuilder(this);
    }

    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return execute(TransportClearScrollAction.TYPE, request);
    }

    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        execute(TransportClearScrollAction.TYPE, request, listener);
    }

    public FieldCapabilitiesRequestBuilder prepareFieldCaps(String... indices) {
        return new FieldCapabilitiesRequestBuilder(this, indices);
    }

    public ActionFuture<FieldCapabilitiesResponse> fieldCaps(FieldCapabilitiesRequest request) {
        return execute(TransportFieldCapabilitiesAction.TYPE, request);
    }

    public void fieldCaps(FieldCapabilitiesRequest request, ActionListener<FieldCapabilitiesResponse> listener) {
        execute(TransportFieldCapabilitiesAction.TYPE, request, listener);
    }

    public final Settings settings() {
        return this.settings;
    }

    public Client filterWithHeader(Map<String, String> headers) {
        return new FilterClient(this) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                ThreadContext threadContext = threadPool().getThreadContext();
                try (ThreadContext.StoredContext ctx = threadContext.stashAndMergeHeaders(headers)) {
                    super.doExecute(action, request, listener);
                }
            }
        };
    }

    /**
     * Returns a client to a remote cluster with the given cluster alias.
     *
     * @throws IllegalArgumentException if the given clusterAlias doesn't exist
     * @throws UnsupportedOperationException if this functionality is not available on this client.
     */
    public RemoteClusterClient getRemoteClusterClient(
        String clusterAlias,
        Executor responseExecutor,
        RemoteClusterService.DisconnectedStrategy disconnectedStrategy
    ) {
        throw new UnsupportedOperationException("this client doesn't support remote cluster connections");
    }

    /**
     * Same as {@link PlainActionFuture} but for use with {@link RefCounted} result types. Unlike {@code PlainActionFuture} this future
     * acquires a reference to its result. This means that the result reference must be released by a call to {@link RefCounted#decRef()}
     * on the result before it goes out of scope.
     * @param <R> reference counted result type
     */
    private static class RefCountedFuture<R extends RefCounted> extends PlainActionFuture<R> {

        @Override
        public final void onResponse(R result) {
            result.mustIncRef();
            if (set(result) == false) {
                result.decRef();
            }
        }

        private final AtomicBoolean getCalled = new AtomicBoolean(false);

        @Override
        public R get() throws InterruptedException, ExecutionException {
            final boolean firstCall = getCalled.compareAndSet(false, true);
            if (firstCall == false) {
                final IllegalStateException ise = new IllegalStateException("must only call .get() once per instance to avoid leaks");
                assert false : ise;
                throw ise;
            }
            return super.get();
        }
    }
}
