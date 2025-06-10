/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal.support;

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
import org.elasticsearch.action.support.UnsafePlainActionFuture;
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
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractClient implements Client {

    protected final Logger logger;

    protected final Settings settings;
    private final ThreadPool threadPool;
    private final ProjectResolver projectResolver;
    private final AdminClient admin;

    @SuppressWarnings("this-escape")
    public AbstractClient(Settings settings, ThreadPool threadPool, ProjectResolver projectResolver) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.admin = new AdminClient(this);
        this.logger = LogManager.getLogger(this.getClass());
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
    public ProjectResolver projectResolver() {
        return projectResolver;
    }

    @Override
    public final AdminClient admin() {
        return admin;
    }

    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
        ActionType<Response> action,
        Request request
    ) {
        PlainActionFuture<Response> actionFuture = new RefCountedFuture<>();
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

    @Override
    public ActionFuture<DocWriteResponse> index(final IndexRequest request) {
        return execute(TransportIndexAction.TYPE, request);
    }

    @Override
    public void index(final IndexRequest request, final ActionListener<DocWriteResponse> listener) {
        execute(TransportIndexAction.TYPE, request, listener);
    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, null);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index) {
        return new IndexRequestBuilder(this, index);
    }

    @Override
    public ActionFuture<UpdateResponse> update(final UpdateRequest request) {
        return execute(TransportUpdateAction.TYPE, request);
    }

    @Override
    public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        execute(TransportUpdateAction.TYPE, request, listener);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return new UpdateRequestBuilder(this, null, null);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(String index, String id) {
        return new UpdateRequestBuilder(this, index, id);
    }

    @Override
    public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        return execute(TransportDeleteAction.TYPE, request);
    }

    @Override
    public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        execute(TransportDeleteAction.TYPE, request, listener);
    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this, null);
    }

    @Override
    public DeleteRequestBuilder prepareDelete(String index, String id) {
        return prepareDelete().setIndex(index).setId(id);
    }

    @Override
    public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        return execute(TransportBulkAction.TYPE, request);
    }

    @Override
    public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        execute(TransportBulkAction.TYPE, request, listener);
    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this);
    }

    @Override
    public BulkRequestBuilder prepareBulk(@Nullable String globalIndex) {
        return new BulkRequestBuilder(this, globalIndex);
    }

    @Override
    public ActionFuture<GetResponse> get(final GetRequest request) {
        return execute(TransportGetAction.TYPE, request);
    }

    @Override
    public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        execute(TransportGetAction.TYPE, request, listener);
    }

    @Override
    public GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this, null);
    }

    @Override
    public GetRequestBuilder prepareGet(String index, String id) {
        return prepareGet().setIndex(index).setId(id);
    }

    @Override
    public ActionFuture<MultiGetResponse> multiGet(final MultiGetRequest request) {
        return execute(TransportMultiGetAction.TYPE, request);
    }

    @Override
    public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        execute(TransportMultiGetAction.TYPE, request, listener);
    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this);
    }

    @Override
    public ActionFuture<SearchResponse> search(final SearchRequest request) {
        return execute(TransportSearchAction.TYPE, request);
    }

    @Override
    public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        execute(TransportSearchAction.TYPE, request, listener);
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        return execute(TransportSearchScrollAction.TYPE, request);
    }

    @Override
    public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        execute(TransportSearchScrollAction.TYPE, request, listener);
    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return new SearchScrollRequestBuilder(this, scrollId);
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return execute(TransportMultiSearchAction.TYPE, request);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        execute(TransportMultiSearchAction.TYPE, request, listener);
    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return new MultiSearchRequestBuilder(this);
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
        return new TermVectorsRequestBuilder(this);
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors(String index, String id) {
        return new TermVectorsRequestBuilder(this, index, id);
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
        return new MultiTermVectorsRequestBuilder(this);
    }

    @Override
    public ExplainRequestBuilder prepareExplain(String index, String id) {
        return new ExplainRequestBuilder(this, index, id);
    }

    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return execute(TransportExplainAction.TYPE, request);
    }

    @Override
    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        execute(TransportExplainAction.TYPE, request, listener);
    }

    @Override
    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        execute(TransportClearScrollAction.TYPE, request, listener);
    }

    @Override
    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return execute(TransportClearScrollAction.TYPE, request);
    }

    @Override
    public ClearScrollRequestBuilder prepareClearScroll() {
        return new ClearScrollRequestBuilder(this);
    }

    @Override
    public void fieldCaps(FieldCapabilitiesRequest request, ActionListener<FieldCapabilitiesResponse> listener) {
        execute(TransportFieldCapabilitiesAction.TYPE, request, listener);
    }

    @Override
    public ActionFuture<FieldCapabilitiesResponse> fieldCaps(FieldCapabilitiesRequest request) {
        return execute(TransportFieldCapabilitiesAction.TYPE, request);
    }

    @Override
    public FieldCapabilitiesRequestBuilder prepareFieldCaps(String... indices) {
        return new FieldCapabilitiesRequestBuilder(this, indices);
    }

    @Override
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

    @Override
    public Client projectClient(ProjectId projectId) {
        // We only take the shortcut when the given project ID matches the "current" project ID. If it doesn't, we'll let #executeOnProject
        // take care of error handling.
        if (projectResolver.supportsMultipleProjects() == false && projectId.equals(projectResolver.getProjectId())) {
            return this;
        }
        return new FilterClient(this) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                projectResolver.executeOnProject(projectId, () -> super.doExecute(action, request, listener));
            }
        };
    }

    /**
     * Same as {@link PlainActionFuture} but for use with {@link RefCounted} result types. Unlike {@code PlainActionFuture} this future
     * acquires a reference to its result. This means that the result reference must be released by a call to {@link RefCounted#decRef()}
     * on the result before it goes out of scope.
     * @param <R> reference counted result type
     */
    // todo: the use of UnsafePlainActionFuture here is quite broad, we should find a better way to be more specific
    // (unless making all usages safe is easy).
    private static class RefCountedFuture<R extends RefCounted> extends UnsafePlainActionFuture<R> {

        private RefCountedFuture() {
            super(ThreadPool.Names.GENERIC);
        }

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
