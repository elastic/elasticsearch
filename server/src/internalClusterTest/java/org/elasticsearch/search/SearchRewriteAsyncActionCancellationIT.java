/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Async actions registered during the coordinator rewrite of a search (fetching a query vector, running query-time
 * inference, looking up terms...) must be child tasks of the search task, so that cancelling the search, for instance
 * because its HTTP client disconnected, also cancels the work they started.
 */
public class SearchRewriteAsyncActionCancellationIT extends ESIntegTestCase {

    private static final String INDEX = "test";

    private static volatile CountDownLatch blockingActionStarted;
    private static final AtomicBoolean blockingActionCancelled = new AtomicBoolean();
    private static final AtomicReference<ActionListener<ActionResponse.Empty>> blockedListener = new AtomicReference<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BlockingActionPlugin.class);
    }

    @Before
    public void resetBlockingAction() {
        blockingActionStarted = new CountDownLatch(1);
        blockingActionCancelled.set(false);
        blockedListener.set(null);
    }

    @After
    public void releaseBlockingAction() {
        // unblocks the rewrite if the cancellation never reached the blocking action
        ActionListener<ActionResponse.Empty> listener = blockedListener.getAndSet(null);
        if (listener != null) {
            listener.onResponse(ActionResponse.Empty.INSTANCE);
        }
    }

    public void testCancellingSearchCancelsRewriteAsyncAction() throws Exception {
        createIndex(INDEX);
        indexDoc(INDEX, "1", "field", "value");
        refresh(INDEX);

        SearchRequest request = new SearchRequest(INDEX).source(new SearchSourceBuilder().retriever(new BlockingRetrieverBuilder()));
        ActionFuture<SearchResponse> future = client().search(request);
        safeAwait(blockingActionStarted);

        List<TaskInfo> searchTasks = clusterAdmin().prepareListTasks().setActions(TransportSearchAction.TYPE.name()).get().getTasks();
        assertThat(searchTasks, hasSize(1));
        TaskId searchTaskId = searchTasks.get(0).taskId();

        List<TaskInfo> blockingTasks = clusterAdmin().prepareListTasks().setActions(BlockingAction.NAME).get().getTasks();
        assertThat(blockingTasks, hasSize(1));
        assertThat(blockingTasks.get(0).parentTaskId(), equalTo(searchTaskId));

        clusterAdmin().prepareCancelTasks().setTargetTaskId(searchTaskId).get();
        assertBusy(() -> assertTrue(blockingActionCancelled.get()));

        Exception e = expectThrows(Exception.class, future::actionGet);
        assertThat(ExceptionsHelper.unwrap(e, TaskCancelledException.class), notNullValue());
    }

    /**
     * Registers an async action that calls {@link BlockingAction} on its first rewrite, then rewrites to a match_all retriever.
     */
    private static class BlockingRetrieverBuilder extends RetrieverBuilder {
        private final SetOnce<RetrieverBuilder> rewritten;

        BlockingRetrieverBuilder() {
            this(new SetOnce<>());
        }

        private BlockingRetrieverBuilder(SetOnce<RetrieverBuilder> rewritten) {
            this.rewritten = rewritten;
        }

        @Override
        public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
            if (rewritten.get() != null) {
                return rewritten.get();
            }
            SetOnce<RetrieverBuilder> next = new SetOnce<>();
            ctx.registerAsyncAction(
                (client, listener) -> client.execute(BlockingAction.INSTANCE, new BlockingAction.Request(), ActionListener.wrap(r -> {
                    next.set(new StandardRetrieverBuilder(QueryBuilders.matchAllQuery()));
                    listener.onResponse(null);
                }, listener::onFailure))
            );
            return new BlockingRetrieverBuilder(next);
        }

        @Override
        public QueryBuilder topDocsQuery() {
            throw new AssertionError("must be rewritten first");
        }

        @Override
        public void extractToSearchSourceBuilder(SearchSourceBuilder sourceBuilder, boolean compoundUsed) {
            throw new AssertionError("must be rewritten first");
        }

        @Override
        public String getName() {
            return "blocking";
        }

        @Override
        protected void doToXContent(XContentBuilder builder, Params params) {}

        @Override
        protected boolean doEquals(Object o) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }

    /**
     * A cancellable action that only completes once its task is cancelled.
     */
    public static class BlockingAction extends ActionType<ActionResponse.Empty> {
        static final String NAME = "internal:test/search/rewrite/blocking";
        static final BlockingAction INSTANCE = new BlockingAction();

        private BlockingAction() {
            super(NAME);
        }

        public static class Request extends ActionRequest {
            Request() {}

            Request(StreamInput in) throws IOException {
                super(in);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new CancellableTask(id, type, action, "blocking rewrite action", parentTaskId, headers);
            }
        }
    }

    public static class TransportBlockingAction extends HandledTransportAction<BlockingAction.Request, ActionResponse.Empty> {
        @Inject
        public TransportBlockingAction(TransportService transportService, ActionFilters actionFilters) {
            super(BlockingAction.NAME, transportService, actionFilters, BlockingAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }

        @Override
        protected void doExecute(Task task, BlockingAction.Request request, ActionListener<ActionResponse.Empty> listener) {
            CancellableTask cancellableTask = (CancellableTask) task;
            blockedListener.set(listener);
            cancellableTask.addListener(() -> {
                ActionListener<ActionResponse.Empty> blocked = blockedListener.getAndSet(null);
                if (blocked != null) {
                    blockingActionCancelled.set(true);
                    blocked.onFailure(new TaskCancelledException(cancellableTask.getReasonCancelled()));
                }
            });
            blockingActionStarted.countDown();
        }
    }

    public static class BlockingActionPlugin extends Plugin implements ActionPlugin {
        @Override
        public Collection<ActionHandler> getActions() {
            return List.of(new ActionHandler(BlockingAction.INSTANCE, TransportBlockingAction.class));
        }
    }
}
