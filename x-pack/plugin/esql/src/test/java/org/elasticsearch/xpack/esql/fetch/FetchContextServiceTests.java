/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The keep-alive requests that a data node serves for the coordinators of the fetch phase.
 */
public class FetchContextServiceTests extends FetchContextsTestCase {
    private static final String OTHER_INDEX = "fetch-contexts-other";

    @Override
    protected Settings nodeSettings() {
        // frees an expired context soon after its keep-alive passed
        return Settings.builder()
            .put(super.nodeSettings())
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(100))
            .build();
    }

    /**
     * A keep-alive counts as a use of the context, so its keep-alive starts again. Refreshed contexts stay open for
     * several keep-alives, and expire once nobody refreshes them.
     */
    public void testAKeepAliveRestartsTheKeepAliveOfAContext() throws Exception {
        TimeValue keepAlive = TimeValue.timeValueMillis(500);
        updateClusterSettings(Settings.builder().put(SearchService.DEFAULT_KEEPALIVE_SETTING.getKey(), keepAlive).build());
        try {
            createIndexWithDocs(INDEX);
            List<ShardSearchContextId> open = respond(INDEX);
            FetchContextKeepAliveRequest request = new FetchContextKeepAliveRequest(QUERY_INDICES, open, keepAlive);
            long until = System.nanoTime() + 4 * keepAlive.nanos();
            while (System.nanoTime() < until) {
                service.keepAlive(request);
                safeSleep(keepAlive.millis() / 5);
            }
            assertThat("refreshed contexts stay open", searchService().getActiveContexts(), equalTo(SHARDS));

            assertBusy(() -> assertThat("the reaper frees them", searchService().getActiveContexts(), equalTo(0)));
        } finally {
            updateClusterSettings(Settings.builder().putNull(SearchService.DEFAULT_KEEPALIVE_SETTING.getKey()).build());
        }
    }

    /**
     * The context stays open at least as long as the request asks, whatever the default keep-alive of the node.
     */
    public void testAKeepAliveRaisesTheKeepAliveOfAContextToTheRequestedOne() throws Exception {
        createIndexWithDocs(INDEX);
        List<ShardSearchContextId> open = respond(INDEX);
        TimeValue longer = TimeValue.timeValueMillis(searchService().getDefaultKeepAliveInMillis() + between(1, 3600) * 1000L);

        service.keepAlive(new FetchContextKeepAliveRequest(QUERY_INDICES, open, longer));

        for (ShardSearchContextId id : open) {
            assertThat(keepAliveOf(id), equalTo(longer.millis()));
        }
        free(open);
    }

    public void testAKeepAliveNeverShortensTheKeepAliveOfAContext() throws Exception {
        createIndexWithDocs(INDEX);
        List<ShardSearchContextId> open = respond(INDEX);
        long nodeDefault = searchService().getDefaultKeepAliveInMillis();

        service.keepAlive(new FetchContextKeepAliveRequest(QUERY_INDICES, open, TimeValue.timeValueMillis(between(1, 1000))));

        for (ShardSearchContextId id : open) {
            assertThat(keepAliveOf(id), equalTo(nodeDefault));
        }
        free(open);
    }

    /**
     * A freed or expired context is skipped, and the other contexts of the request are still kept alive.
     */
    public void testAKeepAliveSkipsAContextThatIsGone() throws Exception {
        createIndexWithDocs(INDEX);
        List<ShardSearchContextId> open = respond(INDEX);
        ShardSearchContextId gone = randomFrom(open);
        searchService().freeReaderContext(gone);
        TimeValue longer = TimeValue.timeValueMillis(searchService().getDefaultKeepAliveInMillis() + 60_000);

        service.keepAlive(new FetchContextKeepAliveRequest(QUERY_INDICES, open, longer));

        List<ShardSearchContextId> left = new ArrayList<>(open);
        left.remove(gone);
        for (ShardSearchContextId id : left) {
            assertThat(keepAliveOf(id), equalTo(longer.millis()));
        }
        assertFalse(isOpen(gone));
        free(left);
    }

    /**
     * The lookup ignores the searcher id, and the reader of another searcher holds other documents.
     */
    public void testAKeepAliveSkipsAContextOfAnotherSearcher() throws Exception {
        createIndexWithDocs(INDEX);
        List<ShardSearchContextId> open = respond(INDEX);
        ShardSearchContextId id = randomFrom(open);
        ShardSearchContextId otherSearcher = new ShardSearchContextId(id.getSessionId(), id.getId(), "another-searcher");
        long nodeDefault = searchService().getDefaultKeepAliveInMillis();

        service.keepAlive(new FetchContextKeepAliveRequest(QUERY_INDICES, List.of(otherSearcher), TimeValue.timeValueHours(1)));

        assertThat(keepAliveOf(id), equalTo(nodeDefault));
        free(open);
    }

    /**
     * A scroll or a point in time isn't a context of the fetch phase, so a keep-alive must not extend it.
     */
    public void testAKeepAliveSkipsAScrollOrAPointInTime() throws Exception {
        createIndexWithDocs(INDEX);
        ShardId shardId = new ShardId(resolveIndex(INDEX), 0);
        TimeValue pointInTimeKeepAlive = TimeValue.timeValueMinutes(5);
        PlainActionFuture<ShardSearchContextId> opened = new PlainActionFuture<>();
        searchService().openReaderContext(shardId, pointInTimeKeepAlive, SplitShardCountSummary.IRRELEVANT, opened);
        ShardSearchContextId pointInTime = opened.actionGet(TimeValue.timeValueSeconds(10));
        try {
            service.keepAlive(new FetchContextKeepAliveRequest(QUERY_INDICES, List.of(pointInTime), TimeValue.timeValueHours(1)));

            assertThat(keepAliveOf(pointInTime), equalTo(pointInTimeKeepAlive.millis()));
        } finally {
            searchService().freeReaderContext(pointInTime);
        }
        assertAllFreed();
    }

    /**
     * A context of an index the request may not read is skipped, and the other contexts of the request are still kept
     * alive.
     */
    public void testAKeepAliveSkipsAContextOfAnIndexTheRequestMayNotRead() throws Exception {
        createIndexWithDocs(INDEX);
        createIndexWithDocs(OTHER_INDEX);
        List<ShardSearchContextId> readable = respond(INDEX);
        List<ShardSearchContextId> forbidden = respond(OTHER_INDEX);
        List<ShardSearchContextId> both = new ArrayList<>(readable);
        both.addAll(forbidden);
        long nodeDefault = searchService().getDefaultKeepAliveInMillis();
        TimeValue longer = TimeValue.timeValueMillis(nodeDefault + 60_000);
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();

        try (var ignored = threadContext.stashContext()) {
            IndicesAccessControl onlyIndex = new IndicesAccessControl(
                true,
                Map.of(INDEX, IndicesAccessControl.IndexAccessControl.ALLOW_ALL)
            );
            AuthorizationServiceField.INDICES_PERMISSIONS_VALUE.set(threadContext, onlyIndex);
            service.keepAlive(new FetchContextKeepAliveRequest(QUERY_INDICES, randomFrom(both, both.reversed()), longer));
        }

        for (ShardSearchContextId id : readable) {
            assertThat(keepAliveOf(id), equalTo(longer.millis()));
        }
        for (ShardSearchContextId id : forbidden) {
            assertThat(keepAliveOf(id), equalTo(nodeDefault));
        }
        free(both);
    }

    /**
     * The answer is the same whether the contexts exist or not, so it tells nobody which ones do.
     */
    public void testTheAnswerIsTheSameForContextsThatDontExist() throws Exception {
        registerKeepAliveHandler();
        createIndexWithDocs(INDEX);
        List<ShardSearchContextId> open = respond(INDEX);
        List<ShardSearchContextId> missing = new ArrayList<>();
        for (ShardSearchContextId id : open) {
            missing.add(new ShardSearchContextId(id.getSessionId(), id.getId() + 1000));
        }
        missing.add(new ShardSearchContextId("another-node", 1));

        assertThat(sendKeepAlive(open), sameInstance(ActionResponse.Empty.INSTANCE));
        assertThat(sendKeepAlive(missing), sameInstance(ActionResponse.Empty.INSTANCE));
        free(open);
    }

    /**
     * Dropping a reference to a context can close its reader, which must not happen on a transport thread. The search
     * pool could reject the request when it is busy, which is when slow queries need their contexts the most.
     */
    public void testKeepAlivesRunOffTheTransportThreads() {
        registerKeepAliveHandler();
        TransportService transportService = getInstanceFromNode(TransportService.class);

        assertThat(
            transportService.getRequestHandler(FetchContextService.KEEP_ALIVE_ACTION_NAME).getExecutor(),
            sameInstance(getInstanceFromNode(ThreadPool.class).generic())
        );
    }

    private long keepAliveOf(ShardSearchContextId id) {
        return searchService().findReaderContext(id, new LookupRequest(), null).keepAlive();
    }

    private void free(List<ShardSearchContextId> ids) throws Exception {
        ids.forEach(id -> searchService().freeReaderContext(id));
        assertAllFreed();
    }

    /**
     * Sends a keep-alive for {@code ids} to this node, through its handler, and returns the answer.
     */
    private ActionResponse.Empty sendKeepAlive(List<ShardSearchContextId> ids) {
        PlainActionFuture<ActionResponse.Empty> answered = new PlainActionFuture<>();
        getInstanceFromNode(TransportService.class).sendRequest(
            localNode(),
            FetchContextService.KEEP_ALIVE_ACTION_NAME,
            new FetchContextKeepAliveRequest(QUERY_INDICES, ids, TimeValue.timeValueMinutes(5)),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(answered, in -> ActionResponse.Empty.INSTANCE, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
        return answered.actionGet(TimeValue.timeValueSeconds(10));
    }
}
