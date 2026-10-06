/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The fetch contexts of one data node request, on a real {@link SearchService}. {@link MockSearchService} tracks every
 * registered context, and the check after each test fails on any context a test left open.
 */
public class NodeFetchContextsTests extends ESSingleNodeTestCase {
    private static final String INDEX = "fetch-contexts";
    /** Indices whose name starts with this prefix get no {@link FetchContextListener}. */
    private static final String UNLISTENED_PREFIX = "unlistened";
    private static final int SHARDS = 3;

    /**
     * Registers a {@link FetchContextListener} on every index, the way the ES|QL plugin does.
     */
    public static class FetchContextListenerPlugin extends Plugin {
        private final SetOnce<FetchContextListener> listener = new SetOnce<>();

        @Override
        public Collection<?> createComponents(PluginServices services) {
            listener.set(new FetchContextListener(services.clusterService().getSettings(), services.threadPool().getThreadContext()));
            return List.of();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getIndex().getName().startsWith(UNLISTENED_PREFIX) == false) {
                indexModule.addSearchOperationListener(listener.get());
                indexModule.addIndexEventListener(listener.get());
            }
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(FetchContextService.MAX_OPEN_CONTEXTS);
        }
    }

    /**
     * A request of the fetch phase, which may look a fetch context up.
     */
    private static class FetchRequest extends AbstractTransportRequest implements FetchContextRequest {}

    /**
     * Any other request, like the fetch phase of a search.
     */
    private static class OtherRequest extends AbstractTransportRequest {}

    private final List<SearchContext> querySearchContexts = new ArrayList<>();
    private FetchContextService service;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MockSearchService.TestPlugin.class, FetchContextListenerPlugin.class);
    }

    @Before
    public void createService() {
        service = new FetchContextService(
            getInstanceFromNode(SearchService.class),
            getInstanceFromNode(ThreadPool.class),
            getInstanceFromNode(ClusterService.class).getClusterSettings()
        );
    }

    @After
    public void closeQuerySearchContexts() {
        querySearchContexts.forEach(SearchContext::close);
        querySearchContexts.clear();
        ((MockSearchService) getInstanceFromNode(SearchService.class)).setOnCreateSearchContext(context -> {});
        ((MockSearchService) getInstanceFromNode(SearchService.class)).setOnPutContext(context -> {});
    }

    public void testOpensRegisteredContexts() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        List<SearchContext> opened = openEveryShard(contexts, INDEX);

        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));
        assertThat(service.openContexts(), equalTo(SHARDS));
        for (SearchContext searchContext : opened) {
            ReaderContext readerContext = searchContext.readerContext();
            assertThat(searchService().findReaderContext(readerContext.id(), new FetchRequest(), null), sameInstance(readerContext));
        }

        contexts.freeAll("the test is done");
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * After the response only the contexts whose rows survived the node cut stay open, and the response lists exactly
     * those.
     */
    public void testResponseKeepsTheContributingContexts() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        List<SearchContext> opened = openEveryShard(contexts, INDEX);
        List<SearchContext> contributing = randomSubsetOf(between(1, SHARDS - 1), opened);
        contributing.forEach(contexts::originOf);

        List<OpenContextInfo> open = contexts.listOpenContributing();
        assertThat(
            open,
            containsInAnyOrder(
                contributing.stream()
                    .map(c -> new OpenContextInfo(c.shardTarget().getShardId(), c.readerContext().id()))
                    .toArray(OpenContextInfo[]::new)
            )
        );

        contexts.responded();
        endRequest(contexts);
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(contributing.size())));
        assertThat(service.openContexts(), equalTo(contributing.size()));

        // the coordinator owns the rest now, and frees them at the end of the query
        for (OpenContextInfo context : open) {
            assertTrue(searchService().freeReaderContext(context.contextId()));
        }
        assertAllFreed();
    }

    /**
     * The origin names the registered context, so the fetch phase finds it again on this node.
     */
    public void testOriginNamesTheRegisteredContext() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        SearchContext searchContext = contexts.open(shardRequest(new ShardId(resolveIndex(INDEX), 0)));
        querySearchContexts.add(searchContext);

        DocRefOrigin origin = contexts.originOf(searchContext);

        assertThat(origin.clusterAlias(), equalTo(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));
        assertThat(origin.nodeId(), equalTo(getInstanceFromNode(ClusterService.class).localNode().getId()));
        assertThat(origin.shardId(), equalTo(new ShardId(resolveIndex(INDEX), 0)));
        assertThat(origin.contextId(), equalTo(searchContext.readerContext().id()));
        contexts.freeAll("the test is done");
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Only a request of the fetch phase from the user who opened the context finds it. Everyone else sees no context, and
     * the context stays open for its owner.
     */
    public void testOnlyTheOwnersFetchRequestsFindTheContext() throws Exception {
        createIndexWithDocs(INDEX);
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        Authentication owner = AuthenticationTestHelper.builder().user(new User("owner")).build();
        Authentication other = AuthenticationTestHelper.builder().user(new User("other")).build();
        NodeFetchContexts contexts = newNodeContexts();
        ReaderContext readerContext;
        try (var ignored = threadContext.stashContext()) {
            owner.writeToContext(threadContext);
            SearchContext searchContext = contexts.open(shardRequest(new ShardId(resolveIndex(INDEX), 0)));
            querySearchContexts.add(searchContext);
            readerContext = searchContext.readerContext();
            assertThat(readerContext.getFromContext(AuthenticationField.AUTHENTICATION_KEY), equalTo(owner));

            assertThat(searchService().findReaderContext(readerContext.id(), new FetchRequest(), null), sameInstance(readerContext));
            expectThrows(
                SearchContextMissingException.class,
                () -> searchService().findReaderContext(readerContext.id(), new OtherRequest(), null)
            );
        }
        try (var ignored = threadContext.stashContext()) {
            other.writeToContext(threadContext);
            expectThrows(
                SearchContextMissingException.class,
                () -> searchService().findReaderContext(readerContext.id(), new FetchRequest(), null)
            );
        }
        assertThat("a rejected lookup leaves the context open", searchService().getActiveContexts(), equalTo(1));
        contexts.freeAll("the test is done");
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * A failed or cancelled request frees every context at once. The readers stay open until the request ends, after
     * the search contexts of its query phase closed, so the request drops the last reference to each.
     */
    public void testFreeAllFreesEveryContext() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        List<SearchContext> opened = openEveryShard(contexts, INDEX);
        randomSubsetOf(opened).forEach(contexts::originOf);

        contexts.freeAll("the request failed");
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(0)));
        closeQuerySearchContexts();
        assertThat("the request still holds every reader", service.openContexts(), equalTo(SHARDS));

        contexts.close();
        assertAllFreed();
        assertThat(contexts.listOpenContributing(), empty());
    }

    /**
     * A request that ends without a response or a failure frees every context, also the contributing ones.
     */
    public void testEndWithoutResponseFreesEveryContext() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        openEveryShard(contexts, INDEX).forEach(contexts::originOf);

        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Cancellation can close the contexts of a request while it still opens some. Those fail and leave nothing behind.
     */
    public void testOpenAfterFreeAllFails() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        contexts.freeAll("the request was cancelled");

        expectThrows(TaskCancelledException.class, () -> contexts.open(shardRequest(new ShardId(resolveIndex(INDEX), 0))));
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Cancellation can also come while a context opens, after the listener bound it. That open fails and leaves nothing.
     */
    public void testFreeAllDuringOpenLeavesNothing() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        ((MockSearchService) searchService()).setOnCreateSearchContext(context -> contexts.freeAll("the request was cancelled"));

        expectThrows(TaskCancelledException.class, () -> contexts.open(shardRequest(new ShardId(resolveIndex(INDEX), 0))));
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Removing the index can race with the open after the listener bound the context. The open fails and the context
     * closes, and it leaves the limit exactly once.
     */
    public void testIndexRemovalDuringOpenLeavesNothing() throws Exception {
        createIndexWithDocs(INDEX);
        Index index = resolveIndex(INDEX);
        ((MockSearchService) searchService()).setOnPutContext(context -> { throw new IndexNotFoundException(index); });
        NodeFetchContexts contexts = newNodeContexts();

        expectThrows(IndexNotFoundException.class, () -> contexts.open(shardRequest(new ShardId(index, 0))));
        endRequest(contexts);
        assertAllFreed();
    }

    public void testRejectsContextsBeyondTheLimit() throws Exception {
        createIndexWithDocs(INDEX);
        updateMaxOpenContexts(1);
        try {
            NodeFetchContexts contexts = newNodeContexts();
            querySearchContexts.add(contexts.open(shardRequest(new ShardId(resolveIndex(INDEX), 0))));

            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> contexts.open(shardRequest(new ShardId(resolveIndex(INDEX), 1)))
            );
            assertThat(e.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(e.getMessage(), containsString("[esql.fetch.max_open_contexts]"));
            assertThat(service.openContexts(), equalTo(1));

            contexts.freeAll("the test is done");
            endRequest(contexts);
            assertAllFreed();
        } finally {
            updateMaxOpenContexts(null);
        }
    }

    /**
     * A context whose query phase can't start is freed at once.
     */
    public void testFailedSearchContextLeavesNothing() throws Exception {
        createIndexWithDocs(INDEX);
        ((MockSearchService) getInstanceFromNode(SearchService.class)).setOnCreateSearchContext(context -> {
            throw new IllegalStateException("simulated failure");
        });
        NodeFetchContexts contexts = newNodeContexts();

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> contexts.open(shardRequest(new ShardId(resolveIndex(INDEX), 0)))
        );
        assertThat(e.getMessage(), equalTo("simulated failure"));
        assertAllFreed();
    }

    /**
     * Without the listener nobody binds the owner, so anyone who guesses the id could use the context. The open fails
     * and frees it.
     */
    public void testIndexWithoutTheListenerFails() throws Exception {
        String index = UNLISTENED_PREFIX + "-index";
        createIndexWithDocs(index);
        NodeFetchContexts contexts = newNodeContexts();

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> contexts.open(shardRequest(new ShardId(resolveIndex(index), 0)))
        );
        assertThat(e.getMessage(), containsString("no fetch context listener bound reader context"));
        assertAllFreed();
    }

    /**
     * A shard that closes on this node, for example because it relocated, frees its fetch contexts.
     */
    public void testShardCloseFreesItsContexts() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        List<SearchContext> opened = openEveryShard(contexts, INDEX);
        opened.forEach(contexts::originOf);
        contexts.responded();
        endRequest(contexts);
        ShardId closed = opened.getFirst().shardTarget().getShardId();

        listener().afterIndexShardClosed(closed, null, Settings.EMPTY);

        assertBusy(() -> {
            assertThat(searchService().getActiveContexts(), equalTo(SHARDS - 1));
            assertThat(service.openContexts(), equalTo(SHARDS - 1));
        });
        assertThat(contexts.listOpenContributing().stream().map(OpenContextInfo::shardId).toList(), not(hasItem(closed)));
        for (OpenContextInfo context : contexts.listOpenContributing()) {
            searchService().freeReaderContext(context.contextId());
        }
        assertAllFreed();
    }

    /**
     * Deleting the index frees its contexts, and the request stops listing them.
     */
    public void testIndexDeletionClosesTheContexts() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        openEveryShard(contexts, INDEX).forEach(contexts::originOf);
        contexts.responded();
        endRequest(contexts);
        assertBusy(() -> assertThat(service.openContexts(), equalTo(SHARDS)));

        client().admin().indices().prepareDelete(INDEX).get();

        assertAllFreed();
        assertThat(contexts.listOpenContributing(), empty());
    }

    private NodeFetchContexts newNodeContexts() {
        return service.newNodeContexts(TimeValue.timeValueMinutes(5), null);
    }

    private List<SearchContext> openEveryShard(NodeFetchContexts contexts, String index) throws IOException {
        List<SearchContext> opened = new ArrayList<>();
        for (int shard = 0; shard < SHARDS; shard++) {
            SearchContext searchContext = contexts.open(shardRequest(new ShardId(resolveIndex(index), shard)));
            querySearchContexts.add(searchContext);
            opened.add(searchContext);
            assertThat(searchContext.readerContext().getFromContext(FetchContextListener.MARKER_KEY), notNullValue());
        }
        return opened;
    }

    private void createIndexWithDocs(String index) {
        createIndex(index, Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0).build());
        for (int i = 0; i < 20; i++) {
            prepareIndex(index).setSource("field", i).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
    }

    private static ShardSearchRequest shardRequest(ShardId shardId) {
        return new ShardSearchRequest(shardId, System.currentTimeMillis(), AliasFilter.EMPTY, null, SplitShardCountSummary.IRRELEVANT);
    }

    /**
     * Ends the request the way a data node does: the search contexts of the query phase close first, then the fetch
     * contexts.
     */
    private void endRequest(NodeFetchContexts contexts) {
        closeQuerySearchContexts();
        contexts.close();
    }

    /**
     * Waits until no context is registered and no fetch context is open. A context closes when its last user lets go of
     * it, so the query search contexts close first.
     */
    private void assertAllFreed() throws Exception {
        closeQuerySearchContexts();
        assertBusy(() -> {
            assertThat(searchService().getActiveContexts(), equalTo(0));
            assertThat(service.openContexts(), equalTo(0));
        });
    }

    private void updateMaxOpenContexts(Integer max) {
        Settings.Builder settings = Settings.builder();
        if (max == null) {
            settings.putNull(FetchContextService.MAX_OPEN_CONTEXTS.getKey());
        } else {
            settings.put(FetchContextService.MAX_OPEN_CONTEXTS.getKey(), max);
        }
        clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(settings).get();
    }

    private SearchService searchService() {
        return getInstanceFromNode(SearchService.class);
    }

    private FetchContextListener listener() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(FetchContextListenerPlugin.class).findFirst().orElseThrow().listener
            .get();
    }
}
