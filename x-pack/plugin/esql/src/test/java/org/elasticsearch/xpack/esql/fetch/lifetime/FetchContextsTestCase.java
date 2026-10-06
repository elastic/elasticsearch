/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Fetch contexts on a real {@link SearchService}. {@link MockSearchService} tracks every registered context, and the check
 * after each test fails on any context a test left open.
 */
public abstract class FetchContextsTestCase extends ESSingleNodeTestCase {
    static final String INDEX = "fetch-contexts";
    /**
     * Indices whose name starts with this prefix get no {@link FetchContextListener}.
     */
    static final String UNLISTENED_PREFIX = "unlistened";
    static final int SHARDS = 3;

    /**
     * Registers a {@link FetchContextListener} on every index and the settings of fetch contexts, the way the ES|QL
     * plugin does.
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
            return List.of(FetchContextService.MAX_OPEN_CONTEXTS, FetchContextService.CONTEXT_KEEP_ALIVE);
        }
    }

    /**
     * A request of the fetch phase, which may look a fetch context up.
     */
    static class TestFetchContextRequest extends AbstractTransportRequest implements FetchContextRequest {}

    /**
     * Any other request, like the fetch phase of a search.
     */
    static class OtherRequest extends AbstractTransportRequest {}

    private final List<SearchContext> querySearchContexts = new ArrayList<>();
    protected FetchContextService service;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MockSearchService.TestPlugin.class, FetchContextListenerPlugin.class);
    }

    @Before
    public void createService() {
        TransportService transportService = getInstanceFromNode(TransportService.class);
        service = new FetchContextService(
            searchService(),
            transportService,
            getInstanceFromNode(ClusterService.class).getClusterSettings()
        );
        // the handler only reads the search service, so the service of the first test serves them all
        if (transportService.getRequestHandler(FetchContextService.FREE_ACTION_NAME) == null) {
            service.registerHandlers();
        }
    }

    @After
    public void cleanUpSearchContexts() {
        closeQuerySearchContexts();
        ((MockSearchService) searchService()).setOnCreateSearchContext(context -> {});
        ((MockSearchService) searchService()).setOnPutContext(context -> {});
    }

    /**
     * Closes the search contexts the query phase read through, as a data node does by the time it responds.
     */
    protected void closeQuerySearchContexts() {
        querySearchContexts.forEach(SearchContext::close);
        querySearchContexts.clear();
    }

    /**
     * Ends the request the way a data node does: the search contexts of the query phase close first, then the fetch
     * contexts.
     */
    protected void endRequest(NodeFetchContexts contexts) {
        closeQuerySearchContexts();
        contexts.close();
    }

    protected NodeFetchContexts newNodeContexts() {
        return service.newNodeContexts(TimeValue.timeValueMinutes(5), null);
    }

    /**
     * Opens a fetch context for {@code shard} of {@code index}. The test closes its query search context.
     */
    protected SearchContext open(NodeFetchContexts contexts, String index, int shard) throws IOException {
        SearchContext searchContext = contexts.open(shardRequest(new ShardId(resolveIndex(index), shard)));
        querySearchContexts.add(searchContext);
        return searchContext;
    }

    protected List<SearchContext> openEveryShard(NodeFetchContexts contexts, String index) throws IOException {
        List<SearchContext> opened = new ArrayList<>();
        for (int shard = 0; shard < SHARDS; shard++) {
            SearchContext searchContext = open(contexts, index, shard);
            assertThat(searchContext.readerContext().getFromContext(FetchContextListener.MARKER_KEY), notNullValue());
            opened.add(searchContext);
        }
        return opened;
    }

    /**
     * What a data node does for a request whose every shard has rows that survive the node cut: opens a context per shard,
     * responds with all of them, and lets go of them.
     */
    protected List<OpenContextInfo> respond(String index) throws IOException {
        NodeFetchContexts contexts = newNodeContexts();
        openEveryShard(contexts, index).forEach(contexts::originOf);
        List<OpenContextInfo> open = contexts.listOpenContributing();
        contexts.responded();
        endRequest(contexts);
        return open;
    }

    protected void createIndexWithDocs(String index) {
        createIndex(index, Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0).build());
        for (int i = 0; i < 20; i++) {
            prepareIndex(index).setSource("field", i).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
    }

    protected static ShardSearchRequest shardRequest(ShardId shardId) {
        return new ShardSearchRequest(shardId, System.currentTimeMillis(), AliasFilter.EMPTY, null, SplitShardCountSummary.IRRELEVANT);
    }

    protected static OriginalIndices originalIndices(String index) {
        return new OriginalIndices(new String[] { index }, IndicesOptions.strictExpandOpen());
    }

    protected DiscoveryNode localNode() {
        return getInstanceFromNode(ClusterService.class).localNode();
    }

    /**
     * Waits until no context is registered and no fetch context is open. A context closes when its last user lets go of
     * it, so the query search contexts close first.
     */
    protected void assertAllFreed() throws Exception {
        closeQuerySearchContexts();
        assertBusy(() -> {
            assertThat(searchService().getActiveContexts(), equalTo(0));
            assertThat(service.openContexts(), equalTo(0));
        });
    }

    /**
     * Runs {@code body} in a thread context authenticated as {@code authentication}.
     */
    protected <T> T as(Authentication authentication, CheckedSupplier<T, Exception> body) throws Exception {
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        try (var ignored = threadContext.stashContext()) {
            authentication.writeToContext(threadContext);
            return body.get();
        }
    }

    protected void updateSetting(Setting<?> setting, Object value) {
        Settings.Builder settings = Settings.builder();
        if (value == null) {
            settings.putNull(setting.getKey());
        } else {
            settings.put(setting.getKey(), value.toString());
        }
        clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(settings).get();
    }

    protected SearchService searchService() {
        return getInstanceFromNode(SearchService.class);
    }

    protected FetchContextListener listener() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(FetchContextListenerPlugin.class).findFirst().orElseThrow().listener
            .get();
    }
}
