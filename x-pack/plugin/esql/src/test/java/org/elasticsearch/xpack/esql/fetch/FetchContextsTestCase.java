/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Fetch contexts on a real {@link SearchService}. {@link MockSearchService} tracks every registered context, and the check
 * after each test fails on any context a test left open.
 */
public abstract class FetchContextsTestCase extends ESSingleNodeTestCase {
    static final String INDEX = "fetch-contexts";
    protected static final int SHARDS = 3;

    /**
     * Any request that looks a context up, like a fetch request.
     */
    static class LookupRequest extends AbstractTransportRequest {}

    private final List<SearchContext> querySearchContexts = new ArrayList<>();
    protected FetchContextService service;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MockSearchService.TestPlugin.class);
    }

    @Before
    public void createService() {
        service = new FetchContextService(searchService(), getInstanceFromNode(SearchTransportService.class));
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
        return service.newNodeContexts(null);
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
            opened.add(open(contexts, index, shard));
        }
        return opened;
    }

    /**
     * What a data node does for a request whose every shard has rows that survive the node cut: opens a context per shard,
     * responds with all of them, and lets go of them.
     */
    protected List<ShardSearchContextId> respond(String index) throws IOException {
        NodeFetchContexts contexts = newNodeContexts();
        openEveryShard(contexts, index).forEach(contexts::originOf);
        List<ShardSearchContextId> open = contexts.listOpenContributing();
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

    protected DiscoveryNode localNode() {
        return getInstanceFromNode(ClusterService.class).localNode();
    }

    /**
     * Waits until no context is registered. A context closes when its last user lets go of it, so the query search contexts
     * close first.
     */
    protected void assertAllFreed() throws Exception {
        closeQuerySearchContexts();
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(0)));
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

    protected SearchService searchService() {
        return getInstanceFromNode(SearchService.class);
    }
}
