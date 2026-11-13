/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The fetch contexts of one data node request, on a real {@link SearchService}. {@link MockSearchService} tracks every
 * registered context, and the check after each test fails on any context a test left open.
 */
public class NodeFetchContextsTests extends ESSingleNodeTestCase {
    private static final String INDEX = "fetch-contexts";
    private static final int SHARDS = 3;

    private final List<SearchContext> querySearchContexts = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MockSearchService.TestPlugin.class);
    }

    @After
    public void cleanUpSearchContexts() {
        closeQuerySearchContexts();
        searchService().setOnCreateSearchContext(context -> {});
        searchService().setOnPutContext(context -> {});
    }

    /**
     * Each shard gets a single-session context of the search service, with the default keep-alive of {@code _search},
     * which a request of the fetch phase finds again by id.
     */
    public void testOpensSingleSessionContexts() throws Exception {
        createIndexWithDocs();
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());
        List<SearchContext> opened = openEveryShard(contexts);

        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));
        for (SearchContext searchContext : opened) {
            ReaderContext readerContext = searchContext.readerContext();
            assertTrue(readerContext.singleSession());
            assertThat(readerContext.keepAlive(), equalTo(searchService().getDefaultKeepAliveInMillis()));
            assertThat(searchService().findReaderContext(readerContext.id(), new LookupRequest(), null), sameInstance(readerContext));
        }

        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Reader contexts keep their keep-alive in milliseconds, so a shorter one would keep a context for no time at all.
     */
    public void testNeedsAKeepAliveOfAtLeastAMillisecond() {
        TimeValue keepAlive = randomFrom(TimeValue.ZERO, TimeValue.MINUS_ONE, TimeValue.timeValueNanos(between(1, 999_999)));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newNodeContexts(keepAlive));
        assertThat(e.getMessage(), equalTo("the keep-alive must be at least a millisecond but was [" + keepAlive + "]"));
    }

    /**
     * Only the contexts whose rows survived the node cut go to the coordinator. The request frees the others when it ends.
     */
    public void testHandsOffTheContributingContexts() throws Exception {
        createIndexWithDocs();
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());
        List<SearchContext> opened = openEveryShard(contexts);
        List<SearchContext> contributing = randomSubsetOf(between(1, SHARDS - 1), opened);
        contributing.forEach(contexts::originOf);

        List<ShardSearchContextId> handedOff = contexts.handOffContributing();

        assertThat(handedOff, containsInAnyOrder(contributing.stream().map(c -> c.readerContext().id()).toArray()));
        endRequest(contexts);
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(contributing.size())));
        freeAll(handedOff);
        assertAllFreed();
    }

    /**
     * A context the coordinator takes over gets the keep-alive of the request, but never less than the default keep-alive
     * it opened with.
     */
    public void testHandOffGivesTheKeepAliveOfTheRequest() throws Exception {
        createIndexWithDocs();
        long nodeDefault = searchService().getDefaultKeepAliveInMillis();
        long requested = randomBoolean() ? nodeDefault + between(1, 3600) * 1000L : between(1, (int) nodeDefault);
        NodeFetchContexts contexts = newNodeContexts(TimeValue.timeValueMillis(requested));
        List<SearchContext> opened = openEveryShard(contexts);
        opened.forEach(contexts::originOf);

        List<ShardSearchContextId> handedOff = contexts.handOffContributing();

        for (SearchContext searchContext : opened) {
            assertThat(searchContext.readerContext().keepAlive(), equalTo(Math.max(nodeDefault, requested)));
        }
        endRequest(contexts);
        freeAll(handedOff);
        assertAllFreed();
    }

    /**
     * The request lets go of the contexts it handed off, so their readers close as soon as the coordinator frees them.
     */
    public void testHandOffDropsTheReferenceOfTheRequest() throws Exception {
        createIndexWithDocs();
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());
        List<SearchContext> opened = openEveryShard(contexts);
        opened.forEach(contexts::originOf);
        AtomicInteger closedReaders = countClosedReaders(opened);

        List<ShardSearchContextId> handedOff = contexts.handOffContributing();
        closeQuerySearchContexts();
        freeAll(handedOff);

        assertBusy(() -> assertThat(closedReaders.get(), equalTo(SHARDS)));
        contexts.close();
        assertAllFreed();
    }

    /**
     * The request holds a reference on each context until it ends, so none closes under its rows, also when something
     * else frees it, like the removal of its index.
     */
    public void testTheRequestHoldsItsContextsUntilItEnds() throws Exception {
        createIndexWithDocs();
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());
        List<SearchContext> opened = openEveryShard(contexts);
        AtomicInteger closedReaders = countClosedReaders(opened);
        closeQuerySearchContexts();

        freeAll(opened.stream().map(c -> c.readerContext().id()).toList());
        assertThat("the request still holds every reader", closedReaders.get(), equalTo(0));

        contexts.close();
        assertBusy(() -> assertThat(closedReaders.get(), equalTo(SHARDS)));
        assertAllFreed();
    }

    /**
     * A request that fails or is cancelled never hands its contexts off, so it frees every one of them when it ends, also
     * the contributing ones.
     */
    public void testEndWithoutHandOffFreesEveryContext() throws Exception {
        createIndexWithDocs();
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());
        List<SearchContext> opened = openEveryShard(contexts);
        randomSubsetOf(opened).forEach(contexts::originOf);
        AtomicInteger closedReaders = countClosedReaders(opened);

        endRequest(contexts);

        assertBusy(() -> assertThat(closedReaders.get(), equalTo(SHARDS)));
        assertAllFreed();
    }

    /**
     * The origin names the registered context, so the fetch phase finds it again on this node.
     */
    public void testOriginNamesTheRegisteredContext() throws Exception {
        createIndexWithDocs();
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());
        SearchContext searchContext = open(contexts, 0);

        DocOrigin origin = contexts.originOf(searchContext);

        assertThat(origin.clusterAlias(), equalTo(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));
        assertThat(origin.nodeId(), equalTo(getInstanceFromNode(ClusterService.class).localNode().getId()));
        assertThat(origin.shardId(), equalTo(new ShardId(resolveIndex(INDEX), 0)));
        assertThat(origin.contextId(), equalTo(searchContext.readerContext().id()));
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Nobody would free a context that opens after the request ended, so it fails and leaves nothing behind.
     */
    public void testOpenAfterCloseFails() throws Exception {
        createIndexWithDocs();
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());
        contexts.close();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> open(contexts, 0));
        assertThat(e.getMessage(), equalTo("opened a fetch context after the request ended"));
        assertAllFreed();
    }

    /**
     * Removing the index can race with the open while the context registers. The open fails and the context closes.
     */
    public void testIndexRemovalDuringOpenLeavesNothing() throws Exception {
        createIndexWithDocs();
        Index index = resolveIndex(INDEX);
        searchService().setOnPutContext(context -> { throw new IndexNotFoundException(index); });
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());

        expectThrows(IndexNotFoundException.class, () -> open(contexts, 0));
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * A context whose query phase can't start is freed at once.
     */
    public void testFailedSearchContextLeavesNothing() throws Exception {
        createIndexWithDocs();
        searchService().setOnCreateSearchContext(context -> { throw new IllegalStateException("simulated failure"); });
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> open(contexts, 0));
        assertThat(e.getMessage(), equalTo("simulated failure"));
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Deleting the index frees the contexts the coordinator took over.
     */
    public void testIndexDeletionFreesTheHandedOffContexts() throws Exception {
        createIndexWithDocs();
        NodeFetchContexts contexts = newNodeContexts(randomKeepAlive());
        openEveryShard(contexts).forEach(contexts::originOf);
        contexts.handOffContributing();
        endRequest(contexts);
        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));

        client().admin().indices().prepareDelete(INDEX).get();

        assertAllFreed();
    }

    /**
     * Any request that looks a context up, like a fetch request.
     */
    private static class LookupRequest extends AbstractTransportRequest {}

    private NodeFetchContexts newNodeContexts(TimeValue keepAlive) {
        return new NodeFetchContexts(searchService(), getInstanceFromNode(ThreadPool.class).generic(), null, keepAlive);
    }

    private static TimeValue randomKeepAlive() {
        return TimeValue.timeValueMinutes(between(1, 10));
    }

    /**
     * Opens a fetch context for {@code shard}. The test closes its query search context.
     */
    private SearchContext open(NodeFetchContexts contexts, int shard) throws IOException {
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            new ShardId(resolveIndex(INDEX), shard),
            System.currentTimeMillis(),
            AliasFilter.EMPTY,
            null,
            SplitShardCountSummary.IRRELEVANT
        );
        SearchContext searchContext = contexts.open(shardRequest);
        querySearchContexts.add(searchContext);
        return searchContext;
    }

    private List<SearchContext> openEveryShard(NodeFetchContexts contexts) throws IOException {
        List<SearchContext> opened = new ArrayList<>();
        for (int shard = 0; shard < SHARDS; shard++) {
            opened.add(open(contexts, shard));
        }
        return opened;
    }

    /**
     * Ends the request the way a data node does: the search contexts of the query phase close first, then the fetch
     * contexts.
     */
    private void endRequest(NodeFetchContexts contexts) {
        closeQuerySearchContexts();
        contexts.close();
    }

    private void closeQuerySearchContexts() {
        querySearchContexts.forEach(SearchContext::close);
        querySearchContexts.clear();
    }

    /**
     * Frees {@code ids} the way the coordinator does at the end of the query, or the way index removal does.
     */
    private void freeAll(List<ShardSearchContextId> ids) {
        for (ShardSearchContextId id : ids) {
            assertTrue("[" + id + "] is open", searchService().freeReaderContext(id));
            expectThrows(SearchContextMissingException.class, () -> searchService().findReaderContext(id, new LookupRequest(), null));
        }
    }

    private static AtomicInteger countClosedReaders(List<SearchContext> searchContexts) {
        AtomicInteger closedReaders = new AtomicInteger();
        searchContexts.forEach(searchContext -> searchContext.readerContext().addOnClose(closedReaders::incrementAndGet));
        return closedReaders;
    }

    /**
     * Waits until no context is registered. The request drops its references on the generic pool.
     */
    private void assertAllFreed() throws Exception {
        closeQuerySearchContexts();
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(0)));
    }

    private void createIndexWithDocs() {
        createIndex(INDEX, Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0).build());
        for (int i = 0; i < 20; i++) {
            prepareIndex(INDEX).setSource("field", i).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
    }

    private MockSearchService searchService() {
        return (MockSearchService) getInstanceFromNode(SearchService.class);
    }
}
