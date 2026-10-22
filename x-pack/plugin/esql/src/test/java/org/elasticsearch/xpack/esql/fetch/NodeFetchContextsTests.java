/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The fetch contexts of one data node request.
 */
public class NodeFetchContextsTests extends FetchContextsTestCase {
    /**
     * Each shard gets a single-session context of the search service, with the keep-alive of {@code _search}, which a
     * request of the fetch phase finds again by id.
     */
    public void testOpensSingleSessionContexts() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        List<SearchContext> opened = openEveryShard(contexts, INDEX);

        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));
        for (SearchContext searchContext : opened) {
            ReaderContext readerContext = searchContext.readerContext();
            assertTrue(readerContext.singleSession());
            assertThat(readerContext.keepAlive(), equalTo(searchService().getDefaultKeepAliveInMillis()));
            assertThat(searchService().findReaderContext(readerContext.id(), new LookupRequest(), null), sameInstance(readerContext));
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

        List<ShardSearchContextId> open = contexts.listOpenContributing();
        assertThat(open, containsInAnyOrder(contributing.stream().map(c -> c.readerContext().id()).toArray()));

        contexts.responded();
        endRequest(contexts);
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(contributing.size())));

        // the coordinator owns the rest now, and frees them at the end of the query
        for (ShardSearchContextId id : open) {
            assertTrue(searchService().freeReaderContext(id));
        }
        assertAllFreed();
    }

    /**
     * A cancellation that reaches the request after its response frees the contributing contexts too, because the query
     * is over.
     */
    public void testFreeAllAfterTheResponseFreesEveryContext() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        openEveryShard(contexts, INDEX).forEach(contexts::originOf);
        contexts.responded();

        contexts.freeAll("the request was cancelled");
        endRequest(contexts);

        assertAllFreed();
    }

    /**
     * The origin names the registered context, so the fetch phase finds it again on this node.
     */
    public void testOriginNamesTheRegisteredContext() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        SearchContext searchContext = open(contexts, INDEX, 0);

        DocOrigin origin = contexts.originOf(searchContext);

        assertThat(origin.clusterAlias(), equalTo(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));
        assertThat(origin.nodeId(), equalTo(getInstanceFromNode(ClusterService.class).localNode().getId()));
        assertThat(origin.shardId(), equalTo(new ShardId(resolveIndex(INDEX), 0)));
        assertThat(origin.contextId(), equalTo(searchContext.readerContext().id()));
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
        AtomicInteger closedReaders = new AtomicInteger();
        opened.forEach(searchContext -> searchContext.readerContext().addOnClose(closedReaders::incrementAndGet));

        contexts.freeAll("the request failed");
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(0)));
        closeQuerySearchContexts();
        assertThat("the request still holds every reader", closedReaders.get(), equalTo(0));

        contexts.close();
        assertBusy(() -> assertThat(closedReaders.get(), equalTo(SHARDS)));
        assertAllFreed();
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

        expectThrows(TaskCancelledException.class, () -> open(contexts, INDEX, 0));
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Cancellation can also come while a context opens, after it registered. That open fails and leaves nothing.
     */
    public void testFreeAllDuringOpenLeavesNothing() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        ((MockSearchService) searchService()).setOnCreateSearchContext(context -> contexts.freeAll("the request was cancelled"));

        expectThrows(TaskCancelledException.class, () -> open(contexts, INDEX, 0));
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * Removing the index can race with the open while the context registers. The open fails and the context closes.
     */
    public void testIndexRemovalDuringOpenLeavesNothing() throws Exception {
        createIndexWithDocs(INDEX);
        Index index = resolveIndex(INDEX);
        ((MockSearchService) searchService()).setOnPutContext(context -> { throw new IndexNotFoundException(index); });
        NodeFetchContexts contexts = newNodeContexts();

        expectThrows(IndexNotFoundException.class, () -> open(contexts, INDEX, 0));
        endRequest(contexts);
        assertAllFreed();
    }

    /**
     * A context whose query phase can't start is freed at once.
     */
    public void testFailedSearchContextLeavesNothing() throws Exception {
        createIndexWithDocs(INDEX);
        ((MockSearchService) searchService()).setOnCreateSearchContext(
            context -> { throw new IllegalStateException("simulated failure"); }
        );
        NodeFetchContexts contexts = newNodeContexts();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> open(contexts, INDEX, 0));
        assertThat(e.getMessage(), equalTo("simulated failure"));
        assertAllFreed();
    }

    /**
     * Deleting the index frees its contexts.
     */
    public void testIndexDeletionFreesTheContexts() throws Exception {
        createIndexWithDocs(INDEX);
        respond(INDEX);
        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));

        client().admin().indices().prepareDelete(INDEX).get();

        assertAllFreed();
    }
}
