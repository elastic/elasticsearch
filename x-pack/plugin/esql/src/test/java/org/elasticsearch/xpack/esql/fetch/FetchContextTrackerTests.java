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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * The coordinator's tracker of the fetch contexts of one execution, and the requests that free them. The contexts are real
 * contexts of the {@link SearchService} of the node, and the tracker frees them with the real free action of
 * {@code _search}. {@link MockSearchService} tracks every registered context, and the check after each test fails on any
 * context a test left open. {@link MockTransportService} only watches the free requests.
 */
public class FetchContextTrackerTests extends ESSingleNodeTestCase {
    private static final String INDEX = "fetch-contexts";
    private static final int SHARDS = 3;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MockSearchService.TestPlugin.class, MockTransportService.TestPlugin.class);
    }

    /**
     * The listeners that {@link #countFreeRequests} added, which outlive the rules of the transport service.
     */
    private final List<TransportMessageListener> freeCounters = new ArrayList<>();

    @After
    public void clearTransportRules() {
        MockTransportService transportService = (MockTransportService) getInstanceFromNode(TransportService.class);
        transportService.clearAllRules();
        freeCounters.forEach(transportService::removeMessageListener);
        freeCounters.clear();
    }

    /**
     * Once the fetch ended, or the execution failed or was cancelled, the tracker frees every context left, each with the
     * free action of {@code _search}, like {@code _search} frees the contexts of its query phase.
     */
    public void testCloseFreesEveryContext() throws Exception {
        createIndexWithDocs();
        FetchContextTracker tracker = tracker();
        track(tracker, keepEveryShard());
        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));
        AtomicInteger freeRequests = countFreeRequests();

        tracker.close();

        assertAllFreed();
        assertThat(freeRequests.get(), equalTo(SHARDS));
    }

    /**
     * A data node keeps every context that the origins of a page that left it name, so the tracker frees them too, also
     * when no row of the page reads them.
     */
    public void testTracksTheContextsNoRowOfAPageReads() throws Exception {
        createIndexWithDocs();
        List<DocOrigin> kept = keepEveryShard();
        FetchContextTracker tracker = tracker();
        Page page = pageReadingOnly(kept, kept.getFirst());
        try {
            tracker.track(page);
        } finally {
            page.releaseBlocks();
        }

        tracker.close();

        assertAllFreed();
    }

    /**
     * An administrator can cancel the query of another user. The tracker still frees its contexts as the user who ran the
     * query, who opened them.
     */
    public void testAFreeRunsAsTheUserWhoRanTheQuery() throws Exception {
        createIndexWithDocs();
        List<String> authentications = captureFreeAuthentications();
        Authentication owner = AuthenticationTestHelper.builder().user(new User("owner")).build();
        Authentication administrator = AuthenticationTestHelper.builder().user(new User("administrator")).build();
        FetchContextTracker tracker = as(owner, this::tracker);
        track(tracker, keepEveryShard());

        as(administrator, () -> {
            tracker.close();
            return null;
        });

        assertAllFreed();
        assertBusy(() -> assertThat(authentications, hasSize(SHARDS)));
        assertThat(authentications, everyItem(equalTo(owner.encode())));
    }

    /**
     * Pages that arrive after the execution ended still name kept contexts. Nobody else would free them before the reaper
     * does.
     */
    public void testContextsThatArriveAfterCloseAreFreedAtOnce() throws Exception {
        createIndexWithDocs();
        FetchContextTracker tracker = tracker();
        tracker.close();

        track(tracker, keepEveryShard());

        assertAllFreed();
    }

    /**
     * Once the cut ended, the tracker frees the contexts its rows don't read, and keeps the others for the fetch.
     */
    public void testTheEndOfTheCutFreesTheContextsItsRowsDontRead() throws Exception {
        createIndexWithDocs();
        List<DocOrigin> kept = keepEveryShard();
        FetchContextTracker tracker = tracker();
        track(tracker, kept);

        tracker.cutEnded(Set.of(kept.getFirst().contextId()));

        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(1)));
        assertTrue("the rows of the cut read it", isOpen(kept.getFirst().contextId()));
        tracker.close();
        assertAllFreed();
    }

    /**
     * Pages that arrive after the cut ended have their contexts freed at once, unless the rows of the cut read them.
     */
    public void testContextsThatArriveAfterTheCutAreFreedUnlessTheCutReadsThem() throws Exception {
        createIndexWithDocs();
        List<DocOrigin> kept = keepEveryShard();
        FetchContextTracker tracker = tracker();
        tracker.cutEnded(Set.of(kept.getFirst().contextId()));

        track(tracker, kept);

        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(1)));
        assertTrue("the rows of the cut read it", isOpen(kept.getFirst().contextId()));
        tracker.close();
        assertAllFreed();
    }

    /**
     * Many pages arrive through the same context, also after the tracker freed it. One free is enough.
     */
    public void testFreesAContextOnce() throws Exception {
        createIndexWithDocs();
        List<DocOrigin> kept = keepEveryShard();
        FetchContextTracker tracker = tracker();
        AtomicInteger freeRequests = countFreeRequests();
        int pagesBefore = between(1, 4);
        for (int p = 0; p < pagesBefore; p++) {
            track(tracker, kept);
        }

        tracker.close();
        int pagesAfter = between(1, 4);
        for (int p = 0; p < pagesAfter; p++) {
            track(tracker, kept);
        }

        assertAllFreed();
        assertThat(freeRequests.get(), equalTo(SHARDS));
    }

    /**
     * A node frees a context once it answered the fetch that read it for the last time. The tracker doesn't free it again.
     */
    public void testDoesNotFreeAContextItsNodeFreed() throws Exception {
        createIndexWithDocs();
        List<DocOrigin> kept = keepEveryShard();
        FetchContextTracker tracker = tracker();
        track(tracker, kept);
        AtomicInteger freeRequests = countFreeRequests();

        tracker.freedByTheirNode(List.of(kept.getFirst().contextId()));
        tracker.close();

        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(1)));
        assertThat(freeRequests.get(), equalTo(SHARDS - 1));
        // the node of these tests didn't free it, the test does
        searchService().freeReaderContext(kept.getFirst().contextId());
        assertAllFreed();
    }

    /**
     * A node that left the cluster took its contexts with it, so freeing them sends nothing, and fails nothing.
     */
    public void testANodeThatLeftLeavesItsContextsAlone() throws Exception {
        createIndexWithDocs();
        List<DocOrigin> kept = keepEveryShard();
        DocOrigin first = kept.getFirst();
        DocOrigin ofANodeThatLeft = new DocOrigin(first.clusterAlias(), "gone", first.shardId(), first.contextId());
        FetchContextTracker tracker = tracker();
        track(tracker, List.of(ofANodeThatLeft));
        AtomicInteger freeRequests = countFreeRequests();

        tracker.close();

        assertThat(freeRequests.get(), equalTo(0));
        free(kept);
    }

    /**
     * The fetch phase reads the local cluster only, so the tracker frees no context of a remote cluster, even of a node with
     * the id of a local one.
     */
    public void testFreesNoContextOfARemoteCluster() throws Exception {
        createIndexWithDocs();
        List<DocOrigin> kept = keepEveryShard();
        DocOrigin first = kept.getFirst();
        DocOrigin remote = new DocOrigin("remote", first.nodeId(), first.shardId(), first.contextId());
        FetchContextTracker tracker = tracker();
        track(tracker, List.of(remote));
        AtomicInteger freeRequests = countFreeRequests();

        tracker.close();

        assertThat(freeRequests.get(), equalTo(0));
        assertTrue(isOpen(first.contextId()));
        free(kept);
    }

    private FetchContextTracker tracker() {
        return new FetchContextTracker(getInstanceFromNode(SearchTransportService.class), getInstanceFromNode(ClusterService.class));
    }

    /**
     * Pages whose rows read each of {@code origins} reached the coordinator.
     */
    private static void track(FetchContextTracker tracker, List<DocOrigin> origins) {
        Page page = pageReadingOnly(origins, origins.toArray(DocOrigin[]::new));
        try {
            tracker.track(page);
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * A page whose documents name {@code origins}, and whose rows read only {@code read}.
     */
    private static Page pageReadingOnly(List<DocOrigin> origins, DocOrigin... read) {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, read.length)) {
            for (int r = 0; r < read.length; r++) {
                builder.appendShard(origins.indexOf(read[r])).appendSegment(0).appendDoc(r);
            }
            return new Page(builder.build(DocVector.config().origins(DocOrigins.of(origins))));
        }
    }

    /**
     * Keeps a context of each shard, the way a data node request whose rows all left the node does.
     */
    private List<DocOrigin> keepEveryShard() throws IOException {
        NodeFetchContexts contexts = new NodeFetchContexts(
            searchService(),
            null,
            TimeValue.timeValueMillis(searchService().getDefaultKeepAliveInMillis())
        );
        List<DocOrigin> kept = new ArrayList<>();
        try {
            for (int shard = 0; shard < SHARDS; shard++) {
                ShardSearchRequest shardRequest = new ShardSearchRequest(
                    new ShardId(resolveIndex(INDEX), shard),
                    System.currentTimeMillis(),
                    AliasFilter.EMPTY,
                    null,
                    SplitShardCountSummary.IRRELEVANT
                );
                // a data node closes the search context of the query phase before the request ends
                try (SearchContext searchContext = contexts.open(shardRequest)) {
                    kept.add(contexts.originOf(searchContext));
                }
            }
        } finally {
            contexts.close();
        }
        return kept;
    }

    private void createIndexWithDocs() {
        createIndex(INDEX, Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0).build());
        for (int i = 0; i < 20; i++) {
            prepareIndex(INDEX).setSource("field", i).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
    }

    /**
     * Whether the context {@code id} is still registered.
     */
    private boolean isOpen(ShardSearchContextId id) {
        try {
            searchService().findReaderContext(id, new LookupRequest(), null);
            return true;
        } catch (SearchContextMissingException e) {
            return false;
        }
    }

    /**
     * Frees the contexts the tracker left open on purpose, then waits until none is registered.
     */
    private void free(List<DocOrigin> kept) throws Exception {
        kept.forEach(origin -> searchService().freeReaderContext(origin.contextId()));
        assertAllFreed();
    }

    /**
     * Waits until no context is registered. A data node drops its references on the generic pool.
     */
    private void assertAllFreed() throws Exception {
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(0)));
    }

    /**
     * Runs {@code body} in a thread context authenticated as {@code authentication}.
     */
    private <T> T as(Authentication authentication, CheckedSupplier<T, Exception> body) throws Exception {
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            authentication.writeToContext(threadContext);
            return body.get();
        }
    }

    private SearchService searchService() {
        return getInstanceFromNode(SearchService.class);
    }

    /**
     * Any request that looks a context up, like a fetch request.
     */
    private static class LookupRequest extends AbstractTransportRequest {}

    /**
     * The users that the free requests this node receives from now on come as. It serves them as usual.
     *
     * @return the encoded authentication header of each request, {@code null} for one without it
     */
    private List<String> captureFreeAuthentications() {
        List<String> received = new CopyOnWriteArrayList<>();
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        ((MockTransportService) getInstanceFromNode(TransportService.class)).<TransportRequest>addRequestHandlingBehavior(
            SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME,
            (handler, request, channel, task) -> {
                received.add(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY));
                handler.messageReceived(request, channel, task);
            }
        );
        return received;
    }

    /**
     * Counts the free requests that this node sends from now on. The tracker sends them on the thread that frees, and the
     * count goes up there, before the node handles the request on another thread. So once a call to the tracker returned,
     * the count holds every free it sent, and a count of zero proves it sent none.
     */
    private AtomicInteger countFreeRequests() {
        AtomicInteger freeRequests = new AtomicInteger();
        TransportMessageListener counter = new TransportMessageListener() {
            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions finalOptions,
                int networkMessageSize
            ) {
                if (SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME.equals(action)) {
                    freeRequests.incrementAndGet();
                }
            }
        };
        ((MockTransportService) getInstanceFromNode(TransportService.class)).addMessageListener(counter);
        freeCounters.add(counter);
        return freeRequests;
    }
}
