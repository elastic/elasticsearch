/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The fetch contexts of one data node request.
 */
public class NodeFetchContextsTests extends FetchContextsTestCase {
    public void testOpensRegisteredContexts() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = newNodeContexts();
        List<SearchContext> opened = openEveryShard(contexts, INDEX);

        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));
        assertThat(service.openContexts(), equalTo(SHARDS));
        for (SearchContext searchContext : opened) {
            ReaderContext readerContext = searchContext.readerContext();
            assertThat(
                searchService().findReaderContext(readerContext.id(), new TestFetchContextRequest(), null),
                sameInstance(readerContext)
            );
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
        // a freed context leaves the registry first and closes when its last reference goes, so both are awaited
        assertBusy(() -> {
            assertThat(searchService().getActiveContexts(), equalTo(contributing.size()));
            assertThat(service.openContexts(), equalTo(contributing.size()));
        });

        // the coordinator owns the rest now, and frees them at the end of the query
        for (OpenContextInfo context : open) {
            assertTrue(searchService().freeReaderContext(context.contextId()));
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
        Authentication owner = AuthenticationTestHelper.builder().user(new User("owner")).build();
        Authentication other = AuthenticationTestHelper.builder().user(new User("other")).build();
        NodeFetchContexts contexts = newNodeContexts();
        ReaderContext readerContext = as(owner, () -> open(contexts, INDEX, 0).readerContext());
        assertThat(readerContext.getFromContext(AuthenticationField.AUTHENTICATION_KEY), equalTo(owner));

        as(owner, () -> {
            assertThat(
                searchService().findReaderContext(readerContext.id(), new TestFetchContextRequest(), null),
                sameInstance(readerContext)
            );
            expectThrows(
                SearchContextMissingException.class,
                () -> searchService().findReaderContext(readerContext.id(), new OtherRequest(), null)
            );
            return null;
        });
        as(other, () -> {
            expectThrows(
                SearchContextMissingException.class,
                () -> searchService().findReaderContext(readerContext.id(), new TestFetchContextRequest(), null)
            );
            return null;
        });
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
        randomSubsetOf(openEveryShard(contexts, INDEX)).forEach(contexts::originOf);

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

        expectThrows(TaskCancelledException.class, () -> open(contexts, INDEX, 0));
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

        expectThrows(TaskCancelledException.class, () -> open(contexts, INDEX, 0));
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

        expectThrows(IndexNotFoundException.class, () -> open(contexts, INDEX, 0));
        endRequest(contexts);
        assertAllFreed();
    }

    public void testRejectsContextsBeyondTheLimit() throws Exception {
        createIndexWithDocs(INDEX);
        updateSetting(FetchContextService.MAX_OPEN_CONTEXTS, 1);
        try {
            NodeFetchContexts contexts = newNodeContexts();
            open(contexts, INDEX, 0);

            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> open(contexts, INDEX, 1));
            assertThat(e.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(e.getMessage(), containsString("[esql.fetch.max_open_contexts]"));
            assertThat(service.openContexts(), equalTo(1));

            contexts.freeAll("the test is done");
            endRequest(contexts);
            assertAllFreed();
        } finally {
            updateSetting(FetchContextService.MAX_OPEN_CONTEXTS, null);
        }
    }

    /**
     * Each data node checks the keep-alive the coordinator sends against its own limit, and names both settings.
     */
    public void testRejectsAKeepAliveAboveTheLimitOfTheNode() throws Exception {
        createIndexWithDocs(INDEX);
        NodeFetchContexts contexts = service.newNodeContexts(TimeValue.timeValueHours(25), null);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> open(contexts, INDEX, 0));
        assertThat(e.getMessage(), containsString("[esql.fetch.context_keep_alive]"));
        assertThat(e.getMessage(), containsString("[search.max_keep_alive] allows at most [1d]"));
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
     * Without the listener nobody binds the owner, so anyone who guesses the id could use the context. The open fails
     * and frees it.
     */
    public void testIndexWithoutTheListenerFails() throws Exception {
        String index = UNLISTENED_PREFIX + "-index";
        createIndexWithDocs(index);
        NodeFetchContexts contexts = newNodeContexts();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> open(contexts, index, 0));
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
        ShardId closed = opened.getFirst().shardTarget().getShardId();
        contexts.responded();
        endRequest(contexts);

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
     * Deleting the index frees its contexts.
     */
    public void testIndexDeletionFreesTheContexts() throws Exception {
        createIndexWithDocs(INDEX);
        respond(INDEX);
        assertBusy(() -> assertThat(service.openContexts(), equalTo(SHARDS)));

        client().admin().indices().prepareDelete(INDEX).get();

        assertAllFreed();
    }
}
