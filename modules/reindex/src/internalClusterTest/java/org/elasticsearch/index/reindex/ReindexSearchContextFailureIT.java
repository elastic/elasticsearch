/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * Asserts on the errors returned from a reindexing task when the underlying search context is no longer available.
 * {@link SearchContextMissingException} is ordinarily surfaced as a 4xx, but reindex should wrap the failure so the
 * task reports a 5xx.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ReindexSearchContextFailureIT extends ESIntegTestCase {

    private static final AtomicBoolean INJECT = new AtomicBoolean(false);
    /**
     * Counts {@link TransportSearchAction} invocations
     */
    private static final AtomicInteger PIT_USER_SEARCH_INVOCATIONS = new AtomicInteger(0);
    private static final AtomicBoolean INJECTION_APPLIED = new AtomicBoolean(false);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, InjectedSearchContextFailurePlugin.class);
    }

    @Before
    public void resetInjectionState() {
        INJECT.set(false);
        PIT_USER_SEARCH_INVOCATIONS.set(0);
        INJECTION_APPLIED.set(false);
    }

    /**
     * This test covers the <em>point-in-time</em> local reindex path ({@link ReindexPlugin#REINDEX_PIT_SEARCH_FEATURE}).
     * The first page after opening the PIT succeeds. The test then injects a {@link SearchContextMissingException}
     * on the second PIT search, asserting on the returned error
     */
    public void testReindexFailsWith500WhenPitSearchContextMissing() {
        String source = "rsc-fail-source";
        String dest = "rsc-fail-dest";
        int numDocs = randomIntBetween(10, 50);
        assumeTrue("PIT-based reindex path", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        client().admin()
            .indices()
            .prepareCreate(source)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .get();
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, numDocs)
                .mapToObj(i -> prepareIndex(source).setId(Integer.toString(i)).setSource("n", Integer.toString(i)))
                .collect(Collectors.toList())
        );
        assertHitCount(client().prepareSearch(source).setSize(0).setTrackTotalHits(true), numDocs);

        INJECT.set(true);
        ReindexRequestBuilder reindex = new ReindexRequestBuilder(client());
        reindex.source(source).destination(dest);
        reindex.setSlices(1);
        reindex.source().setSize(1);
        // First PIT page succeeds; the second shard query fails with an injected SearchContextMissingException, which
        // reindex wraps for a 5xx response.
        assertFutureThrows(reindex.execute(), ReindexSourceSearchContextLostException.class, RestStatus.INTERNAL_SERVER_ERROR);
        assertTrue("injected search-context failure should have been applied", INJECTION_APPLIED.get());
    }

    private static boolean isPitReindexUserSearchAction(String action) {
        return TransportSearchAction.NAME.equals(action);
    }

    @SuppressWarnings("unchecked")
    private static <Response extends ActionResponse> void applySearchContextMissingFailure(ActionListener<Response> listener) {
        INJECTION_APPLIED.set(true);
        ShardSearchContextId id = new ShardSearchContextId(UUIDs.randomBase64UUID(), 1L, null);
        listener.onFailure(new SearchContextMissingException(id));
    }

    public static class InjectedSearchContextFailurePlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MIN_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (INJECT.get() == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    // Fail on the second PIT search
                    if (isPitReindexUserSearchAction(action) && PIT_USER_SEARCH_INVOCATIONS.incrementAndGet() == 2) {
                        applySearchContextMissingFailure(listener);
                        return;
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }
}
