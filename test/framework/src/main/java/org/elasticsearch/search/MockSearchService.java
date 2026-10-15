/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.PitReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class MockSearchService extends SearchService {
    /**
     * Marker plugin used by {@link MockNode} to enable {@link MockSearchService}.
     */
    public static class TestPlugin extends Plugin {}

    private static final Logger logger = LogManager.getLogger(MockSearchService.class);

    private static final Map<ReaderContext, ActiveContext> ACTIVE_SEARCH_CONTEXTS = new ConcurrentHashMap<>();

    /**
     * A tracked context and when it was registered, so that a leak can report how long it has been held.
     */
    private record ActiveContext(Throwable creationSite, long registeredAtNanos) {}

    private Consumer<ReaderContext> onPutContext = context -> {};
    private Consumer<ReaderContext> onRemoveContext = context -> {};

    private Consumer<SearchContext> onCreateSearchContext = context -> {};

    private Function<CancellableTask, CancellableTask> onCheckCancelled = Function.identity();

    /**
     * Throw an {@link AssertionError} if there are still in-flight contexts.
     * <p>
     * Note: this assertion can spuriously trip when {@code search.low_level_cancellation} is {@code false}
     *     (a setting that is randomized by {@link org.elasticsearch.test.ESIntegTestCase}) and a test stops
     *     the coordinating node while a search is in-flight. With low-level cancellation disabled, the
     *     search is not canceled when the associated connection drops, leaving the reader context active
     *     long enough to fail this check. Tests that deliberately stop coordinators mid-search should
     *     override the setting via their {@code nodeSettings}
     *     (e.g. {@code search.low_level_cancellation: true}) rather than relaxing this assertion, so that
     *     genuine context leaks remain detectable.
     */
    public static void assertNoInFlightContext() {
        final Map<ReaderContext, ActiveContext> copy = new HashMap<>(ACTIVE_SEARCH_CONTEXTS);
        if (copy.isEmpty() == false) {
            final StringBuilder message = new StringBuilder(
                "There are still ["
                    + copy.size()
                    + "] in-flight contexts. The first one's creation site is listed as the cause of this exception."
            );
            copy.forEach((context, active) -> message.append('\n').append(describe(context, active)));
            throw new AssertionError(message.toString(), copy.values().iterator().next().creationSite());
        }
    }

    /**
     * Describes a leaked context, including the node, since this map spans every node of an internal test
     * cluster. Reading a context is best effort: it can be closed while being described, and that must not
     * mask the leak.
     */
    private static String describe(ReaderContext context, ActiveContext active) {
        final TimeValue heldFor = TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - active.registeredAtNanos()));
        final StringBuilder details = new StringBuilder("  ").append(context.getClass().getSimpleName())
            .append(" held for ")
            .append(heldFor);
        try {
            final ShardRouting routing = context.indexShard().routingEntry();
            final long creatorTaskId = context.creatorTaskId();
            details.append(" on shard ")
                .append(context.indexShard().shardId())
                .append(" of node ")
                .append(routing == null ? "unassigned" : routing.currentNodeId())
                .append(", id=")
                .append(context.id())
                .append(", creatorTask=")
                .append(creatorTaskId == 0L ? "unknown" : Long.toString(creatorTaskId))
                .append(", singleSession=")
                .append(context.singleSession())
                .append(", keepAlive=")
                .append(TimeValue.timeValueMillis(context.keepAlive()));
        } catch (Exception e) {
            details.append(" (details unavailable: ").append(e).append(')');
        }
        return details.toString();
    }

    /**
     * Add an active search context to the list of tracked contexts. Package private for testing.
     */
    static void addActiveContext(ReaderContext context) {
        ACTIVE_SEARCH_CONTEXTS.put(
            context,
            new ActiveContext(
                new RuntimeException(String.format(Locale.ROOT, "%s : %s", context.toString(), context.id())),
                System.nanoTime()
            )
        );
    }

    /**
     * Clear an active search context from the list of tracked contexts. Package private for testing.
     */
    static void removeActiveContext(ReaderContext context) {
        ACTIVE_SEARCH_CONTEXTS.remove(context);
    }

    public MockSearchService(
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ScriptService scriptService,
        BigArrays bigArrays,
        FetchPhase fetchPhase,
        CircuitBreakerService circuitBreakerService,
        ExecutorSelector executorSelector,
        Tracer tracer,
        OnlinePrewarmingService onlinePrewarmingService
    ) {
        super(
            clusterService,
            indicesService,
            threadPool,
            scriptService,
            bigArrays,
            fetchPhase,
            circuitBreakerService,
            executorSelector,
            tracer,
            onlinePrewarmingService
        );
    }

    @Override
    protected void putReaderContext(ReaderContext context) {
        onPutContext.accept(context);
        addActiveContext(context);
        super.putReaderContext(context);
    }

    @Override
    protected void putRelocatedReaderContext(Long mappingKey, PitReaderContext context) {
        onPutContext.accept(context);
        addActiveContext(context);
        super.putRelocatedReaderContext(mappingKey, context);
    }

    @Override
    protected ReaderContext removeReaderContext(ShardSearchContextId id) {
        final ReaderContext removed = super.removeReaderContext(id);
        if (removed != null) {
            onRemoveContext.accept(removed);
            removeActiveContext(removed);
        }
        return removed;
    }

    public void setOnPutContext(Consumer<ReaderContext> onPutContext) {
        this.onPutContext = onPutContext;
    }

    public void setOnRemoveContext(Consumer<ReaderContext> onRemoveContext) {
        this.onRemoveContext = onRemoveContext;
    }

    public void setOnCreateSearchContext(Consumer<SearchContext> onCreateSearchContext) {
        this.onCreateSearchContext = onCreateSearchContext;
    }

    @Override
    protected SearchContext createContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        CancellableTask task,
        ResultsType resultsType,
        boolean includeAggregations
    ) throws IOException {
        SearchContext searchContext = super.createContext(readerContext, request, task, resultsType, includeAggregations);
        try {
            onCreateSearchContext.accept(searchContext);
        } catch (Exception e) {
            searchContext.close();
            throw e;
        }
        return searchContext;
    }

    @Override
    public SearchContext createSearchContext(ShardSearchRequest request, TimeValue timeout) throws IOException {
        SearchContext searchContext = super.createSearchContext(request, timeout);
        try {
            onCreateSearchContext.accept(searchContext);
        } catch (Exception e) {
            searchContext.close();
            throw e;
        }
        searchContext.addReleasable(() -> onRemoveContext.accept(searchContext.readerContext()));
        return searchContext;
    }

    public void setOnCheckCancelled(Function<CancellableTask, CancellableTask> onCheckCancelled) {
        this.onCheckCancelled = onCheckCancelled;
    }

    @Override
    protected void checkCancelled(CancellableTask task) {
        super.checkCancelled(onCheckCancelled.apply(task));
    }
}
