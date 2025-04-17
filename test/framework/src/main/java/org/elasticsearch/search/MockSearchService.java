/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.search.OnlinePrewarmingService;
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
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class MockSearchService extends SearchService {
    /**
     * Marker plugin used by {@link MockNode} to enable {@link MockSearchService}.
     */
    public static class TestPlugin extends Plugin {}

    private static final Map<ReaderContext, Throwable> ACTIVE_SEARCH_CONTEXTS = new ConcurrentHashMap<>();

    private Consumer<ReaderContext> onPutContext = context -> {};
    private Consumer<ReaderContext> onRemoveContext = context -> {};

    private Consumer<SearchContext> onCreateSearchContext = context -> {};

    private Function<CancellableTask, CancellableTask> onCheckCancelled = Function.identity();

    /** Throw an {@link AssertionError} if there are still in-flight contexts. */
    public static void assertNoInFlightContext() {
        final Map<ReaderContext, Throwable> copy = new HashMap<>(ACTIVE_SEARCH_CONTEXTS);
        if (copy.isEmpty() == false) {
            throw new AssertionError(
                "There are still ["
                    + copy.size()
                    + "] in-flight contexts. The first one's creation site is listed as the cause of this exception.",
                copy.values().iterator().next()
            );
        }
    }

    /**
     * Add an active search context to the list of tracked contexts. Package private for testing.
     */
    static void addActiveContext(ReaderContext context) {
        ACTIVE_SEARCH_CONTEXTS.put(context, new RuntimeException(context.toString()));
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
    protected ReaderContext removeReaderContext(long id) {
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
        onPutContext.accept(searchContext.readerContext());
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
