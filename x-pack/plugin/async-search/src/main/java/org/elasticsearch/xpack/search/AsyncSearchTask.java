/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.tasks.TaskId.EMPTY_TASK_ID;

/**
 * Task that tracks the progress of a currently running {@link SearchRequest}.
 */
class AsyncSearchTask extends SearchTask {
    private final AsyncSearchId searchId;
    private final ThreadPool threadPool;
    private final Supplier<ReduceContext> reduceContextSupplier;
    private final Listener progressListener;

    private final Map<String, String> originHeaders;

    private boolean hasInitialized;
    private boolean hasCompleted;
    private long completionId;
    private final List<Runnable> initListeners = new ArrayList<>();
    private final Map<Long, Consumer<AsyncSearchResponse>> completionListeners = new HashMap<>();

    private MutableSearchResponse searchResponse;

    /**
     * Creates an instance of {@link AsyncSearchTask}.
     *
     * @param id The id of the task.
     * @param type The type of the task.
     * @param action The action name.
     * @param originHeaders All the request context headers.
     * @param taskHeaders The filtered request headers for the task.
     * @param searchId The {@link AsyncSearchId} of the task.
     * @param threadPool The threadPool to schedule runnable.
     * @param reduceContextSupplier A supplier to create final reduce contexts.
     */
    AsyncSearchTask(long id,
                    String type,
                    String action,
                    Map<String, String> originHeaders,
                    Map<String, String> taskHeaders,
                    AsyncSearchId searchId,
                    ThreadPool threadPool,
                    Supplier<ReduceContext> reduceContextSupplier) {
        super(id, type, action, null, EMPTY_TASK_ID, taskHeaders);
        this.originHeaders = originHeaders;
        this.searchId = searchId;
        this.threadPool = threadPool;
        this.reduceContextSupplier = reduceContextSupplier;
        this.progressListener = new Listener();
        setProgressListener(progressListener);
    }

    /**
     * Returns all of the request contexts headers
     */
    Map<String, String> getOriginHeaders() {
        return originHeaders;
    }

    /**
     * Returns the {@link AsyncSearchId} of the task
     */
    AsyncSearchId getSearchId() {
        return searchId;
    }

    @Override
    public SearchProgressActionListener getProgressListener() {
        return progressListener;
    }

    /**
     * Creates a listener that listens for an {@link AsyncSearchResponse} and executes the
     * consumer when the task is finished or when the provided <code>waitForCompletion</code>
     * timeout occurs. In such case the consumed {@link AsyncSearchResponse} will contain partial results.
     */
    public void addCompletionListener(Consumer<AsyncSearchResponse> listener, TimeValue waitForCompletion) {
        boolean executeImmediatly = false;
        synchronized (this) {
            if (hasCompleted) {
                executeImmediatly = true;
            } else {
                addInitListener(() -> internalAddCompletionListener(listener, waitForCompletion));
            }
        }
        if (executeImmediatly) {
            listener.accept(getResponse());
        }
    }

    /**
     * Creates a listener that listens for an {@link AsyncSearchResponse} and executes the
     * consumer when the task is finished.
     */
    public void addCompletionListener(Consumer<AsyncSearchResponse>  listener) {
        boolean executeImmediatly = false;
        synchronized (this) {
            if (hasCompleted == false) {
                completionListeners.put(completionId++, resp -> listener.accept(getResponse()));
            } else {
                executeImmediatly = true;
            }
        }
        if (executeImmediatly) {
            listener.accept(getResponse());
        }
    }

    private void internalAddCompletionListener(Consumer<AsyncSearchResponse> listener, TimeValue waitForCompletion) {
        boolean executeImmediatly = false;
        synchronized (this) {
            if (hasCompleted == false) {
                // ensure that we consumes the listener only once
                AtomicBoolean hasRun = new AtomicBoolean(false);
                long id = completionId++;
                Cancellable cancellable =
                    threadPool.schedule(() -> {
                        if (hasRun.compareAndSet(false, true)) {
                            // timeout occurred before completion
                            removeCompletionListener(id);
                            listener.accept(getResponse());
                        }
                    }, waitForCompletion, "generic");
                completionListeners.put(id, resp -> {
                    if (hasRun.compareAndSet(false, true)) {
                        // completion occurred before timeout
                        cancellable.cancel();
                        listener.accept(resp);
                    }
                });
            } else {
                executeImmediatly = true;
            }
        }
        if (executeImmediatly) {
            listener.accept(getResponse());
        }
    }

    private void removeCompletionListener(long id) {
        synchronized (this) {
            if (hasCompleted == false) {
                completionListeners.remove(id);
            }
        }
    }

    private void addInitListener(Runnable listener) {
        boolean executeImmediatly = false;
        synchronized (this) {
            if (hasInitialized) {
                executeImmediatly = true;
            } else {
                initListeners.add(listener);
            }
        }
        if (executeImmediatly) {
            listener.run();
        }
    }

    private void executeInitListeners() {
        synchronized (this) {
            if (hasInitialized) {
                return;
            }
            hasInitialized = true;
        }
        for (Runnable listener : initListeners) {
            listener.run();
        }
        initListeners.clear();
    }

    private void executeCompletionListeners() {
        synchronized (this) {
            hasCompleted = true;
        }
        AsyncSearchResponse finalResponse = getResponse();
        for (Consumer<AsyncSearchResponse> listener : completionListeners.values()) {
            listener.accept(finalResponse);
        }
        completionListeners.clear();
    }

    private AsyncSearchResponse getResponse() {
        assert searchResponse != null;
        return searchResponse.toAsyncSearchResponse(this);
    }

    private class Listener extends SearchProgressActionListener {
        @Override
        public void onListShards(List<SearchShard> shards, List<SearchShard> skipped, Clusters clusters, boolean fetchPhase) {
            searchResponse = new MutableSearchResponse(shards.size() + skipped.size(), skipped.size(), clusters, reduceContextSupplier);
            executeInitListeners();
        }

        @Override
        public void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
            searchResponse.addShardFailure(shardIndex, new ShardSearchFailure(exc, shardTarget));
        }

        @Override
        public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
            searchResponse.updatePartialResponse(shards.size(),
                new InternalSearchResponse(new SearchHits(SearchHits.EMPTY, totalHits, Float.NaN), aggs,
                    null, null, false, null, reducePhase), aggs == null);
        }

        @Override
        public void onReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
            searchResponse.updatePartialResponse(shards.size(),
                new InternalSearchResponse(new SearchHits(SearchHits.EMPTY, totalHits, Float.NaN), aggs,
                    null, null, false, null, reducePhase), true);
        }

        @Override
        public void onResponse(SearchResponse response) {
            searchResponse.updateFinalResponse(response.getSuccessfulShards(), response.getInternalResponse());
            executeCompletionListeners();
        }

        @Override
        public void onFailure(Exception exc) {
            if (searchResponse == null) {
                // if the failure occurred before calling onListShards
                searchResponse = new MutableSearchResponse(-1, -1, null, reduceContextSupplier);
            }
            searchResponse.updateWithFailure(exc);
            executeInitListeners();
            executeCompletionListeners();
        }
    }
}
