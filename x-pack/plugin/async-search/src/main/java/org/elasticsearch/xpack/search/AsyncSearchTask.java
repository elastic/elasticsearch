/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.search.CCSSingleCoordinatorSearchProgressListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTask;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * Task that tracks the progress of a currently running {@link SearchRequest}.
 */
final class AsyncSearchTask extends SearchTask implements AsyncTask, Releasable {
    private final AsyncExecutionId searchId;
    private final Client client;
    private final ThreadPool threadPool;
    private final Supplier<AggregationReduceContext> aggReduceContextSupplier;
    private final Listener progressListener;

    private final Map<String, String> originHeaders;

    private boolean ccsMinimizeRoundtrips;
    private boolean hasInitialized;
    private boolean hasCompleted;
    private long completionId;
    private final List<Runnable> initListeners = new ArrayList<>();
    private final Map<Long, Consumer<AsyncSearchResponse>> completionListeners = new HashMap<>();

    private volatile long expirationTimeMillis;
    private final AtomicBoolean isCancelling = new AtomicBoolean(false);

    private final SetOnce<MutableSearchResponse> searchResponse = new SetOnce<>();

    /**
     * Creates an instance of {@link AsyncSearchTask}.
     *
     * @param id The id of the task.
     * @param type The type of the task.
     * @param action The action name.
     * @param parentTaskId The parent task id.
     * @param originHeaders All the request context headers.
     * @param taskHeaders The filtered request headers for the task.
     * @param searchId The {@link AsyncExecutionId} of the task.
     * @param threadPool The threadPool to schedule runnable.
     * @param aggReduceContextSupplierFactory A factory that creates as supplier to create final reduce contexts, we need a factory in
     *                                        order to inject the task itself to the reduce context.
     */
    AsyncSearchTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        Supplier<String> descriptionSupplier,
        TimeValue keepAlive,
        Map<String, String> originHeaders,
        Map<String, String> taskHeaders,
        AsyncExecutionId searchId,
        Client client,
        ThreadPool threadPool,
        Function<Supplier<Boolean>, Supplier<AggregationReduceContext>> aggReduceContextSupplierFactory
    ) {
        super(id, type, action, () -> "async_search{" + descriptionSupplier.get() + "}", parentTaskId, taskHeaders);
        this.expirationTimeMillis = getStartTime() + keepAlive.getMillis();
        this.originHeaders = originHeaders;
        this.searchId = searchId;
        this.client = client;
        this.threadPool = threadPool;
        this.aggReduceContextSupplier = aggReduceContextSupplierFactory.apply(this::isCancelled);
        this.progressListener = new Listener();
        setProgressListener(progressListener);
    }

    /**
     * Returns all of the request contexts headers
     */
    @Override
    public Map<String, String> getOriginHeaders() {
        return originHeaders;
    }

    /**
     * Returns the {@link AsyncExecutionId} of the task
     */
    @Override
    public AsyncExecutionId getExecutionId() {
        return searchId;
    }

    Listener getSearchProgressActionListener() {
        return progressListener;
    }

    /**
     * Update the expiration time of the (partial) response.
     */
    @Override
    public void setExpirationTime(long expirationTime) {
        this.expirationTimeMillis = expirationTime;
    }

    @Override
    public void cancelTask(TaskManager taskManager, Runnable runnable, String reason) {
        cancelTask(runnable, reason);
    }

    /**
     * Cancels the running task and its children.
     */
    public void cancelTask(Runnable runnable, String reason) {
        if (isCancelled() == false && isCancelling.compareAndSet(false, true)) {
            CancelTasksRequest req = new CancelTasksRequest().setTargetTaskId(searchId.getTaskId()).setReason(reason);
            client.admin().cluster().cancelTasks(req, new ActionListener<>() {
                @Override
                public void onResponse(ListTasksResponse cancelTasksResponse) {
                    runnable.run();
                }

                @Override
                public void onFailure(Exception exc) {
                    // cancelling failed
                    isCancelling.compareAndSet(true, false);
                    runnable.run();
                }
            });
        } else {
            runnable.run();
        }
    }

    @Override
    protected void onCancelled() {
        super.onCancelled();
        isCancelling.compareAndSet(true, false);
    }

    /**
     * Creates a listener that listens for an {@link AsyncSearchResponse} and notifies the
     * listener when the task is finished or when the provided <code>waitForCompletion</code>
     * timeout occurs. In such case the consumed {@link AsyncSearchResponse} will contain partial results.
     */
    public boolean addCompletionListener(ActionListener<AsyncSearchResponse> listener, TimeValue waitForCompletion) {
        boolean executeImmediately = false;
        long startTime = threadPool.relativeTimeInMillis();
        synchronized (this) {
            if (hasCompleted) {
                executeImmediately = true;
            } else {
                addInitListener(() -> {
                    final TimeValue remainingWaitForCompletion;
                    if (waitForCompletion.getMillis() > 0) {
                        long elapsedTime = threadPool.relativeTimeInMillis() - startTime;
                        // subtract the initialization time from the provided waitForCompletion.
                        remainingWaitForCompletion = TimeValue.timeValueMillis(Math.max(0, waitForCompletion.getMillis() - elapsedTime));
                    } else {
                        remainingWaitForCompletion = TimeValue.ZERO;
                    }
                    internalAddCompletionListener(listener, remainingWaitForCompletion);
                });
            }
        }
        if (executeImmediately) {
            ActionListener.respondAndRelease(listener, getResponseWithHeaders());
        }
        return true; // unused
    }

    /**
     * Creates a listener that listens for an {@link AsyncSearchResponse} and executes the
     * consumer when the task is finished.
     */
    public void addCompletionListener(Consumer<AsyncSearchResponse> listener) {
        boolean executeImmediately = false;
        synchronized (this) {
            if (hasCompleted) {
                executeImmediately = true;
            } else {
                this.completionListeners.put(completionId++, listener);
            }
        }
        if (executeImmediately) {
            var response = getResponseWithHeaders();
            try {
                listener.accept(response);
            } finally {
                response.decRef();
            }
        }
    }

    private void internalAddCompletionListener(ActionListener<AsyncSearchResponse> listener, TimeValue waitForCompletion) {
        boolean executeImmediately = false;
        synchronized (this) {
            if (hasCompleted || waitForCompletion.getMillis() == 0) {
                executeImmediately = true;
            } else {
                // ensure that we consume the listener only once
                AtomicBoolean hasRun = new AtomicBoolean(false);
                long id = completionId++;
                final Cancellable cancellable;
                try {
                    cancellable = threadPool.schedule(() -> {
                        if (hasRun.compareAndSet(false, true)) {
                            // timeout occurred before completion
                            removeCompletionListener(id);
                            ActionListener.respondAndRelease(listener, getResponseWithHeaders());
                        }
                    }, waitForCompletion, threadPool.generic());
                } catch (Exception exc) {
                    listener.onFailure(exc);
                    return;
                }
                completionListeners.put(id, resp -> {
                    if (hasRun.compareAndSet(false, true)) {
                        // completion occurred before timeout
                        cancellable.cancel();
                        listener.onResponse(resp);
                    }
                });
            }
        }
        if (executeImmediately) {
            ActionListener.respondAndRelease(listener, getResponseWithHeaders());
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
        boolean executeImmediately = false;
        synchronized (this) {
            if (hasInitialized) {
                executeImmediately = true;
            } else {
                initListeners.add(listener);
            }
        }
        if (executeImmediately) {
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
        Map<Long, Consumer<AsyncSearchResponse>> completionsListenersCopy;
        synchronized (this) {
            if (hasCompleted) {
                return;
            }
            hasCompleted = true;
            completionsListenersCopy = new HashMap<>(this.completionListeners);
            this.completionListeners.clear();
        }
        // we don't need to restore the response headers, they should be included in the current
        // context since we are called by the search action listener.
        AsyncSearchResponse finalResponse = getResponse();
        try {
            for (Consumer<AsyncSearchResponse> consumer : completionsListenersCopy.values()) {
                consumer.accept(finalResponse);
            }
        } finally {
            finalResponse.decRef();
        }
    }

    /**
     * Returns the current {@link AsyncSearchResponse}.
     */
    private AsyncSearchResponse getResponse() {
        return getResponse(false);
    }

    /**
     * Returns the current {@link AsyncSearchResponse} and restores the response headers
     * in the local thread context.
     */
    private AsyncSearchResponse getResponseWithHeaders() {
        return getResponse(true);
    }

    private AsyncSearchResponse getResponse(boolean restoreResponseHeaders) {
        MutableSearchResponse mutableSearchResponse = searchResponse.get();
        assert mutableSearchResponse != null;
        checkCancellation();
        AsyncSearchResponse asyncSearchResponse;
        try {
            asyncSearchResponse = mutableSearchResponse.toAsyncSearchResponse(this, expirationTimeMillis, restoreResponseHeaders);
        } catch (Exception e) {
            ElasticsearchException exception = new ElasticsearchStatusException(
                "Async search: error while reducing partial results",
                ExceptionsHelper.status(e),
                e
            );
            asyncSearchResponse = mutableSearchResponse.toAsyncSearchResponse(this, expirationTimeMillis, exception);
        }
        return asyncSearchResponse;
    }

    // checks if the search task should be cancelled
    private synchronized void checkCancellation() {
        long now = System.currentTimeMillis();
        if (hasCompleted == false && expirationTimeMillis < now) {
            // we cancel expired search task even if they are still running
            cancelTask(() -> {}, "async search has expired");
        }
    }

    /**
     * Returns the status from {@link AsyncSearchTask}
     */
    public static AsyncStatusResponse getStatusResponse(AsyncSearchTask asyncTask) {
        MutableSearchResponse mutableSearchResponse = asyncTask.searchResponse.get();
        assert mutableSearchResponse != null;
        return mutableSearchResponse.toStatusResponse(
            asyncTask.searchId.getEncoded(),
            asyncTask.getStartTime(),
            asyncTask.expirationTimeMillis
        );
    }

    @Override
    public void close() {
        Releasables.close(searchResponse.get());
    }

    class Listener extends SearchProgressActionListener {

        // needed when there's a single coordinator for all CCS search phases (minimize_roundtrips=false)
        private CCSSingleCoordinatorSearchProgressListener delegate;

        @Override
        protected void onQueryResult(int shardIndex, QuerySearchResult queryResult) {
            checkCancellation();
            if (delegate != null) {
                delegate.onQueryResult(shardIndex, queryResult);
            }
        }

        @Override
        protected void onFetchResult(int shardIndex) {
            checkCancellation();
            if (delegate != null) {
                delegate.onFetchResult(shardIndex);
            }
        }

        @Override
        protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
            // best effort to cancel expired tasks
            checkCancellation();
            if (delegate != null) {
                delegate.onQueryFailure(shardIndex, shardTarget, exc);
            }
            searchResponse.get()
                .addQueryFailure(
                    shardIndex,
                    // the nodeId is null if all replicas of this shard failed
                    new ShardSearchFailure(exc, shardTarget.getNodeId() != null ? shardTarget : null)
                );
        }

        @Override
        protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
            // best effort to cancel expired tasks
            checkCancellation();
            // ignore fetch failures: they make the shards count confusing if we count them as shard failures because the query
            // phase ran fine and we don't want to end up with e.g. total: 5 successful: 5 failed: 5.
            // Given that partial results include only aggs they are not affected by fetch failures. Async search receives the fetch
            // failures either as an exception (when all shards failed during fetch, in which case async search will return the error
            // as well as the response obtained after the final reduction) or as part of the final response (if only some shards failed,
            // in which case the final response already includes results as well as shard fetch failures)
        }

        /**
         * onListShards is guaranteed to be the first SearchProgressListener method called and
         * the search will not progress until this returns, so this is a safe place to initialize state
         * that is needed for handling subsequent callbacks.
         */
        @Override
        protected void onListShards(
            List<SearchShard> shards,
            List<SearchShard> skipped,
            Clusters clusters,
            boolean fetchPhase,
            TransportSearchAction.SearchTimeProvider timeProvider
        ) {
            // best effort to cancel expired tasks
            checkCancellation();
            assert clusters.isCcsMinimizeRoundtrips() != null : "CCS minimize_roundtrips value must be set in this context";
            ccsMinimizeRoundtrips = clusters.isCcsMinimizeRoundtrips();
            if (ccsMinimizeRoundtrips == false && clusters.hasClusterObjects()) {
                delegate = new CCSSingleCoordinatorSearchProgressListener();
                delegate.onListShards(shards, skipped, clusters, fetchPhase, timeProvider);
            }
            searchResponse.set(
                new MutableSearchResponse(shards.size() + skipped.size(), skipped.size(), clusters, threadPool.getThreadContext())
            );
            executeInitListeners();
        }

        @Override
        public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggregations, int reducePhase) {
            // best effort to cancel expired tasks
            checkCancellation();
            if (delegate != null) {
                delegate.onPartialReduce(shards, totalHits, aggregations, reducePhase);
            }
            // The way that the MutableSearchResponse will build the aggs.
            Supplier<InternalAggregations> reducedAggs;
            if (aggregations == null) {
                // There aren't any aggs to reduce.
                reducedAggs = () -> null;
            } else {
                /*
                 * Keep a reference to the partially reduced aggs and reduce it on the fly when someone asks
                 * for it. It's important that we wait until someone needs
                 * the result so we don't perform the final reduce only to
                 * throw it away. And it is important that we keep the reference
                 * to the aggregations because SearchPhaseController
                 * *already* has that reference so we're not creating more garbage.
                 */
                reducedAggs = () -> InternalAggregations.topLevelReduce(singletonList(aggregations), aggReduceContextSupplier.get());
            }
            searchResponse.get().updatePartialResponse(shards.size(), totalHits, reducedAggs, reducePhase);
        }

        /**
         * Called when the final reduce of <b>local</b> shards is done.
         * During a CCS search, there may still be shard searches in progress on remote clusters when this is called.
         */
        @Override
        public void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggregations, int reducePhase) {
            // best effort to cancel expired tasks
            checkCancellation();
            if (delegate != null) {
                delegate.onFinalReduce(shards, totalHits, aggregations, reducePhase);
            }
            searchResponse.get().updatePartialResponse(shards.size(), totalHits, () -> aggregations, reducePhase);
        }

        /**
         * Indicates that a cluster has finished a search operation. Used for CCS minimize_roundtrips=true only.
         *
         * @param clusterAlias alias of cluster that has finished a search operation and returned a SearchResponse.
         *                     The cluster alias for the local cluster is RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.
         * @param clusterResponse SearchResponse from cluster 'clusterAlias'
         */
        @Override
        public void onClusterResponseMinimizeRoundtrips(String clusterAlias, SearchResponse clusterResponse) {
            // no need to call the delegate progress listener, since this method is only called for minimize_roundtrips=true
            searchResponse.get().updateResponseMinimizeRoundtrips(clusterAlias, clusterResponse);
        }

        @Override
        public void onResponse(SearchResponse response) {
            searchResponse.get().updateFinalResponse(response, ccsMinimizeRoundtrips);
            executeCompletionListeners();
        }

        @Override
        public void onFailure(Exception exc) {
            // if the failure occurred before calling onListShards
            var r = new MutableSearchResponse(-1, -1, null, threadPool.getThreadContext());
            if (searchResponse.trySet(r) == false) {
                r.close();
            }
            searchResponse.get()
                .updateWithFailure(new ElasticsearchStatusException("error while executing search", ExceptionsHelper.status(exc), exc));
            executeInitListeners();
            executeCompletionListeners();
        }
    }
}
