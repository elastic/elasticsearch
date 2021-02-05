/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class AsyncPersistentSearch {
    private final Logger logger = LogManager.getLogger(AsyncPersistentSearch.class);

    private final SearchRequest searchRequest;
    private final String asyncSearchId;
    private final SearchTask task;
    private final OriginalIndices originalIndices;
    private final TimeValue expirationTime;
    private final int maxConcurrentQueryRequests;
    private final TransportSearchAction.SearchTimeProvider searchTimeProvider;
    private final SearchShardTargetResolver searchShardTargetResolver;
    private final SearchTransportService searchTransportService;
    private final ThreadPool threadPool;
    private final BiFunction<String, String, Transport.Connection> connectionProvider;
    private final ClusterService clusterService;
    private final ActionListener<Void> onCompletionListener;
    private final AtomicInteger pendingShardsToQueryCount;
    private final ShardQueryResultsReducer shardQueryResultsReducer;
    private final Queue<AsyncShardQueryAndFetch> pendingShardsToQuery;
    private final List<SearchShard> searchShards;

    private final AtomicInteger runningShardQueries = new AtomicInteger(0);
    private final AtomicBoolean searchRunning = new AtomicBoolean(true);

    public AsyncPersistentSearch(SearchRequest searchRequest,
                                 String asyncSearchId,
                                 SearchTask task,
                                 List<SearchShard> searchShards,
                                 OriginalIndices originalIndices,
                                 Map<String, AliasFilter> aliasFiltersByIndex,
                                 TimeValue expirationTime,
                                 int maxConcurrentQueryRequests,
                                 int maxShardsPerReduceRequest,
                                 TransportSearchAction.SearchTimeProvider searchTimeProvider,
                                 SearchShardTargetResolver searchShardTargetResolver,
                                 SearchTransportService searchTransportService,
                                 ThreadPool threadPool,
                                 BiFunction<String, String, Transport.Connection> connectionProvider,
                                 ClusterService clusterService,
                                 ActionListener<Void> onCompletionListener) {
        this.searchRequest = searchRequest;
        this.asyncSearchId = asyncSearchId;
        this.task = task;
        this.originalIndices = originalIndices;
        this.expirationTime = expirationTime;
        this.maxConcurrentQueryRequests = maxConcurrentQueryRequests;
        this.searchTimeProvider = searchTimeProvider;
        this.searchShardTargetResolver = searchShardTargetResolver;
        this.searchTransportService = searchTransportService;
        this.threadPool = threadPool;
        this.connectionProvider = connectionProvider;
        this.clusterService = clusterService;
        this.onCompletionListener = onCompletionListener;
        this.pendingShardsToQueryCount = new AtomicInteger(searchShards.size());
        this.shardQueryResultsReducer = new ShardQueryResultsReducer(searchShards.size(), maxShardsPerReduceRequest);

        // Query and reduce ordering by shard id
        this.searchShards = new ArrayList<>(searchShards);
        Collections.sort(this.searchShards);
        Queue<AsyncShardQueryAndFetch> pendingShardQueries = new PriorityQueue<>();
        final long expireAbsoluteTime = expirationTime.millis() + searchTimeProvider.getAbsoluteStartMillis();
        for (int i = 0; i < this.searchShards.size(); i++) {
            SearchShard searchShard = this.searchShards.get(i);
            PersistentSearchShardId persistentSearchShardId = new PersistentSearchShardId(searchShard, asyncSearchId, i);
            ShardSearchRequest shardSearchRequest =
                buildShardSearchRequest(searchRequest, originalIndices, searchTimeProvider, aliasFiltersByIndex, searchShard.getShardId());
            ExecutePersistentQueryFetchRequest request = new ExecutePersistentQueryFetchRequest(persistentSearchShardId,
                expireAbsoluteTime,
                shardSearchRequest);
            pendingShardQueries.add(new AsyncShardQueryAndFetch(persistentSearchShardId, request));
        }
        this.pendingShardsToQuery = pendingShardQueries;
    }

    public String getId() {
        return asyncSearchId;
    }

    public void start() {
        executePendingShardQueries();
    }

    public void cancelSearch() {
        searchRunning.compareAndSet(false, true);
    }

    private synchronized void executePendingShardQueries() {
        while (pendingShardsToQuery.peek() != null && runningShardQueries.get() < maxConcurrentQueryRequests) {
            final AsyncShardQueryAndFetch asyncShardQueryAndFetch = pendingShardsToQuery.poll();
            assert asyncShardQueryAndFetch != null;
            runningShardQueries.incrementAndGet();
            asyncShardQueryAndFetch.run();
        }
    }

    private void onShardQuerySuccess(PersistentSearchShardId searchShard) {
        runningShardQueries.decrementAndGet();
        shardQueryResultsReducer.reduce(searchShard);
        decrementPendingShardsToQuery();
    }

    private void onShardQueryFailure(AsyncShardQueryAndFetch searchShard, Exception e) {
        runningShardQueries.decrementAndGet();

        if (searchShard.canBeRetried()) {
            threadPool.schedule(() -> reEnqueueShardQuery(searchShard), searchShard.nextExecutionDeadline(), ThreadPool.Names.GENERIC);
        } else {
            // Mark shard as failed on the search result? This is an unrecoverable failure
            decrementPendingShardsToQuery();
        }
    }

    private void decrementPendingShardsToQuery() {
        if (pendingShardsToQueryCount.decrementAndGet() == 0) {
            shardQueryResultsReducer.allShardsAreQueried();
        } else {
            executePendingShardQueries();
        }
    }

    private synchronized void reEnqueueShardQuery(AsyncShardQueryAndFetch searchShard) {
        pendingShardsToQuery.add(searchShard);
        executePendingShardQueries();
    }

    private void sendReduceRequest(ReducePartialPersistentSearchRequest request,
                                   ActionListener<ReducePartialPersistentSearchResponse> listener) {
        // For now, execute reduce requests in the coordinator node
        Transport.Connection connection = getConnection(null, clusterService.localNode().getId());
        searchTransportService.sendExecutePartialReduceRequest(connection,
            request,
            task,
            listener
        );
    }

    private void onSearchSuccess() {
        // TODO: Materialize shard failures in the search response
        onCompletionListener.onResponse(null);
    }

    private void sendShardSearchRequest(SearchShardTarget searchShardTarget,
                                        ExecutePersistentQueryFetchRequest asyncShardSearchRequest,
                                        ActionListener<Void> listener) {
        final Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
        searchTransportService.sendExecutePersistentQueryFetchRequest(connection,
            asyncShardSearchRequest,
            task,
            ActionListener.delegateFailure(listener, (l, r) -> l.onResponse(null))
        );
    }

    private Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return connectionProvider.apply(clusterAlias, nodeId);
    }

    private static ShardSearchRequest buildShardSearchRequest(SearchRequest searchRequest,
                                                              OriginalIndices originalIndices,
                                                              TransportSearchAction.SearchTimeProvider searchTimeProvider,
                                                              Map<String, AliasFilter> aliasFiltersByIndex,
                                                              ShardId shardId) {
        AliasFilter filter = aliasFiltersByIndex.getOrDefault(shardId.getIndexName(), AliasFilter.EMPTY);
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            originalIndices,
            searchRequest,
            shardId,
            0,
            1, //Hardcoded so the optimization of query + fetch is triggered
            filter,
            1.0f,
            searchTimeProvider.getAbsoluteStartMillis(),
            null,
            null,
            null
        );
        shardRequest.canReturnNullResponseIfMatchNoDocs(false);
        return shardRequest;
    }

    private class AsyncShardQueryAndFetch implements Comparable<AsyncShardQueryAndFetch>, ActionListener<Void> {
        private final PersistentSearchShardId searchShardId;
        private final ExecutePersistentQueryFetchRequest shardSearchRequest;
        private volatile SearchShardIterator searchShardIterator = null;
        private volatile List<Throwable> failures = null;
        private int retryCount = 0;

        private AsyncShardQueryAndFetch(PersistentSearchShardId searchShardId, ExecutePersistentQueryFetchRequest shardSearchRequest) {
            this.searchShardId = searchShardId;
            this.shardSearchRequest = shardSearchRequest;
        }

        void run() {
            searchShardTargetResolver.resolve(searchShardId.getSearchShard(), originalIndices, new ActionListener<>() {
                @Override
                public void onResponse(SearchShardIterator searchShardIterator) {
                    doRun(searchShardIterator);
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO: Handle this
                }
            });
        }

        void doRun(SearchShardIterator searchShardIterator) {
            this.searchShardIterator = searchShardIterator;
            sendRequestToNextShardCopy();
        }

        void sendRequestToNextShardCopy() {
            assert searchShardIterator != null;

            if (searchRunning.get() == false) {
                clear();
                return;
            }

            SearchShardTarget target = searchShardIterator.nextOrNull();
            if (target == null) {
                onShardQueryFailure(this, buildException());
                return;
            }

            sendShardSearchRequest(target, shardSearchRequest, this);
        }

        @Override
        public void onResponse(Void unused) {
            onShardQuerySuccess(searchShardId);
            clear();
        }

        @Override
        public void onFailure(Exception e) {
            if (failures == null) {
                failures = new ArrayList<>();
            }
            failures.add(e);
            sendRequestToNextShardCopy();
        }

        boolean canBeRetried() {
            if (failures == null) {
                return true;
            }

            for (Throwable failure : failures) {
                if (failure instanceof IndexNotFoundException) {
                    return false;
                }
            }

            return true;
        }

        TimeValue nextExecutionDeadline() {
            return TimeValue.timeValueSeconds(1 << retryCount++);
        }

        void clear() {
            searchShardIterator = null;
            if (failures != null) {
                failures.clear();
            }
        }

        RuntimeException buildException() {
            RuntimeException e = new RuntimeException("Unable to execute search on shard " + searchShardId);
            if (failures != null) {
                for (Throwable failure : failures) {
                    e.addSuppressed(failure);
                }
            }
            return e;
        }

        @Override
        public int compareTo(AsyncShardQueryAndFetch o) {
            return searchShardId.compareTo(o.searchShardId);
        }
    }

    private class ShardQueryResultsReducer {
        private final PriorityQueue<PersistentSearchShardId> pendingShardsToReduce = new PriorityQueue<>();
        private final AtomicReference<ReducePartialPersistentSearchRequest> runningReduce = new AtomicReference<>(null);
        private final AtomicBoolean allShardsAreQueried = new AtomicBoolean(false);
        private final AtomicInteger numberOfShardsToReduce;
        private final int maxShardsPerReduceBatch;

        private ShardQueryResultsReducer(int numberOfShards, int maxShardsPerReduceRequest) {
            this.numberOfShardsToReduce = new AtomicInteger(numberOfShards);
            this.maxShardsPerReduceBatch = maxShardsPerReduceRequest;
        }

        void reduce(PersistentSearchShardId searchShard) {
            assert allShardsAreQueried.get() == false;

            synchronized (pendingShardsToReduce) {
                pendingShardsToReduce.add(searchShard);
            }

            maybeRun();
        }

        void allShardsAreQueried() {
            if (allShardsAreQueried.compareAndSet(false, true)) {
                maybeRun();
            }
        }

        void reduceAll(List<PersistentSearchShardId> shards) {
            synchronized (pendingShardsToReduce) {
                pendingShardsToReduce.addAll(shards);
            }
            maybeRun();
        }

        void maybeRun() {
            if (searchRunning.get() == false || runningReduce.get() != null) {
                return;
            }

            final ReducePartialPersistentSearchRequest reducePartialPersistentSearchRequest;
            synchronized (pendingShardsToReduce) {
                if (runningReduce.get() == null && (pendingShardsToReduce.size() >= maxShardsPerReduceBatch || allShardsAreQueried.get())) {
                    final List<PersistentSearchShardId> shardsToReduce = new ArrayList<>(maxShardsPerReduceBatch);
                    PersistentSearchShardId next;
                    while (shardsToReduce.size() <= maxShardsPerReduceBatch && (next = pendingShardsToReduce.poll()) != null) {
                        shardsToReduce.add(next);
                    }

                    reducePartialPersistentSearchRequest = new ReducePartialPersistentSearchRequest(asyncSearchId,
                            shardsToReduce,
                            searchRequest,
                            numberOfShardsToReduce.get() == shardsToReduce.size(),
                            searchTimeProvider.getAbsoluteStartMillis(),
                            searchTimeProvider.getRelativeStartNanos(),
                            expirationTime.millis() + searchTimeProvider.getAbsoluteStartMillis()
                        );
                    runningReduce.compareAndSet(null, reducePartialPersistentSearchRequest);
                } else {
                    reducePartialPersistentSearchRequest = null;
                }
            }

            if (reducePartialPersistentSearchRequest != null) {
                sendReduceRequest(reducePartialPersistentSearchRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(ReducePartialPersistentSearchResponse response) {
                        final boolean exchanged = runningReduce.compareAndSet(reducePartialPersistentSearchRequest, null);
                        assert exchanged;
                        final List<PersistentSearchShardId> shardsToReduce = reducePartialPersistentSearchRequest.getShardsToReduce();
                        final List<PersistentSearchShardId> reducedShards = response.getReducedShards();

                        if (reducedShards.size() != shardsToReduce.size()) {
                            final List<PersistentSearchShardId> pendingShardsToReduce = new ArrayList<>(shardsToReduce);
                            pendingShardsToReduce.removeAll(reducedShards);
                            // TODO: In some cases we might need to query the shard again?
                            // Retry failed shards
                            if (pendingShardsToReduce.isEmpty() == false) {
                                reduceAll(pendingShardsToReduce);
                            }
                        }

                        if (numberOfShardsToReduce.addAndGet(-reducedShards.size()) == 0) {
                            onSearchSuccess();
                        } else {
                            // TODO: fork as this might cause a stack overflow
                            maybeRun();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final boolean exchanged = runningReduce.compareAndSet(reducePartialPersistentSearchRequest, null);
                        assert exchanged;
                        // TODO: Store the error
                        // TODO: It's possible that we already reduced and stored the intermediate search response
                        //       but due to a network error we don't get the response back and we get an error back.
                        //       Add some versioning to avoid that (right now, we're executing the reduce phase in the coordinator
                        //       node, so this shouldn't happen)
                        logger.debug("Error during shards reduce", e);
                        reduceAll(reducePartialPersistentSearchRequest.getShardsToReduce());
                    }
                });
            }
        }
    }
}
