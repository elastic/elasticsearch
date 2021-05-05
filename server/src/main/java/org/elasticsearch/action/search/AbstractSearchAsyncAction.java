/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.transport.Transport;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is an abstract base class that encapsulates the logic to fan out to all shards in provided {@link GroupShardsIterator}
 * and collect the results. If a shard request returns a failure this class handles the advance to the next replica of the shard until
 * the shards replica iterator is exhausted. Each shard is referenced by position in the {@link GroupShardsIterator} which is later
 * referred to as the {@code shardIndex}.
 * The fan out and collect algorithm is traditionally used as the initial phase which can either be a query execution or collection of
 * distributed frequencies
 */
abstract class AbstractSearchAsyncAction<Result extends SearchPhaseResult> extends SearchPhase {
    protected final SearchPhaseContext context;
    private final SearchRequest request;

    private final Logger logger;

    protected final SearchPhaseResults<Result> results;

    protected final GroupShardsIterator<SearchShardIterator> toSkipShardsIts;
    protected final GroupShardsIterator<SearchShardIterator> shardsIts;
    protected final Map<SearchShardIterator, Integer> shardItIndexMap;
    private final int expectedTotalOps;
    private final AtomicInteger totalOps = new AtomicInteger();
    private final int maxConcurrentRequestsPerNode;
    private final Map<String, PendingExecutions> pendingExecutionsPerNode = new ConcurrentHashMap<>();
    private final boolean throttleConcurrentRequests;

    AbstractSearchAsyncAction(String name, SearchPhaseContext context, Logger logger,
                              GroupShardsIterator<SearchShardIterator> shardsIts,
                              SearchPhaseResults<Result> resultConsumer, int maxConcurrentRequestsPerNode) {
        super(name);
        this.context = context;
        this.request = context.getRequest();

        final List<SearchShardIterator> toSkipIterators = new ArrayList<>();
        final List<SearchShardIterator> iterators = new ArrayList<>();
        for (final SearchShardIterator iterator : shardsIts) {
            if (iterator.skip()) {
                toSkipIterators.add(iterator);
            } else {
                iterators.add(iterator);
            }
        }
        this.toSkipShardsIts = new GroupShardsIterator<>(toSkipIterators);
        this.shardsIts = new GroupShardsIterator<>(iterators);
        this.shardItIndexMap = new HashMap<>();

        // we compute the shard index based on the natural order of the shards
        // that participate in the search request. This means that this number is
        // consistent between two requests that target the same shards.
        List<SearchShardIterator> naturalOrder = new ArrayList<>(iterators);
        CollectionUtil.timSort(naturalOrder);
        for (int i = 0; i < naturalOrder.size(); i++) {
            shardItIndexMap.put(naturalOrder.get(i), i);
        }

        // we need to add 1 for non active partition, since we count it in the total. This means for each shard in the iterator we sum up
        // it's number of active shards but use 1 as the default if no replica of a shard is active at this point.
        // on a per shards level we use shardIt.remaining() to increment the totalOps pointer but add 1 for the current shard result
        // we process hence we add one for the non active partition here.
        this.expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        this.maxConcurrentRequestsPerNode = maxConcurrentRequestsPerNode;
        // in the case were we have less shards than maxConcurrentRequestsPerNode we don't need to throttle
        this.throttleConcurrentRequests = maxConcurrentRequestsPerNode < shardsIts.size();
        this.logger = logger;
        this.results = resultConsumer;
    }

    /**
     * This is the main entry point for a search. This method starts the search execution of the initial phase.
     */
    public final void start() {
        if (context.getNumShards() == 0) {
            //no search shards to search on, bail with empty response
            //(it happens with search across _all with no indices around and consistent with broadcast operations)
            int trackTotalHitsUpTo = request.source() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO :
                request.source().trackTotalHitsUpTo() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO :
                    request.source().trackTotalHitsUpTo();
            // total hits is null in the response if the tracking of total hits is disabled
            boolean withTotalHits = trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
            context.getListener().onResponse(new SearchResponse(InternalSearchResponse.empty(withTotalHits), null, 0, 0, 0,
                context.buildTookInMillis(), ShardSearchFailure.EMPTY_ARRAY, context.getClusters(), null));
            return;
        }
        executePhase(this);
    }

    @Override
    public final void run() {
        for (final SearchShardIterator iterator : toSkipShardsIts) {
            assert iterator.skip();
            skipShard(iterator);
        }
        if (shardsIts.size() > 0) {
            assert request.allowPartialSearchResults() != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (request.allowPartialSearchResults() == false) {
                final StringBuilder missingShards = new StringBuilder();
                // Fail-fast verification of all shards being available
                for (int index = 0; index < shardsIts.size(); index++) {
                    final SearchShardIterator shardRoutings = shardsIts.get(index);
                    if (shardRoutings.size() == 0) {
                        if(missingShards.length() > 0){
                            missingShards.append(", ");
                        }
                        missingShards.append(shardRoutings.shardId());
                    }
                }
                if (missingShards.length() > 0) {
                    //Status red - shard is missing all copies and would produce partial results for an index search
                    final String msg = "Search rejected due to missing shards ["+ missingShards +
                        "]. Consider using `allow_partial_search_results` setting to bypass this error.";
                    throw new SearchPhaseExecutionException(getName(), msg, null, ShardSearchFailure.EMPTY_ARRAY);
                }
            }
            Version version = request.minCompatibleShardNode();
            if (version != null && Version.CURRENT.minimumCompatibilityVersion().equals(version) == false) {
                if (checkMinimumVersion(shardsIts) == false) {
                    throw new VersionMismatchException("One of the shards is incompatible with the required minimum version [{}]",
                        request.minCompatibleShardNode());
                }
            }
            for (int i = 0; i < shardsIts.size(); i++) {
                final SearchShardIterator shardRoutings = shardsIts.get(i);
                assert shardRoutings.skip() == false;
                assert shardItIndexMap.containsKey(shardRoutings);
                int shardIndex = shardItIndexMap.get(shardRoutings);
                performPhaseOnShard(shardIndex, shardRoutings, shardRoutings.nextOrNull());
            }
        }
    }

    void skipShard(SearchShardIterator iterator) {
        context.getSuccessfulOps().incrementAndGet();
        context.getSkippedOps().incrementAndGet();
        assert iterator.skip();
        successfulShardExecution(iterator);
    }

    private boolean checkMinimumVersion(GroupShardsIterator<SearchShardIterator> shardsIts) {
        for (SearchShardIterator it : shardsIts) {
            if (it.getTargetNodeIds().isEmpty() == false) {
                boolean isCompatible = it.getTargetNodeIds().stream().anyMatch(nodeId -> {
                    Transport.Connection conn = context.getConnection(it.getClusterAlias(), nodeId);
                    return conn == null ? true : conn.getVersion().onOrAfter(request.minCompatibleShardNode());
                });
                if (isCompatible == false) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean assertExecuteOnStartThread() {
        // Ensure that the current code has the following stacktrace:
        // AbstractSearchAsyncAction#start -> AbstractSearchAsyncAction#executePhase -> AbstractSearchAsyncAction#performPhaseOnShard
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        assert stackTraceElements.length >= 6 : stackTraceElements;
        int index = 0;
        assert stackTraceElements[index++].getMethodName().equals("getStackTrace");
        assert stackTraceElements[index++].getMethodName().equals("assertExecuteOnStartThread");
        assert stackTraceElements[index++].getMethodName().equals("performPhaseOnShard");
        if (stackTraceElements[index].getMethodName().equals("performPhaseOnShard")) {
            assert stackTraceElements[index].getClassName().endsWith("CanMatchPreFilterSearchPhase");
            index++;
        }
        assert stackTraceElements[index].getClassName().endsWith("AbstractSearchAsyncAction");
        assert stackTraceElements[index++].getMethodName().equals("run");

        assert stackTraceElements[index].getClassName().endsWith("AbstractSearchAsyncAction");
        assert stackTraceElements[index++].getMethodName().equals("executePhase");

        assert stackTraceElements[index].getClassName().endsWith("AbstractSearchAsyncAction");
        assert stackTraceElements[index++].getMethodName().equals("start");

        assert stackTraceElements[index].getClassName().endsWith("AbstractSearchAsyncAction") == false;
        return true;
    }

    protected void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final SearchShardTarget shard) {
        /*
         * We capture the thread that this phase is starting on. When we are called back after executing the phase, we are either on the
         * same thread (because we never went async, or the same thread was selected from the thread pool) or a different thread. If we
         * continue on the same thread in the case that we never went async and this happens repeatedly we will end up recursing deeply and
         * could stack overflow. To prevent this, we fork if we are called back on the same thread that execution started on and otherwise
         * we can continue (cf. InitialSearchPhase#maybeFork).
         */
        if (shard == null) {
            //assert assertExecuteOnStartThread();
            SearchShardTarget unassignedShard = new SearchShardTarget(null, shardIt.shardId(),
                shardIt.getClusterAlias(), shardIt.getOriginalIndices());
            onShardFailure(shardIndex, unassignedShard, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
        } else {
            final PendingExecutions pendingExecutions = throttleConcurrentRequests ?
                pendingExecutionsPerNode.computeIfAbsent(shard.getNodeId(), n -> new PendingExecutions(maxConcurrentRequestsPerNode))
                : null;
            Runnable r = () -> {
                final Thread thread = Thread.currentThread();
                try {
                    executePhaseOnShard(shardIt, shard,
                        new SearchActionListener<Result>(shard, shardIndex) {
                            @Override
                            public void innerOnResponse(Result result) {
                                try {
                                    onShardResult(result, shardIt);
                                } catch (Exception exc) {
                                    onShardFailure(shardIndex, shard, shardIt, exc);
                                } finally {
                                    executeNext(pendingExecutions, thread);
                                }
                            }

                            @Override
                            public void onFailure(Exception t) {
                                try {
                                    onShardFailure(shardIndex, shard, shardIt, t);
                                } finally {
                                    executeNext(pendingExecutions, thread);
                                }
                            }
                        });
                } catch (final Exception e) {
                    try {
                        /*
                         * It is possible to run into connection exceptions here because we are getting the connection early and might
                         * run into nodes that are not connected. In this case, on shard failure will move us to the next shard copy.
                         */
                        fork(() -> onShardFailure(shardIndex, shard, shardIt, e));
                    } finally {
                        executeNext(pendingExecutions, thread);
                    }
                }
            };
            if (throttleConcurrentRequests) {
                pendingExecutions.tryRun(r);
            } else {
                r.run();
            }
        }
    }

    /**
     * Sends the request to the actual shard.
     * @param shardIt the shards iterator
     * @param shard the shard routing to send the request for
     * @param listener the listener to notify on response
     */
    protected abstract void executePhaseOnShard(SearchShardIterator shardIt,
                                                SearchShardTarget shard,
                                                SearchActionListener<Result> listener);

    protected void fork(final Runnable runnable) {
        context.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
            }

            @Override
            protected void doRun() {
                runnable.run();
            }

            @Override
            public boolean isForceExecution() {
                // we can not allow a stuffed queue to reject execution here
                return true;
            }
        });
    }

    private void executePhase(SearchPhase phase) {
        try {
            phase.run();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(new ParameterizedMessage("Failed to execute [{}] while moving to [{}] phase", request, phase.getName()), e);
            }
            context.onPhaseFailure(phase, "", e);
        }
    }

    private void onShardFailure(final int shardIndex, SearchShardTarget shard, final SearchShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        context.onShardFailure(shardIndex, shard, e);
        final SearchShardTarget nextShard = shardIt.nextOrNull();
        final boolean lastShard = nextShard == null;
        logger.debug(() -> new ParameterizedMessage("{}: Failed to execute [{}] lastShard [{}]", shard, request, lastShard), e);
        if (lastShard) {
            if (request.allowPartialSearchResults() == false) {
                if (context.getRequestCancelled().compareAndSet(false, true)) {
                    try {
                        context.getSearchTransport().cancelSearchTask(context.getTask(),
                            "partial results are not allowed and at least one shard has failed");
                    } catch (Exception cancelFailure) {
                        logger.debug("Failed to cancel search request", cancelFailure);
                    }
                }
            }
            onShardGroupFailure(shardIndex, shard, e);
        }
        final int totalOps = this.totalOps.incrementAndGet();
        if (totalOps == expectedTotalOps) {
            onPhaseDone();
        } else if (totalOps > expectedTotalOps) {
            throw new AssertionError("unexpected higher total ops [" + totalOps + "] compared to expected [" + expectedTotalOps + "]");
        } else {
            if (lastShard == false) {
                performPhaseOnShard(shardIndex, shardIt, nextShard);
            }
        }
    }

    /**
     * Executed once for every {@link ShardId} that failed on all available shard routing.
     *
     * @param shardIndex the shard index that failed
     * @param shardTarget the last shard target for this failure
     * @param exc the last failure reason
     */
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {}

    /**
     * Executed once for every successful shard level request.
     * @param result the result returned form the shard
     * @param shardIt the shard iterator
     */
    protected void onShardResult(Result result, SearchShardIterator shardIt) {
        assert result.getShardIndex() != -1 : "shard index is not set";
        assert result.getSearchShardTarget() != null : "search shard target must not be null";
        context.hasShardResponse().set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.getSearchShardTarget() : null);
        }
        results.consumeResult(result, () -> onShardResultConsumed(result, shardIt));
    }

    private void onShardResultConsumed(Result result, SearchShardIterator shardIt) {
        context.getSuccessfulOps().incrementAndGet();
        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so its ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = context.getShardFailures().get();
        if (shardFailures != null) {
            shardFailures.set(result.getShardIndex(), null);
        }
        // we need to increment successful ops first before we compare the exit condition otherwise if we
        // are fast we could concurrently update totalOps but then preempt one of the threads which can
        // cause the successor to read a wrong value from successfulOps if second phase is very fast ie. count etc.
        // increment all the "future" shards to update the total ops since we some may work and some may not...
        // and when that happens, we break on total ops, so we must maintain them
        successfulShardExecution(shardIt);
    }

    private void successfulShardExecution(SearchShardIterator shardsIt) {
        final int remainingOpsOnIterator;
        if (shardsIt.skip()) {
            // It's possible that we're skipping a shard that's unavailable
            // but its range was available in the IndexMetadata, in that
            // case the shardsIt.remaining() would be 0, expectedTotalOps
            // accounts for unavailable shards too.
            remainingOpsOnIterator = Math.max(shardsIt.remaining(), 1);
        } else {
            remainingOpsOnIterator = shardsIt.remaining() + 1;
        }
        final int xTotalOps = totalOps.addAndGet(remainingOpsOnIterator);
        if (xTotalOps == expectedTotalOps) {
            onPhaseDone();
        } else if (xTotalOps > expectedTotalOps) {
            throw new AssertionError("unexpected higher total ops [" + xTotalOps + "] compared to expected [" + expectedTotalOps + "]");
        }
    }

    /**
     * Executed once all shard results have been received and processed
     * @see DefaultSearchPhaseContext#onShardFailure(int, SearchShardTarget, Exception)
     * @see #onShardResult(SearchPhaseResult, SearchShardIterator)
     */
    final void onPhaseDone() {  // as a tribute to @kimchy aka. finishHim()
        context.executeNextPhase(this, getNextPhase(results, context));
    }

    /**
     * Returns the next phase based on the results of the initial search phase
     * @param results the results of the initial search phase. Each non null element in the result array represent a successfully
     *                executed shard request
     * @param context the search context for the next phase
     */
    protected abstract SearchPhase getNextPhase(SearchPhaseResults<Result> results, SearchPhaseContext context);

    private void executeNext(PendingExecutions pendingExecutions, Thread originalThread) {
        executeNext(pendingExecutions == null ? null : pendingExecutions.finishAndGetNext(), originalThread);
    }

    void executeNext(Runnable runnable, Thread originalThread) {
        if (runnable != null) {
            assert throttleConcurrentRequests;
            if (originalThread == Thread.currentThread()) {
                fork(runnable);
            } else {
                runnable.run();
            }
        }
    }

    private static final class PendingExecutions {
        private final int permits;
        private int permitsTaken = 0;
        private ArrayDeque<Runnable> queue = new ArrayDeque<>();

        PendingExecutions(int permits) {
            assert permits > 0 : "not enough permits: " + permits;
            this.permits = permits;
        }

        Runnable finishAndGetNext() {
            synchronized (this) {
                permitsTaken--;
                assert permitsTaken >= 0 : "illegal taken permits: " + permitsTaken;
            }
            return tryQueue(null);
        }

        void tryRun(Runnable runnable) {
            Runnable r = tryQueue(runnable);
            if (r != null) {
                r.run();
            }
        }

        private synchronized Runnable tryQueue(Runnable runnable) {
            Runnable toExecute = null;
            if (permitsTaken < permits) {
                permitsTaken++;
                toExecute = runnable;
                if (toExecute == null) { // only poll if we don't have anything to execute
                    toExecute = queue.poll();
                }
                if (toExecute == null) {
                    permitsTaken--;
                }
            } else if (runnable != null) {
                queue.add(runnable);
            }
            return toExecute;
        }
    }
}
