/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;


abstract class AbstractSearchAsyncAction<FirstResult extends SearchPhaseResult> extends AbstractAsyncAction {
    private static final float DEFAULT_INDEX_BOOST = 1.0f;
    protected final Logger logger;
    protected final SearchTransportService searchTransportService;
    private final Executor executor;
    protected final ActionListener<SearchResponse> listener;
    private final GroupShardsIterator shardsIts;
    protected final SearchRequest request;
    /** Used by subclasses to resolve node ids to DiscoveryNodes. **/
    protected final Function<String, Transport.Connection> nodeIdToConnection;
    protected final SearchTask task;
    private final int expectedSuccessfulOps;
    private final int expectedTotalOps;
    private final AtomicInteger successfulOps = new AtomicInteger();
    private final AtomicInteger totalOps = new AtomicInteger();
    private final AtomicArray<FirstResult> initialResults;
    private final Map<String, AliasFilter> aliasFilter;
    private final Map<String, Float> concreteIndexBoosts;
    private final long clusterStateVersion;
    private volatile AtomicArray<ShardSearchFailure> shardFailures;
    private final Object shardFailuresMutex = new Object();

    protected AbstractSearchAsyncAction(Logger logger, SearchTransportService searchTransportService,
                                        Function<String, Transport.Connection> nodeIdToConnection,
                                        Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                        Executor executor, SearchRequest request, ActionListener<SearchResponse> listener,
                                        GroupShardsIterator shardsIts, long startTime, long clusterStateVersion, SearchTask task) {
        super(startTime);
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.request = request;
        this.task = task;
        this.listener = listener;
        this.nodeIdToConnection = nodeIdToConnection;
        this.clusterStateVersion = clusterStateVersion;
        this.shardsIts = shardsIts;
        expectedSuccessfulOps = shardsIts.size();
        // we need to add 1 for non active partition, since we count it in the total!
        expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        initialResults = new AtomicArray<>(shardsIts.size());
        this.aliasFilter = aliasFilter;
        this.concreteIndexBoosts = concreteIndexBoosts;
    }

    public void start() {
        if (expectedSuccessfulOps == 0) {
            //no search shards to search on, bail with empty response
            //(it happens with search across _all with no indices around and consistent with broadcast operations)
            listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, buildTookInMillis(),
                ShardSearchFailure.EMPTY_ARRAY));
            return;
        }
        int shardIndex = -1;
        for (final ShardIterator shardIt : shardsIts) {
            shardIndex++;
            final ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                performInitialPhase(shardIndex, shardIt, shard);
            } else {
                // really, no shards active in this group
                onInitialPhaseResult(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
            }
        }
    }

    void performInitialPhase(final int shardIndex, final ShardIterator shardIt, final ShardRouting shard) {
        if (shard == null) {
            // TODO upgrade this to an assert...
            // no more active shards... (we should not really get here, but just for safety)
            onInitialPhaseResult(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
        } else {
            try {
                final Transport.Connection connection = nodeIdToConnection.apply(shard.currentNodeId());
                AliasFilter filter = this.aliasFilter.get(shard.index().getUUID());
                assert filter != null;

                float indexBoost = concreteIndexBoosts.getOrDefault(shard.index().getUUID(), DEFAULT_INDEX_BOOST);
                ShardSearchTransportRequest transportRequest = new ShardSearchTransportRequest(request, shardIt.shardId(), shardsIts.size(),
                    filter, indexBoost, startTime());
                sendExecuteFirstPhase(connection, transportRequest, new ActionListener<FirstResult>() {
                    @Override
                    public void onResponse(FirstResult result) {
                        onInitialPhaseResult(shardIndex, shard.currentNodeId(), result, shardIt);
                    }

                    @Override
                    public void onFailure(Exception t) {
                        onInitialPhaseResult(shardIndex, shard, connection.getNode().getId(), shardIt, t);
                    }
                });
            } catch (ConnectTransportException | IllegalArgumentException ex) {
                // we are getting the connection early here so we might run into nodes that are not connected. in that case we move on to
                // the next shard. previously when using discovery nodes here we had a special case for null when a node was not connected
                // at all which is not not needed anymore.
                onInitialPhaseResult(shardIndex, shard, shard.currentNodeId(), shardIt, ex);
            }
        }
    }

    private void onInitialPhaseResult(int shardIndex, String nodeId, FirstResult result, ShardIterator shardIt) {
        result.shardTarget(new SearchShardTarget(nodeId, shardIt.shardId()));
        processFirstPhaseResult(shardIndex, result);
        // we need to increment successful ops first before we compare the exit condition otherwise if we
        // are fast we could concurrently update totalOps but then preempt one of the threads which can
        // cause the successor to read a wrong value from successfulOps if second phase is very fast ie. count etc.
        successfulOps.incrementAndGet();
        // increment all the "future" shards to update the total ops since we some may work and some may not...
        // and when that happens, we break on total ops, so we must maintain them
        final int xTotalOps = totalOps.addAndGet(shardIt.remaining() + 1);
        if (xTotalOps == expectedTotalOps) {
            try {
                innerStartNextPhase();
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "{}: Failed to execute [{}] while moving to second phase",
                            shardIt.shardId(),
                            request),
                        e);
                }
                raiseEarlyFailure(new ReduceSearchPhaseException(initialPhaseName(), "", e, buildShardFailures()));
            }
        } else if (xTotalOps > expectedTotalOps) {
            raiseEarlyFailure(new IllegalStateException("unexpected higher total ops [" + xTotalOps + "] compared " +
                "to expected [" + expectedTotalOps + "]"));
        }
    }

    private void onInitialPhaseResult(final int shardIndex, @Nullable ShardRouting shard, @Nullable String nodeId,
                                      final ShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        SearchShardTarget shardTarget = new SearchShardTarget(nodeId, shardIt.shardId());
        addShardFailure(shardIndex, shardTarget, e);

        if (totalOps.incrementAndGet() == expectedTotalOps) {
            if (logger.isDebugEnabled()) {
                if (e != null && !TransportActions.isShardNotAvailableException(e)) {
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "{}: Failed to execute [{}]",
                            shard != null ? shard.shortSummary() :
                                shardIt.shardId(),
                            request),
                        e);
                } else if (logger.isTraceEnabled()) {
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("{}: Failed to execute [{}]", shard, request), e);
                }
            }
            final ShardSearchFailure[] shardSearchFailures = buildShardFailures();
            if (successfulOps.get() == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("All shards failed for phase: [{}]", initialPhaseName()), e);
                }

                // no successful ops, raise an exception
                raiseEarlyFailure(new SearchPhaseExecutionException(initialPhaseName(), "all shards failed", e, shardSearchFailures));
            } else {
                try {
                    innerStartNextPhase();
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    raiseEarlyFailure(new ReduceSearchPhaseException(initialPhaseName(), "", inner, shardSearchFailures));
                }
            }
        } else {
            final ShardRouting nextShard = shardIt.nextOrNull();
            final boolean lastShard = nextShard == null;
            // trace log this exception
            logger.trace(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "{}: Failed to execute [{}] lastShard [{}]",
                    shard != null ? shard.shortSummary() : shardIt.shardId(),
                    request,
                    lastShard),
                e);
            if (!lastShard) {
                try {
                    performInitialPhase(shardIndex, shardIt, nextShard);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    onInitialPhaseResult(shardIndex, shard, shard.currentNodeId(), shardIt, inner);
                }
            } else {
                // no more shards active, add a failure
                if (logger.isDebugEnabled() && !logger.isTraceEnabled()) { // do not double log this exception
                    if (e != null && !TransportActions.isShardNotAvailableException(e)) {
                        logger.debug(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "{}: Failed to execute [{}] lastShard [{}]",
                                shard != null ? shard.shortSummary() :
                                    shardIt.shardId(),
                                request,
                                lastShard),
                            e);
                    }
                }
            }
        }
    }

    protected final ShardSearchFailure[] buildShardFailures() {
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures;
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<AtomicArray.Entry<ShardSearchFailure>> entries = shardFailures.asList();
        ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = entries.get(i).value;
        }
        return failures;
    }

    protected final void addShardFailure(final int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        // we don't aggregate shard failures on non active shards (but do keep the header counts right)
        if (TransportActions.isShardNotAvailableException(e)) {
            return;
        }

        // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
        if (shardFailures == null) {
            synchronized (shardFailuresMutex) {
                if (shardFailures == null) {
                    shardFailures = new AtomicArray<>(shardsIts.size());
                }
            }
        }
        ShardSearchFailure failure = shardFailures.get(shardIndex);
        if (failure == null) {
            shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
        } else {
            // the failure is already present, try and not override it with an exception that is less meaningless
            // for example, getting illegal shard state
            if (TransportActions.isReadOverrideException(e)) {
                shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
            }
        }
    }

    private void raiseEarlyFailure(Exception e) {
        for (AtomicArray.Entry<FirstResult> entry : initialResults.asList()) {
            try {
                Transport.Connection connection = nodeIdToConnection.apply(entry.value.shardTarget().getNodeId());
                sendReleaseSearchContext(entry.value.id(), connection);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.trace("failed to release context", inner);
            }
        }
        listener.onFailure(e);
    }

    protected void sendReleaseSearchContext(long contextId, Transport.Connection connection) {
        if (connection != null) {
            searchTransportService.sendFreeContext(connection, contextId, request);
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(QuerySearchResult queryResult, int index, IntArrayList entry,
                                                         ScoreDoc[] lastEmittedDocPerShard) {
        final ScoreDoc lastEmittedDoc = (lastEmittedDocPerShard != null) ? lastEmittedDocPerShard[index] : null;
        return new ShardFetchSearchRequest(request, queryResult.id(), entry, lastEmittedDoc);
    }

    protected abstract void sendExecuteFirstPhase(Transport.Connection connection, ShardSearchTransportRequest request,
                                                  ActionListener<FirstResult> listener);

    protected final void processFirstPhaseResult(int shardIndex, FirstResult result) {
        initialResults.set(shardIndex, result);

        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.shardTarget() : null);
        }

        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so its ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures;
        if (shardFailures != null) {
            shardFailures.set(shardIndex, null);
        }
    }

    final void innerStartNextPhase() throws Exception {
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            boolean hadOne = false;
            for (int i = 0; i < initialResults.length(); i++) {
                FirstResult result = initialResults.get(i);
                if (result == null) {
                    continue; // failure
                }
                if (hadOne) {
                    sb.append(",");
                } else {
                    hadOne = true;
                }
                sb.append(result.shardTarget());
            }

            logger.trace("Moving to second phase, based on results from: {} (cluster state version: {})", sb, clusterStateVersion);
        }
        executeNextPhase(initialResults);
    }

    protected abstract void executeNextPhase(AtomicArray<FirstResult> initialResults) throws Exception;

    protected abstract String initialPhaseName();

    protected Executor getExecutor() {
        return executor;
    }

    // this is a simple base class to simplify fan out to shards and collect
    final class CountedCollector<R extends SearchPhaseResult> {
        private final AtomicArray<R> resultArray;
        private final CountDown counter;
        private final IntConsumer onFinish;

        CountedCollector(AtomicArray<R> resultArray, int expectedOps, IntConsumer onFinish) {
            this.resultArray = resultArray;
            this.counter = new CountDown(expectedOps);
            this.onFinish = onFinish;
        }

        void countDown() {
            if (counter.countDown()) {
                onFinish.accept(successfulOps.get());
            }
        }

        void onResult(int index, R result, SearchShardTarget target) {
            try {
                result.shardTarget(target);
                resultArray.set(index, result);
            } finally {
                countDown();
            }
        }

        void onFailure(final int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
            try {
                addShardFailure(shardIndex, shardTarget, e);
            } finally {
                successfulOps.decrementAndGet();
                countDown();
            }
        }

    }

    /*
     *  At this point AbstractSearchAsyncAction is just a base-class for the first phase of a search where we have multiple replicas
     *  for each shardID. If one of them is not available we move to the next one. Yet, once we passed that first stage we have to work with
     *  the shards we succeeded on the initial phase.
     *  Unfortunately, subsequent phases are not fully detached from the initial phase since they are all non-static inner classes.
     *  In future changes this will be changed to detach the inner classes to test them in isolation and to simplify their creation.
     *  The AbstractSearchAsyncAction should be final and it should just get a factory for the next phase instead of requiring subclasses
     *  etc.
     */
    final class FetchPhase implements Runnable {
        private final AtomicArray<FetchSearchResult> fetchResults;
        private final SearchPhaseController searchPhaseController;
        private final AtomicArray<QuerySearchResultProvider> queryResults;

        public FetchPhase(AtomicArray<QuerySearchResultProvider> queryResults,
                           SearchPhaseController searchPhaseController) {
            this.fetchResults = new AtomicArray<>(queryResults.length());
            this.searchPhaseController = searchPhaseController;
            this.queryResults = queryResults;
        }

        @Override
        public void run() {
            try {
                final boolean isScrollRequest = request.scroll() != null;
                ScoreDoc[] sortedShardDocs = searchPhaseController.sortDocs(isScrollRequest, queryResults);
                final IntArrayList[] docIdsToLoad = searchPhaseController.fillDocIdsToLoad(queryResults.length(), sortedShardDocs);
                final IntConsumer finishPhase =  successOpts
                    -> sendResponseAsync(searchPhaseController, sortedShardDocs, queryResults, fetchResults);
                List<QuerySearchResult> resultToRelease = Collections.emptyList();
                try {
                    if (sortedShardDocs.length == 0) { // no docs to fetch -- sidestep everything and return
                        resultToRelease = queryResults.asList().stream().map(e -> e.value.queryResult()).collect(Collectors.toList());
                        finishPhase.accept(successfulOps.get());
                    } else {
                        resultToRelease = new ArrayList<>();
                        final ScoreDoc[] lastEmittedDocPerShard = isScrollRequest ?
                            searchPhaseController.getLastEmittedDocPerShard(queryResults.asList(), sortedShardDocs, queryResults.length())
                            : null;
                        final CountedCollector<FetchSearchResult> counter = new CountedCollector<>(fetchResults,
                            docIdsToLoad.length, // we count down every shard in the result no matter if we got any results or not
                            finishPhase);
                        for (int i = 0; i < docIdsToLoad.length; i++) {
                            IntArrayList entry = docIdsToLoad[i];
                            QuerySearchResultProvider queryResult = queryResults.get(i);
                            if (entry == null) { // no results for this shard ID
                                if (queryResult != null) {  // if we got some hits from this shard we have to release the context there
                                    resultToRelease.add(queryResult.queryResult());
                                }
                                // in any case we count down this result since we don't talk to this shard anymore
                                counter.countDown();
                            } else {
                                Transport.Connection connection = nodeIdToConnection.apply(queryResult.shardTarget().getNodeId());
                                ShardFetchSearchRequest fetchSearchRequest = createFetchRequest(queryResult.queryResult(), i, entry,
                                    lastEmittedDocPerShard);
                                executeFetch(i, queryResult.shardTarget(), counter, fetchSearchRequest, queryResult.queryResult(),
                                    connection);
                            }
                        }
                    }
                } finally {
                    final List<QuerySearchResult> finalResultToRelease = resultToRelease;
                    if (resultToRelease.isEmpty() == false) {
                        getExecutor().execute(() -> {
                            // now release all search contexts for the shards we don't fetch results for
                            for (QuerySearchResult toRelease : finalResultToRelease) {
                                releaseIrrelevantSearchContext(toRelease);
                            }
                        });
                    }
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to fetch results", e);
                }
                listener.onFailure(e);
            }
        }

        private void executeFetch(final int shardIndex, final SearchShardTarget shardTarget,
                                    final CountedCollector<FetchSearchResult> counter,
                                    final ShardFetchSearchRequest fetchSearchRequest, final QuerySearchResult querySearchResult,
                                    final Transport.Connection connection) {
            searchTransportService.sendExecuteFetch(connection, fetchSearchRequest, task, new ActionListener<FetchSearchResult>() {
                @Override
                public void onResponse(FetchSearchResult result) {
                    counter.onResult(shardIndex, result, shardTarget);
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute fetch phase",
                                fetchSearchRequest.id()), e);
                        }
                        counter.onFailure(shardIndex, shardTarget, e);
                    } finally {
                        // the search context might not be cleared on the node where the fetch was executed for example
                        // because the action was rejected by the thread pool. in this case we need to send a dedicated
                        // request to clear the search context.
                        releaseIrrelevantSearchContext(querySearchResult);
                    }
                }
            });
        }

        /**
         * Releases shard targets that are not used in the docsIdsToLoad.
         */
        private void releaseIrrelevantSearchContext(QuerySearchResult queryResult) {
            // we only release search context that we did not fetch from if we are not scrolling
            if (request.scroll() == null) {
                if (queryResult.hasHits()) { // but none of them made it to the global top docs
                    try {
                        Transport.Connection connection = nodeIdToConnection.apply(queryResult.shardTarget().getNodeId());
                        sendReleaseSearchContext(queryResult.id(), connection);
                    } catch (Exception e) {
                        logger.trace("failed to release context", e);
                    }
                }
            }
        }
    }

    /**
     * Sends back a result to the user. This method will create the sorted docs if they are null and will build the scrollID for the
     * response. Note: This method will send the response in a different thread depending on the executor.
     */
    final void sendResponseAsync(SearchPhaseController searchPhaseController, ScoreDoc[] sortedDocs,
                                  AtomicArray<? extends QuerySearchResultProvider> queryResultsArr,
                                  AtomicArray<? extends FetchSearchResultProvider> fetchResultsArr) {
        getExecutor().execute(new ActionRunnable<SearchResponse>(listener) {
            @Override
            public void doRun() throws IOException {
                final boolean isScrollRequest = request.scroll() != null;
                final ScoreDoc[] theScoreDocs = sortedDocs == null ? searchPhaseController.sortDocs(isScrollRequest, queryResultsArr)
                    : sortedDocs;
                final InternalSearchResponse internalResponse = searchPhaseController.merge(isScrollRequest, theScoreDocs, queryResultsArr,
                    fetchResultsArr);
                String scrollId = isScrollRequest ? TransportSearchHelper.buildScrollId(request.searchType(), queryResultsArr) : null;
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(),
                    buildTookInMillis(), buildShardFailures()));
            }

            @Override
            public void onFailure(Exception e) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("merge", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                super.onFailure(failure);
            }
        });
    }
}
