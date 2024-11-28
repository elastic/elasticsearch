/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.AbstractSearchAsyncAction.DEFAULT_INDEX_BOOST;
import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;
import static org.elasticsearch.core.Strings.format;

class SearchQueryThenFetchAsyncAction extends SearchPhase implements AsyncSearchContext {

    private final Logger logger;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final SearchTransportService searchTransportService;
    private final Executor executor;
    private final ActionListener<SearchResponse> listener;
    private final SearchRequest request;

    /**
     * Used by subclasses to resolve node ids to DiscoveryNodes.
     **/
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;
    private final SearchTask task;
    protected final SearchPhaseResults<SearchPhaseResult> results;
    private final long clusterStateVersion;
    private final TransportVersion minTransportVersion;
    private final Map<String, AliasFilter> aliasFilter;
    private final Map<String, Float> concreteIndexBoosts;
    private final SetOnce<AtomicArray<ShardSearchFailure>> shardFailures = new SetOnce<>();
    private final Object shardFailuresMutex = new Object();
    private final AtomicBoolean hasShardResponse = new AtomicBoolean(false);
    private final AtomicInteger successfulOps = new AtomicInteger();
    private final AtomicInteger skippedOps = new AtomicInteger();
    private final TransportSearchAction.SearchTimeProvider timeProvider;
    private final SearchResponse.Clusters clusters;

    protected final GroupShardsIterator<SearchShardIterator> toSkipShardsIts;
    protected final GroupShardsIterator<SearchShardIterator> shardsIts;
    private final SearchShardIterator[] shardIterators;
    private final Map<SearchShardIterator, Integer> shardIndexMap;
    private final int expectedTotalOps;
    private final AtomicInteger totalOps = new AtomicInteger();
    private final int maxConcurrentRequestsPerNode;
    private final Map<String, AbstractSearchAsyncAction.PendingExecutions> pendingExecutionsPerNode = new ConcurrentHashMap<>();
    private final boolean throttleConcurrentRequests;
    private final AtomicBoolean requestCancelled = new AtomicBoolean();

    // protected for tests
    protected final List<Releasable> releasables = new ArrayList<>();

    private final SearchProgressListener progressListener;

    // informations to track the best bottom top doc globally.
    private final int topDocsSize;
    private final int trackTotalHitsUpTo;
    private volatile BottomSortValuesCollector bottomSortCollector;
    private final Client client;

    SearchQueryThenFetchAsyncAction(
        Logger logger,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchPhaseResults<SearchPhaseResult> resultConsumer,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters,
        Client client
    ) {
        super("query");
        this.namedWriteableRegistry = namedWriteableRegistry;
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

        // we compute the shard index based on the natural order of the shards
        // that participate in the search request. This means that this number is
        // consistent between two requests that target the same shards.
        Map<SearchShardIterator, Integer> shardMap = new HashMap<>();
        List<SearchShardIterator> searchIterators = new ArrayList<>(iterators);
        CollectionUtil.timSort(searchIterators);
        for (int i = 0; i < searchIterators.size(); i++) {
            shardMap.put(searchIterators.get(i), i);
        }
        this.shardIndexMap = Collections.unmodifiableMap(shardMap);
        this.shardIterators = searchIterators.toArray(SearchShardIterator[]::new);

        // we need to add 1 for non active partition, since we count it in the total. This means for each shard in the iterator we sum up
        // it's number of active shards but use 1 as the default if no replica of a shard is active at this point.
        // on a per shards level we use shardIt.remaining() to increment the totalOps pointer but add 1 for the current shard result
        // we process hence we add one for the non active partition here.
        this.expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        this.maxConcurrentRequestsPerNode = request.getMaxConcurrentShardRequests();
        // in the case were we have less shards than maxConcurrentRequestsPerNode we don't need to throttle
        this.throttleConcurrentRequests = maxConcurrentRequestsPerNode < shardsIts.size();
        this.timeProvider = timeProvider;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.request = request;
        this.task = task;
        this.listener = ActionListener.runAfter(listener, () -> Releasables.close(releasables));
        this.nodeIdToConnection = nodeIdToConnection;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.clusterStateVersion = clusterState.version();
        this.minTransportVersion = clusterState.getMinTransportVersion();
        this.aliasFilter = aliasFilter;
        this.results = resultConsumer;
        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        releasables.add(resultConsumer);
        this.clusters = clusters;
        this.topDocsSize = getTopDocsSize(request);
        this.trackTotalHitsUpTo = request.resolveTrackTotalHitsUpTo();
        this.progressListener = task.getProgressListener();
        this.client = client;

        // don't build the SearchShard list (can be expensive) if the SearchProgressListener won't use it
        if (progressListener != SearchProgressListener.NOOP) {
            notifyListShards(progressListener, clusters, request.source());
        }
    }

    /**
     * This is the main entry point for a search. This method starts the search execution of the initial phase.
     */
    public final void start() {
        if (results.getNumShards() == 0) {
            // no search shards to search on, bail with empty response
            // (it happens with search across _all with no indices around and consistent with broadcast operations)
            int trackTotalHitsUpTo = request.source() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : request.source().trackTotalHitsUpTo() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : request.source().trackTotalHitsUpTo();
            // total hits is null in the response if the tracking of total hits is disabled
            boolean withTotalHits = trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
            sendSearchResponse(
                withTotalHits ? SearchResponseSections.EMPTY_WITH_TOTAL_HITS : SearchResponseSections.EMPTY_WITHOUT_TOTAL_HITS,
                new AtomicArray<>(0)
            );
            return;
        }
        executePhase(this);
    }

    @Override
    public SearchRequest getRequest() {
        return request;
    }

    /**
     * Builds and sends the final search response back to the user.
     *
     * @param internalSearchResponse the internal search response
     * @param queryResults           the results of the query phase
     */
    public void sendSearchResponse(SearchResponseSections internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        ShardSearchFailure[] failures = buildShardFailures();
        Boolean allowPartialResults = request.allowPartialSearchResults();
        assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (allowPartialResults == false && failures.length > 0) {
            raisePhaseFailure(new SearchPhaseExecutionException("", "Shard failures", null, failures));
        } else {
            final String scrollId = request.scroll() != null ? TransportSearchHelper.buildScrollId(queryResults) : null;
            final BytesReference searchContextId;
            if (request.source() != null
                && request.source().pointInTimeBuilder() != null
                && request.source().pointInTimeBuilder().singleSession() == false) {
                searchContextId = request.source().pointInTimeBuilder().getEncodedId();
            } else {
                searchContextId = null;
            }
            ActionListener.respondAndRelease(listener, buildSearchResponse(internalSearchResponse, failures, scrollId, searchContextId));
        }
    }

    @Override
    public SearchTransportService getSearchTransport() {
        return null;
    }

    @Override
    public SearchTask getTask() {
        return null;
    }

    private SearchResponse buildSearchResponse(
        SearchResponseSections internalSearchResponse,
        ShardSearchFailure[] failures,
        String scrollId,
        BytesReference searchContextId
    ) {
        int numSuccess = successfulOps.get();
        int numFailures = failures.length;
        assert numSuccess + numFailures == results.getNumShards()
            : "numSuccess(" + numSuccess + ") + numFailures(" + numFailures + ") != totalShards(" + results.getNumShards() + ")";
        return new SearchResponse(
            internalSearchResponse,
            scrollId,
            results.getNumShards(),
            numSuccess,
            skippedOps.get(),
            timeProvider.buildTookInMillis(),
            failures,
            clusters,
            searchContextId
        );
    }

    /**
     * Builds an request for the initial search phase.
     *
     * @param shardIt the target {@link SearchShardIterator}
     * @param shardIndex the index of the shard that is used in the coordinator node to
     *                   tiebreak results with identical sort values
     */
    protected final ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex) {
        AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
        assert filter != null;
        float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            shardIt.getOriginalIndices(),
            request,
            shardIt.shardId(),
            shardIndex,
            results.getNumShards(),
            filter,
            indexBoost,
            timeProvider.absoluteStartMillis(),
            shardIt.getClusterAlias(),
            shardIt.getSearchContextId(),
            shardIt.getSearchContextKeepAlive()
        );
        // if we already received a search result we can inform the shard that it
        // can return a null response if the request rewrites to match none rather
        // than creating an empty response in the search thread pool.
        // Note that, we have to disable this shortcut for queries that create a context (scroll and search context).
        shardRequest.canReturnNullResponseIfMatchNoDocs(hasShardResponse.get() && shardRequest.scroll() == null);
        return shardRequest;
    }

    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final Transport.Connection connection,
        final SearchActionListener<SearchPhaseResult> listener
    ) {
        ShardSearchRequest request = rewriteShardSearchRequest(buildShardSearchRequest(shardIt, listener.requestIndex));
        searchTransportService.sendExecuteQuery(connection, request, task, listener);
    }

    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }

    protected void onShardResult(SearchPhaseResult result, SearchShardIterator shardIt) {
        QuerySearchResult queryResult = result.queryResult();
        if (queryResult.isNull() == false
            // disable sort optims for scroll requests because they keep track of the last bottom doc locally (per shard)
            && request.scroll() == null
            // top docs are already consumed if the query was cancelled or in error.
            && queryResult.hasConsumedTopDocs() == false
            && queryResult.topDocs() != null
            && queryResult.topDocs().topDocs.getClass() == TopFieldDocs.class) {
            TopFieldDocs topDocs = (TopFieldDocs) queryResult.topDocs().topDocs;
            if (bottomSortCollector == null) {
                synchronized (this) {
                    if (bottomSortCollector == null) {
                        bottomSortCollector = new BottomSortValuesCollector(topDocsSize, topDocs.fields);
                    }
                }
            }
            bottomSortCollector.consumeTopDocs(topDocs, queryResult.sortValueFormats());
        }
        assert result.getShardIndex() != -1 : "shard index is not set";
        assert result.getSearchShardTarget() != null : "search shard target must not be null";
        hasShardResponse.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.getSearchShardTarget() : null);
        }
        results.consumeResult(result, () -> onShardResultConsumed(result, shardIt));
    }

    static SearchPhase nextPhase(
        Client client,
        AsyncSearchContext context,
        SearchPhaseResults<SearchPhaseResult> queryResults,
        AggregatedDfs aggregatedDfs
    ) {
        var rankFeaturePhaseCoordCtx = RankFeaturePhase.coordinatorContext(context.getRequest().source(), client);
        if (rankFeaturePhaseCoordCtx == null) {
            return new FetchSearchPhase(queryResults, aggregatedDfs, context, null);
        }
        return new RankFeaturePhase(queryResults, aggregatedDfs, context, rankFeaturePhaseCoordCtx);
    }

    protected SearchPhase getNextPhase() {
        return nextPhase(client, this, results, null);
    }

    public OriginalIndices getOriginalIndices(int shardIndex) {
        return shardIterators[shardIndex].getOriginalIndices();
    }

    private ShardSearchRequest rewriteShardSearchRequest(ShardSearchRequest request) {
        if (bottomSortCollector == null) {
            return request;
        }

        // disable tracking total hits if we already reached the required estimation.
        if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_ACCURATE && bottomSortCollector.getTotalHits() > trackTotalHitsUpTo) {
            request.source(request.source().shallowCopy().trackTotalHits(false));
        }

        // set the current best bottom field doc
        if (bottomSortCollector.getBottomSortValues() != null) {
            request.setBottomSortValues(bottomSortCollector.getBottomSortValues());
        }
        return request;
    }

    @Override
    public void run() throws IOException {
        for (final SearchShardIterator iterator : toSkipShardsIts) {
            assert iterator.skip();
            skipShard(iterator);
        }
        if (shardsIts.size() > 0) {
            doCheckNoMissingShards(getName(), request, shardsIts);
            for (int i = 0; i < shardsIts.size(); i++) {
                final SearchShardIterator shardRoutings = shardsIts.get(i);
                assert shardRoutings.skip() == false;
                assert shardIndexMap.containsKey(shardRoutings);
                int shardIndex = shardIndexMap.get(shardRoutings);
                final SearchShardTarget routing = shardRoutings.nextOrNull();
                if (routing == null) {
                    failOnUnavailable(shardIndex, shardRoutings);
                } else {
                    performPhaseOnShard(shardIndex, shardRoutings, routing);
                }
            }
        }
    }

    private void failOnUnavailable(int shardIndex, SearchShardIterator shardIt) {
        SearchShardTarget unassignedShard = new SearchShardTarget(null, shardIt.shardId(), shardIt.getClusterAlias());
        onShardFailure(shardIndex, unassignedShard, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
    }

    protected void notifyListShards(
        SearchProgressListener progressListener,
        SearchResponse.Clusters clusters,
        SearchSourceBuilder sourceBuilder
    ) {
        progressListener.notifyListShards(
            SearchProgressListener.buildSearchShards(this.shardsIts),
            SearchProgressListener.buildSearchShards(toSkipShardsIts),
            clusters,
            sourceBuilder == null || sourceBuilder.size() > 0,
            timeProvider
        );
    }

    protected void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final SearchShardTarget shard) {
        if (throttleConcurrentRequests) {
            var pendingExecutions = pendingExecutionsPerNode.computeIfAbsent(
                shard.getNodeId(),
                n -> new AbstractSearchAsyncAction.PendingExecutions(maxConcurrentRequestsPerNode)
            );
            pendingExecutions.submit(l -> doPerformPhaseOnShard(shardIndex, shardIt, shard, l));
        } else {
            doPerformPhaseOnShard(shardIndex, shardIt, shard, () -> {});
        }
    }

    private void doPerformPhaseOnShard(int shardIndex, SearchShardIterator shardIt, SearchShardTarget shard, Releasable releasable) {
        var shardListener = new SearchActionListener<>(shard, shardIndex) {
            @Override
            public void innerOnResponse(SearchPhaseResult result) {
                try {
                    releasable.close();
                    onShardResult(result, shardIt);
                } catch (Exception exc) {
                    onShardFailure(shardIndex, shard, shardIt, exc);
                }
            }

            @Override
            public void onFailure(Exception e) {
                releasable.close();
                onShardFailure(shardIndex, shard, shardIt, e);
            }
        };
        final Transport.Connection connection;
        try {
            connection = getConnection(shard.getClusterAlias(), shard.getNodeId());
        } catch (Exception e) {
            shardListener.onFailure(e);
            return;
        }
        executePhaseOnShard(shardIt, connection, shardListener);
    }

    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return nodeIdToConnection.apply(clusterAlias, nodeId);
    }

    private void onShardFailure(final int shardIndex, SearchShardTarget shard, final SearchShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        onShardFailure(shardIndex, shard, e);
        final SearchShardTarget nextShard = shardIt.nextOrNull();
        final boolean lastShard = nextShard == null;
        logger.debug(() -> format("%s: Failed to execute [%s] lastShard [%s]", shard, request, lastShard), e);
        if (lastShard) {
            if (request.allowPartialSearchResults() == false) {
                if (requestCancelled.compareAndSet(false, true)) {
                    try {
                        searchTransportService.cancelSearchTask(task, "partial results are not allowed and at least one shard has failed");
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
            throw new AssertionError(
                "unexpected higher total ops [" + totalOps + "] compared to expected [" + expectedTotalOps + "]",
                new SearchPhaseExecutionException(getName(), "Shard failures", null, buildShardFailures())
            );
        } else {
            if (lastShard == false) {
                performPhaseOnShard(shardIndex, shardIt, nextShard);
            }
        }
    }

    /**
     * Executed once for every failed shard level request. This method is invoked before the next replica is tried for the given
     * shard target.
     * @param shardIndex the internal index for this shard. Each shard has an index / ordinal assigned that is used to reference
     *                   it's results
     * @param shardTarget the shard target for this failure
     * @param e the failure reason
     */
    public void onShardFailure(final int shardIndex, SearchShardTarget shardTarget, Exception e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            // Groups shard not available exceptions under a generic exception that returns a SERVICE_UNAVAILABLE(503)
            // temporary error.
            e = NoShardAvailableActionException.forOnShardFailureWrapper(e.getMessage());
        }
        // we don't aggregate shard on failures due to the internal cancellation,
        // but do keep the header counts right
        if ((requestCancelled.get() && AbstractSearchAsyncAction.isTaskCancelledException(e)) == false) {
            AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
            // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
            if (shardFailures == null) { // this is double checked locking but it's fine since SetOnce uses a volatile read internally
                synchronized (shardFailuresMutex) {
                    shardFailures = this.shardFailures.get(); // read again otherwise somebody else has created it?
                    if (shardFailures == null) { // still null so we are the first and create a new instance
                        shardFailures = new AtomicArray<>(results.getNumShards());
                        this.shardFailures.set(shardFailures);
                    }
                }
            }
            ShardSearchFailure failure = shardFailures.get(shardIndex);
            if (failure == null) {
                shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
            } else {
                // the failure is already present, try and not override it with an exception that is less meaningless
                // for example, getting illegal shard state
                if (TransportActions.isReadOverrideException(e) && (e instanceof SearchContextMissingException == false)) {
                    shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
                }
            }

            if (results.hasResult(shardIndex)) {
                assert failure == null : "shard failed before but shouldn't: " + failure;
                successfulOps.decrementAndGet(); // if this shard was successful before (initial phase) we have to adjust the counter
            }
        }
        results.consumeShardFailure(shardIndex);
    }

    void skipShard(SearchShardIterator iterator) {
        successfulOps.incrementAndGet();
        skippedOps.incrementAndGet();
        assert iterator.skip();
        successfulShardExecution(iterator);
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
            throw new AssertionError(
                "unexpected higher total ops [" + xTotalOps + "] compared to expected [" + expectedTotalOps + "]",
                new SearchPhaseExecutionException(getName(), "Shard failures", null, buildShardFailures())
            );
        }
    }

    final void onPhaseDone() {  // as a tribute to @kimchy aka. finishHim()
        executeNextPhase(this, this::getNextPhase);
    }

    protected void executeNextPhase(SearchPhase currentPhase, Supplier<SearchPhase> nextPhaseSupplier) {
        /* This is the main search phase transition where we move to the next phase. If all shards
         * failed or if there was a failure and partial results are not allowed, then we immediately
         * fail. Otherwise we continue to the next phase.
         */
        ShardOperationFailedException[] shardSearchFailures = buildShardFailures();
        if (shardSearchFailures.length == results.getNumShards()) {
            shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
            Throwable cause = shardSearchFailures.length == 0
                ? null
                : ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
            logger.debug(() -> "All shards failed for phase: [" + currentPhase.getName() + "]", cause);
            onPhaseFailure(currentPhase, "all shards failed", cause);
        } else {
            Boolean allowPartialResults = request.allowPartialSearchResults();
            assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (allowPartialResults == false && successfulOps.get() != results.getNumShards()) {
                // check if there are actual failures in the atomic array since
                // successful retries can reset the failures to null
                if (shardSearchFailures.length > 0) {
                    if (logger.isDebugEnabled()) {
                        int numShardFailures = shardSearchFailures.length;
                        shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
                        Throwable cause = ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
                        logger.debug(() -> format("%s shards failed for phase: [%s]", numShardFailures, currentPhase.getName()), cause);
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure", null);
                } else {
                    int discrepancy = results.getNumShards() - successfulOps.get();
                    assert discrepancy > 0 : "discrepancy: " + discrepancy;
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Partial shards failure (unavailable: {}, successful: {}, skipped: {}, num-shards: {}, phase: {})",
                            discrepancy,
                            successfulOps.get(),
                            skippedOps.get(),
                            results.getNumShards(),
                            currentPhase.getName()
                        );
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure (" + discrepancy + " shards unavailable)", null);
                }
                return;
            }
            var nextPhase = nextPhaseSupplier.get();
            if (logger.isTraceEnabled()) {
                final String resultsFrom = results.getSuccessfulResults()
                    .map(r -> r.getSearchShardTarget().toString())
                    .collect(Collectors.joining(","));
                logger.trace(
                    "[{}] Moving to next phase: [{}], based on results from: {} (cluster state version: {})",
                    currentPhase.getName(),
                    nextPhase.getName(),
                    resultsFrom,
                    clusterStateVersion
                );
            }
            executePhase(nextPhase);
        }
    }

    private void executePhase(SearchPhase phase) {
        try {
            phase.run();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> format("Failed to execute [%s] while moving to [%s] phase", request, phase.getName()), e);
            }
            onPhaseFailure(phase, "", e);
        }
    }

    /**
     * This method will communicate a fatal phase failure back to the user. In contrast to a shard failure
     * will this method immediately fail the search request and return the failure to the issuer of the request
     * @param phase the phase that failed
     * @param msg an optional message
     * @param cause the cause of the phase failure
     */
    public void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        raisePhaseFailure(new SearchPhaseExecutionException(phase.getName(), msg, cause, buildShardFailures()));
    }

    @Override
    public void addReleasable(Releasable releasable) {
        releasables.add(releasable);
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    /**
     * This method should be called if a search phase failed to ensure all relevant reader contexts are released.
     * This method will also notify the listener and sends back a failure to the user.
     *
     * @param exception the exception explaining or causing the phase failure
     */
    private void raisePhaseFailure(SearchPhaseExecutionException exception) {
        results.getSuccessfulResults().forEach((entry) -> {
            // Do not release search contexts that are part of the point in time
            if (entry.getContextId() != null && isPartOfPointInTime(entry.getContextId()) == false) {
                try {
                    SearchShardTarget searchShardTarget = entry.getSearchShardTarget();
                    Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
                    sendReleaseSearchContext(entry.getContextId(), connection, getOriginalIndices(entry.getShardIndex()));
                } catch (Exception inner) {
                    inner.addSuppressed(exception);
                    logger.trace("failed to release context", inner);
                }
            }
        });
        listener.onFailure(exception);
    }

    void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection, OriginalIndices originalIndices) {
        assert isPartOfPointInTime(contextId) == false : "Must not release point in time context [" + contextId + "]";
        if (connection != null) {
            searchTransportService.sendFreeContext(connection, contextId, originalIndices);
        }
    }

    public boolean isPartOfPointInTime(ShardSearchContextId contextId) {
        final PointInTimeBuilder pointInTimeBuilder = request.pointInTimeBuilder();
        if (pointInTimeBuilder != null) {
            return request.pointInTimeBuilder().getSearchContextId(namedWriteableRegistry).contains(contextId);
        } else {
            return false;
        }
    }

    private ShardSearchFailure[] buildShardFailures() {
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<ShardSearchFailure> entries = shardFailures.asList();
        ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = entries.get(i);
        }
        return failures;
    }

    private void onShardResultConsumed(SearchPhaseResult result, SearchShardIterator shardIt) {
        successfulOps.incrementAndGet();
        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so its ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
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
}
