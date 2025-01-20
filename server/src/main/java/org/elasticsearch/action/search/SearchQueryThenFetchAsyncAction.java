/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.AbstractSearchAsyncAction.DEFAULT_INDEX_BOOST;
import static org.elasticsearch.action.search.AsyncSearchContext.buildShardFailures;
import static org.elasticsearch.action.search.SearchPhase.doCheckNoMissingShards;
import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;
import static org.elasticsearch.core.Strings.format;

public class SearchQueryThenFetchAsyncAction implements AsyncSearchContext {

    private static final String NAME = "query";

    private static final Logger logger = LogManager.getLogger(SearchQueryThenFetchAsyncAction.class);

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
    private final TransportVersion minNodeVersion;
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
    private final AtomicBoolean requestCancelled = new AtomicBoolean();

    private final Set<Integer> outstandingShards = ConcurrentCollections.newConcurrentSet();

    // protected for tests
    protected final List<Releasable> releasables = new ArrayList<>();

    private final SearchProgressListener progressListener;

    // informations to track the best bottom top doc globally.
    private final int topDocsSize;
    private final int trackTotalHitsUpTo;
    private volatile BottomSortValuesCollector bottomSortCollector;
    private final Client client;

    SearchQueryThenFetchAsyncAction(
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

        this.shardIterators = iterators.toArray(new SearchShardIterator[0]);
        // we compute the shard index based on the natural order of the shards
        // that participate in the search request. This means that this number is
        // consistent between two requests that target the same shards.
        Arrays.sort(shardIterators);
        this.timeProvider = timeProvider;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.request = request;
        this.task = task;
        this.listener = ActionListener.runAfter(listener, () -> Releasables.close(releasables));
        this.nodeIdToConnection = nodeIdToConnection;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.minNodeVersion = clusterState.getMinTransportVersion();
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
            var source = request.source();
            int trackTotalHitsUpTo = source == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : source.trackTotalHitsUpTo() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : source.trackTotalHitsUpTo();
            // total hits is null in the response if the tracking of total hits is disabled
            boolean withTotalHits = trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
            sendSearchResponse(
                withTotalHits ? SearchResponseSections.EMPTY_WITH_TOTAL_HITS : SearchResponseSections.EMPTY_WITHOUT_TOTAL_HITS,
                new AtomicArray<>(0)
            );
            return;
        }
        try {
            run();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> format("Failed to execute [%s] while moving to [" + NAME + "] phase", request), e);
            }
            onPhaseFailure(NAME, "", e);
        }
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
        ShardSearchFailure[] failures = buildShardFailures(shardFailures);
        Boolean allowPartialResults = request.allowPartialSearchResults();
        assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (allowPartialResults == false && failures.length > 0) {
            raisePhaseFailure(new SearchPhaseExecutionException("", "Shard failures", null, failures));
        } else {
            final String scrollId = request.scroll() != null ? TransportSearchHelper.buildScrollId(queryResults) : null;
            var source = request.source();
            final BytesReference searchContextId;
            if (source != null && source.pointInTimeBuilder() != null && source.pointInTimeBuilder().singleSession() == false) {
                searchContextId = source.pointInTimeBuilder().getEncodedId();
            } else {
                searchContextId = null;
            }
            ActionListener.respondAndRelease(listener, buildSearchResponse(internalSearchResponse, failures, scrollId, searchContextId));
        }
    }

    @Override
    public SearchTransportService getSearchTransport() {
        return searchTransportService;
    }

    @Override
    public SearchTask getTask() {
        return task;
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
     * @param shardIndex the index of the shard that is used in the coordinator node to
     *                   tiebreak results with identical sort values
     */
    private static ShardSearchRequest buildShardSearchRequest(
        ShardId shardId,
        String clusterAlias,
        int shardIndex,
        ShardSearchContextId searchContextId,
        OriginalIndices originalIndices,
        AliasFilter aliasFilter,
        TimeValue searchContextKeepAlive,
        float indexBoost,
        SearchRequest searchRequest,
        int totalShardCount,
        long absoluteStartMillis,
        boolean hasResponse
    ) {
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            originalIndices,
            searchRequest,
            shardId,
            shardIndex,
            totalShardCount,
            aliasFilter,
            indexBoost,
            absoluteStartMillis,
            clusterAlias,
            searchContextId,
            searchContextKeepAlive
        );
        // if we already received a search result we can inform the shard that it
        // can return a null response if the request rewrites to match none rather
        // than creating an empty response in the search thread pool.
        // Note that, we have to disable this shortcut for queries that create a context (scroll and search context).
        shardRequest.canReturnNullResponseIfMatchNoDocs(hasResponse && shardRequest.scroll() == null);
        return shardRequest;
    }

    public static class NodeQueryResponse extends TransportResponse {

        private final RefCounted refCounted = LeakTracker.wrap(new SimpleRefCounted());

        private final Object[] results;
        private final SearchPhaseController.TopDocsStats topDocsStats;
        private final QueryPhaseResultConsumer.MergeResult mergeResult;

        NodeQueryResponse(StreamInput in) throws IOException {
            super(in);
            this.results = in.readArray(i -> i.readBoolean() ? new QuerySearchResult(i) : i.readException(), Object[]::new);
            this.mergeResult = QueryPhaseResultConsumer.MergeResult.readFrom(in);
            this.topDocsStats = SearchPhaseController.TopDocsStats.readFrom(in);
        }

        NodeQueryResponse(
            QueryPhaseResultConsumer.MergeResult mergeResult,
            Object[] results,
            SearchPhaseController.TopDocsStats topDocsStats
        ) {
            this.results = results;
            for (int i = 0; i < results.length; i++) {
                if (results[i] instanceof RefCounted r) {
                    r.incRef();
                }
            }
            this.mergeResult = mergeResult;
            this.topDocsStats = topDocsStats;
            assert Arrays.stream(results).noneMatch(Objects::isNull) : Arrays.toString(results);
        }

        // public for tests
        public Object[] getResults() {
            return results;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray((o, v) -> {
                if (v instanceof Exception e) {
                    o.writeBoolean(false);
                    o.writeException(e);
                } else {
                    o.writeBoolean(true);
                    assert v instanceof QuerySearchResult : v;
                    ((QuerySearchResult) v).writeTo(o);
                }
            }, results);
            mergeResult.writeTo(out);
            topDocsStats.writeTo(out);
        }

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }

        @Override
        public boolean decRef() {
            if (refCounted.decRef()) {
                for (int i = 0; i < results.length; i++) {
                    Object result = results[i];
                    if (result instanceof RefCounted r) {
                        r.decRef();
                    }
                    results[i] = null;
                }
                return true;
            }
            return false;
        }
    }

    public static class NodeQueryRequest extends TransportRequest implements IndicesRequest {
        private final List<ShardToQuery> shards;
        private final SearchRequest searchRequest;
        private final Map<String, AliasFilter> aliasFilters;
        private final int totalShards;
        private final long absoluteStartMillis;

        private NodeQueryRequest(
            List<ShardToQuery> shards,
            SearchRequest searchRequest,
            Map<String, AliasFilter> aliasFilters,
            int totalShards,
            long absoluteStartMillis
        ) {
            this.shards = shards;
            this.searchRequest = searchRequest;
            this.aliasFilters = aliasFilters;
            this.totalShards = totalShards;
            this.absoluteStartMillis = absoluteStartMillis;
        }

        private NodeQueryRequest(StreamInput in) throws IOException {
            super(in);
            this.shards = in.readCollectionAsImmutableList(ShardToQuery::readFrom);
            this.searchRequest = new SearchRequest(in);
            this.aliasFilters = in.readImmutableMap(AliasFilter::readFrom);
            this.totalShards = in.readVInt();
            this.absoluteStartMillis = in.readLong();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new SearchShardTask(id, type, action, "NodeQueryRequest", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(shards);
            searchRequest.writeTo(out);
            out.writeMap(aliasFilters, (o, v) -> v.writeTo(o));
            out.writeVInt(totalShards);
            out.writeLong(absoluteStartMillis);
        }

        @Override
        public String[] indices() {
            return shards.stream().map(s -> s.originalIndices().indices()).flatMap(Arrays::stream).distinct().toArray(String[]::new);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return shards.getFirst().originalIndices.indicesOptions();
        }
    }

    private record ShardToQuery(
        float boost,
        OriginalIndices originalIndices,
        int shardIndex,
        ShardId shardId,
        ShardSearchContextId contextId
    ) implements Writeable {

        static ShardToQuery readFrom(StreamInput in) throws IOException {
            return new ShardToQuery(
                in.readFloat(),
                OriginalIndices.readOriginalIndices(in),
                in.readVInt(),
                new ShardId(in),
                in.readOptionalWriteable(ShardSearchContextId::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeFloat(boost);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
            out.writeVInt(shardIndex);
            shardId.writeTo(out);
            out.writeOptionalWriteable(contextId);
        }
    }

    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }

    protected void onShardResult(SearchPhaseResult result) {
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
        results.consumeResult(result, () -> onShardResultConsumed(result));
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

    @Override
    public OriginalIndices getOriginalIndices(int shardIndex) {
        return shardIterators[shardIndex].getOriginalIndices();
    }

    private static ShardSearchRequest rewriteShardSearchRequest(
        BottomSortValuesCollector bottomSortCollector,
        int trackTotalHitsUpTo,
        ShardSearchRequest request
    ) {
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

    private void run() {
        // TODO: stupid but we kinda need to fill all of these in with the current logic, do something nicer before merging
        final Map<SearchShardIterator, Integer> shardIndexMap = Maps.newHashMapWithExpectedSize(shardIterators.length);
        for (int i = 0; i < shardIterators.length; i++) {
            var iterator = shardIterators[i];
            shardIndexMap.put(iterator, i);
            outstandingShards.add(i);
        }
        for (final SearchShardIterator iterator : toSkipShardsIts) {
            assert iterator.skip();
            skipShard(iterator);
        }
        if (shardsIts.size() == 0) {
            finishIfAllDone();
            return;
        }
        final boolean supportsBatchedQuery = minNodeVersion.onOrAfter(TransportVersions.BATCHED_QUERY_PHASE_VERSION);
        final Map<String, NodeQueryRequest> perNodeQueries = new HashMap<>();
        doCheckNoMissingShards(NAME, request, shardsIts, SearchPhase::makeMissingShardsError);
        final String localClusterAlias = request.getLocalClusterAlias();
        for (int i = 0; i < shardsIts.size(); i++) {
            final SearchShardIterator shardRoutings = shardsIts.get(i);
            assert shardRoutings.skip() == false;
            assert shardIndexMap.containsKey(shardRoutings);
            int shardIndex = shardIndexMap.get(shardRoutings);
            final SearchShardTarget routing = shardRoutings.nextOrNull();
            if (routing == null) {
                failOnUnavailable(shardIndex, shardRoutings);
            } else {
                String clusterAlias = routing.getClusterAlias();
                if (supportsBatchedQuery && (clusterAlias == null || Objects.equals(localClusterAlias, clusterAlias))) {
                    perNodeQueries.computeIfAbsent(
                        routing.getNodeId(),
                        ignored -> new NodeQueryRequest(
                            new ArrayList<>(),
                            request,
                            aliasFilter,
                            shardsIts.size(),
                            timeProvider.absoluteStartMillis()
                        )
                    ).shards.add(
                        new ShardToQuery(
                            concreteIndexBoosts.getOrDefault(routing.getShardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST),
                            getOriginalIndices(shardIndex),
                            shardIndex,
                            routing.getShardId(),
                            shardRoutings.getSearchContextId()
                        )
                    );
                } else {
                    performPhaseOnShard(shardIndex, shardRoutings, routing);
                }
            }
        }
        perNodeQueries.forEach((nodeId, request) -> {
            if (request.shards.size() == 1) {
                var shard = request.shards.getFirst();
                final int sidx = shard.shardIndex;
                this.performPhaseOnShard(sidx, shardIterators[sidx], new SearchShardTarget(nodeId, shard.shardId, localClusterAlias));
                return;
            }
            final Transport.Connection connection;
            try {
                connection = getConnection(localClusterAlias, nodeId);
            } catch (Exception e) {
                onNodeQueryFailure(e, request, nodeId);
                return;
            }
            searchTransportService.transportService()
                .sendChildRequest(connection, NODE_SEARCH_ACTION_NAME, request, task, new TransportResponseHandler<NodeQueryResponse>() {
                    @Override
                    public NodeQueryResponse read(StreamInput in) throws IOException {
                        return new NodeQueryResponse(in);
                    }

                    @Override
                    public Executor executor() {
                        return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                    }

                    @Override
                    public void handleResponse(NodeQueryResponse response) {
                        if (results instanceof QueryPhaseResultConsumer queryPhaseResultConsumer) {
                            queryPhaseResultConsumer.addPartialResult(response.topDocsStats, response.mergeResult);
                        }
                        for (int i = 0; i < response.results.length; i++) {
                            var s = request.shards.get(i);
                            int shardIdx = s.shardIndex;
                            final SearchShardTarget target = new SearchShardTarget(nodeId, s.shardId, localClusterAlias);
                            switch (response.results[i]) {
                                case Exception e -> onShardFailure(shardIdx, target, shardIterators[shardIdx], e);
                                case SearchPhaseResult q -> {
                                    q.setShardIndex(shardIdx);
                                    q.setSearchShardTarget(target);
                                    onShardResult(q);
                                }
                                case null, default -> {
                                    assert false : "impossible [" + response.results[i] + "]";
                                }
                            }
                        }
                    }

                    @Override
                    public void handleException(TransportException e) {
                        Exception cause = (Exception) ExceptionsHelper.unwrapCause(e);
                        if (results instanceof QueryPhaseResultConsumer queryPhaseResultConsumer) {
                            queryPhaseResultConsumer.failure.compareAndSet(null, cause);
                        }
                        onPhaseFailure(NAME, "", cause);
                    }
                });
        });
    }

    private void onNodeQueryFailure(Exception e, NodeQueryRequest request, String nodeId) {
        for (ShardToQuery shard : request.shards) {
            int idx = shard.shardIndex;
            onShardFailure(
                idx,
                new SearchShardTarget(nodeId, shard.shardId, request.searchRequest.getLocalClusterAlias()),
                shardIterators[idx],
                e
            );
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
        final Transport.Connection connection;
        try {
            connection = getConnection(shard.getClusterAlias(), shard.getNodeId());
        } catch (Exception e) {
            onShardFailure(shardIndex, shard, shardIt, e);
            return;
        }
        final String indexUUID = shardIt.shardId().getIndex().getUUID();
        searchTransportService.sendExecuteQuery(
            connection,
            rewriteShardSearchRequest(
                bottomSortCollector,
                trackTotalHitsUpTo,
                buildShardSearchRequest(
                    shardIt.shardId(),
                    shardIt.getClusterAlias(),
                    shardIndex,
                    shardIt.getSearchContextId(),
                    shardIt.getOriginalIndices(),
                    aliasFilter.getOrDefault(indexUUID, AliasFilter.EMPTY),
                    shardIt.getSearchContextKeepAlive(),
                    concreteIndexBoosts.getOrDefault(indexUUID, DEFAULT_INDEX_BOOST),
                    request,
                    results.getNumShards(),
                    timeProvider.absoluteStartMillis(),
                    hasShardResponse.get()
                )
            ),
            task,
            new SearchActionListener<>(shard, shardIndex) {
                @Override
                public void innerOnResponse(SearchPhaseResult result) {
                    try {
                        onShardResult(result);
                    } catch (Exception exc) {
                        // TODO: this looks like a nasty bug where it to actually happen
                        assert false : exc;
                        onShardFailure(shardIndex, shard, shardIt, exc);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    onShardFailure(shardIndex, shard, shardIt, e);
                }
            }
        );
    }

    @Override
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
                        searchTransportService.cancelSearchTask(
                            task.getId(),
                            "partial results are not allowed and at least one shard has failed"
                        );
                    } catch (Exception cancelFailure) {
                        logger.debug("Failed to cancel search request", cancelFailure);
                    }
                }
            }
            onShardGroupFailure(shardIndex, shard, e);
            finishShardAndMaybePhase(shardIndex);
        } else {
            performPhaseOnShard(shardIndex, shardIt, nextShard);
        }
    }

    private void finishShardAndMaybePhase(int shardIndex) {
        boolean removed = outstandingShards.remove(shardIndex);
        var shardId = shardIterators[shardIndex].shardId();
        assert removed
            : "unknown shardId "
                + "["
                + shardId
                + "] ["
                + shardId.getIndex().getUUID()
                + "]["
                + System.identityHashCode(SearchQueryThenFetchAsyncAction.this)
                + "]";
        finishIfAllDone();
    }

    private final AtomicBoolean done = new AtomicBoolean(false);

    private void finishIfAllDone() {
        if (outstandingShards.isEmpty() && done.compareAndSet(false, true)) {
            executeNextPhase(NAME, this::getNextPhase);
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
                assert done.get() : "should only be called by subsequent phases, not during query";
                assert failure == null : "shard failed before but shouldn't: " + failure;
                successfulOps.decrementAndGet(); // if this shard was successful before (initial phase) we have to adjust the counter
            }
        }
    }

    void skipShard(SearchShardIterator iterator) {
        successfulOps.incrementAndGet();
        skippedOps.incrementAndGet();
        assert iterator.skip();
    }

    @Override
    public void executeNextPhase(String currentPhase, Supplier<SearchPhase> nextPhaseSupplier) {
        /* This is the main search phase transition where we move to the next phase. If all shards
         * failed or if there was a failure and partial results are not allowed, then we immediately
         * fail. Otherwise we continue to the next phase.
         */
        ShardOperationFailedException[] shardSearchFailures = buildShardFailures(shardFailures);
        final int numShards = results.getNumShards();
        if (shardSearchFailures.length == numShards) {
            shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
            Throwable cause = shardSearchFailures.length == 0
                ? null
                : ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
            logger.debug(() -> "All shards failed for phase: [" + currentPhase + "]", cause);
            onPhaseFailure(currentPhase, "all shards failed", cause);
        } else {
            Boolean allowPartialResults = request.allowPartialSearchResults();
            assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (allowPartialResults == false && successfulOps.get() != numShards) {
                // check if there are actual failures in the atomic array since
                // successful retries can reset the failures to null
                if (shardSearchFailures.length > 0) {
                    if (logger.isDebugEnabled()) {
                        int numShardFailures = shardSearchFailures.length;
                        shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
                        Throwable cause = ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
                        logger.debug(() -> format("%s shards failed for phase: [%s]", numShardFailures, currentPhase), cause);
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure", null);
                } else {
                    int discrepancy = numShards - successfulOps.get();
                    assert discrepancy > 0 : "discrepancy: " + discrepancy;
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Partial shards failure (unavailable: {}, successful: {}, skipped: {}, num-shards: {}, phase: {})",
                            discrepancy,
                            successfulOps.get(),
                            skippedOps.get(),
                            numShards,
                            currentPhase
                        );
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure (" + discrepancy + " shards unavailable)", null);
                }
                return;
            }
            var nextPhase = nextPhaseSupplier.get();
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "[{}] Moving to next phase: [{}], based on results from: {}",
                    currentPhase,
                    nextPhase.getName(),
                    results.getSuccessfulResults().map(r -> r.getSearchShardTarget().toString()).collect(Collectors.joining(","))
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
            onPhaseFailure(phase.getName(), "", e);
        }
    }

    /**
     * This method will communicate a fatal phase failure back to the user. In contrast to a shard failure
     * will this method immediately fail the search request and return the failure to the issuer of the request
     * @param phase the phase that failed
     * @param msg an optional message
     * @param cause the cause of the phase failure
     */
    @Override
    public void onPhaseFailure(String phase, String msg, Throwable cause) {
        raisePhaseFailure(new SearchPhaseExecutionException(phase, msg, cause, buildShardFailures(shardFailures)));
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
                    sendReleaseSearchContext(entry.getContextId(), connection);
                } catch (Exception inner) {
                    inner.addSuppressed(exception);
                    logger.trace("failed to release context", inner);
                }
            }
        });
        listener.onFailure(exception);
    }

    @Override
    public void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection) {
        assert isPartOfPointInTime(contextId) == false : "Must not release point in time context [" + contextId + "]";
        if (connection != null) {
            searchTransportService.sendFreeContext(connection, contextId, ActionListener.noop());
        }
    }

    public boolean isPartOfPointInTime(ShardSearchContextId contextId) {
        return AbstractSearchAsyncAction.isPartOfPIT(namedWriteableRegistry, request, contextId);
    }

    private void onShardResultConsumed(SearchPhaseResult result) {
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
        finishShardAndMaybePhase(result.getShardIndex());
    }

    public static final String NODE_SEARCH_ACTION_NAME = "indices:data/read/search[query][n]";

    private static final CircuitBreaker NOOP_CIRCUIT_BREAKER = new NoopCircuitBreaker("request");

    public static void registerNodeSearchAction(SearchTransportService searchTransportService, SearchService searchService) {
        var transportService = searchTransportService.transportService();
        final Dependencies dependencies = new Dependencies(
            searchService,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        final int searchPoolMax = transportService.getThreadPool().info(ThreadPool.Names.SEARCH).getMax();
        final SearchPhaseController searchPhaseController = new SearchPhaseController(searchService::aggReduceContextBuilder);
        transportService.registerRequestHandler(
            NODE_SEARCH_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            NodeQueryRequest::new,
            (request, channel, task) -> {
                final int shardCount = request.shards.size();
                int workers = Math.min(request.searchRequest.getMaxConcurrentShardRequests(), Math.min(shardCount, searchPoolMax));
                final var state = new QueryPerNodeState(
                    new AtomicInteger(workers - 1),
                    new QueryPhaseResultConsumer(
                        request.searchRequest,
                        dependencies.executor,
                        NOOP_CIRCUIT_BREAKER, // noop cb for now since we do not have a breaker in this situation in un-batched execution
                        searchPhaseController,
                        ((CancellableTask) task)::isCancelled,
                        SearchProgressListener.NOOP,
                        shardCount,
                        Integer.MAX_VALUE,
                        e -> logger.error("failed to merge on data node", e)
                    ),
                    request,
                    (CancellableTask) task,
                    channel,
                    dependencies
                );
                for (int i = 0; i < workers; i++) {
                    dependencies.executor.execute(shardTask(state, i));
                }
            }
        );

    }

    private static void maybeRelease(SearchService searchService, NodeQueryRequest request, SearchPhaseResult phaseResult) {
        if (phaseResult != null
            && phaseResult.hasSearchContext()
            && request.searchRequest.scroll() == null
            && (AbstractSearchAsyncAction.isPartOfPIT(null, request.searchRequest, phaseResult.getContextId()) == false)) {
            searchService.freeReaderContext(phaseResult.getContextId());
        }
    }

    private static AbstractRunnable shardTask(QueryPerNodeState state, int dataNodeLocalIdx) {
        return new AbstractRunnable() {
            @Override
            protected void doRun() {
                var request = state.searchRequest;
                var searchRequest = request.searchRequest;
                var pitBuilder = searchRequest.pointInTimeBuilder();
                var shardToQuery = request.shards.get(dataNodeLocalIdx);
                state.dependencies.searchService.executeQueryPhase(
                    rewriteShardSearchRequest(
                        state.bottomSortCollector,
                        state.trackTotalHitsUpTo,
                        buildShardSearchRequest(
                            shardToQuery.shardId,
                            searchRequest.getLocalClusterAlias(),
                            shardToQuery.shardIndex,
                            shardToQuery.contextId,
                            shardToQuery.originalIndices,
                            request.aliasFilters.getOrDefault(shardToQuery.shardId.getIndex().getUUID(), AliasFilter.EMPTY),
                            pitBuilder == null ? null : pitBuilder.getKeepAlive(),
                            shardToQuery.boost,
                            searchRequest,
                            request.totalShards,
                            request.absoluteStartMillis,
                            state.hasResponse.getAcquire()
                        )
                    ),
                    state.task,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(SearchPhaseResult searchPhaseResult) {
                            try {
                                searchPhaseResult.setShardIndex(dataNodeLocalIdx);
                                final SearchShardTarget target = new SearchShardTarget(
                                    null,
                                    shardToQuery.shardId,
                                    request.searchRequest.getLocalClusterAlias()
                                );
                                searchPhaseResult.setSearchShardTarget(target);
                                // no need for any cache effects when we're already flipped to ture => plain read + set-release
                                state.hasResponse.compareAndExchangeRelease(false, true);
                                state.consumeResult(searchPhaseResult.queryResult());
                                state.queryPhaseResultConsumer.consumeResult(searchPhaseResult, state.onDone);
                            } catch (Exception e) {
                                setFailure(state, dataNodeLocalIdx, e);
                            } finally {
                                maybeNext();
                            }
                        }

                        private void setFailure(QueryPerNodeState state, int dataNodeLocalIdx, Exception e) {
                            state.failures.put(dataNodeLocalIdx, e);
                            state.onDone.run();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                setFailure(state, dataNodeLocalIdx, e);
                                maybeNext();
                            } catch (Throwable expected) {
                                expected.addSuppressed(e);
                                throw new AssertionError(expected);
                            }
                        }

                        private void maybeNext() {
                            final int shardToQuery = state.currentShardIndex.incrementAndGet();
                            if (shardToQuery < request.shards.size()) {
                                state.dependencies.executor.execute(shardTask(state, shardToQuery));
                            }
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                // TODO this could be done better now, we probably should only make sure to have a single loop running at
                // minimum and ignore + requeue rejections in that case
                state.failures.put(dataNodeLocalIdx, e);
                state.onDone.run();
                // TODO SO risk!
                maybeNext();
            }

            @Override
            public void onRejection(Exception e) {
                // TODO this could be done better now, we probably should only make sure to have a single loop running at
                onFailure(e);
            }

            private void maybeNext() {
                final int shardToQuery = state.currentShardIndex.incrementAndGet();
                if (shardToQuery < state.searchRequest.shards.size()) {
                    state.dependencies.executor.execute(shardTask(state, shardToQuery));
                }
            }
        };
    }

    private record Dependencies(SearchService searchService, Executor executor) {}

    private static final class QueryPerNodeState {

        private static final QueryPhaseResultConsumer.MergeResult EMPTY_PARTIAL_MERGE_RESULT = new QueryPhaseResultConsumer.MergeResult(
            List.of(),
            Lucene.EMPTY_TOP_DOCS,
            null,
            0L
        );

        private final AtomicInteger currentShardIndex;
        private final QueryPhaseResultConsumer queryPhaseResultConsumer;
        private final NodeQueryRequest searchRequest;
        private final CancellableTask task;
        private final ConcurrentHashMap<Integer, Exception> failures = new ConcurrentHashMap<>();
        private final Dependencies dependencies;
        private final Runnable onDone;
        private final AtomicBoolean hasResponse = new AtomicBoolean(false);
        private final int trackTotalHitsUpTo;
        private final int topDocsSize;
        private volatile BottomSortValuesCollector bottomSortCollector;

        private QueryPerNodeState(
            AtomicInteger currentShardIndex,
            QueryPhaseResultConsumer queryPhaseResultConsumer,
            NodeQueryRequest searchRequest,
            CancellableTask task,
            TransportChannel channel,
            Dependencies dependencies
        ) {
            this.currentShardIndex = currentShardIndex;
            this.queryPhaseResultConsumer = queryPhaseResultConsumer;
            this.searchRequest = searchRequest;
            this.trackTotalHitsUpTo = searchRequest.searchRequest.resolveTrackTotalHitsUpTo();
            topDocsSize = getTopDocsSize(searchRequest.searchRequest);
            this.task = task;
            final int shardCount = queryPhaseResultConsumer.getNumShards();
            final CountDown countDown = new CountDown(shardCount);
            this.dependencies = dependencies;
            this.onDone = () -> {
                if (countDown.countDown()) {
                    var channelListener = new ChannelActionListener<>(channel);
                    try (queryPhaseResultConsumer) {
                        var failure = queryPhaseResultConsumer.failure.get();
                        if (failure != null) {
                            try {
                                queryPhaseResultConsumer.getSuccessfulResults()
                                    .forEach(
                                        searchPhaseResult -> maybeRelease(
                                            dependencies.searchService,
                                            searchRequest,
                                            searchPhaseResult.queryResult() != null
                                                ? searchPhaseResult.queryResult()
                                                : searchPhaseResult.rankFeatureResult()
                                        )
                                    );
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                            channelListener.onFailure(failure);
                            return;
                        }
                        final Object[] results = new Object[shardCount];
                        for (int i = 0; i < results.length; i++) {
                            var e = failures.get(i);
                            var res = queryPhaseResultConsumer.results.get(i);
                            if (e != null) {
                                results[i] = e;
                                assert res == null;
                            } else {
                                results[i] = res;
                                assert results[i] != null;
                            }
                        }
                        final QueryPhaseResultConsumer.MergeResult mergeResult;
                        try {
                            mergeResult = Objects.requireNonNullElse(
                                queryPhaseResultConsumer.consumePartialResult(),
                                EMPTY_PARTIAL_MERGE_RESULT
                            );
                        } catch (Exception e) {
                            channelListener.onFailure(e);
                            return;
                        }
                        // translate shard indices to those on the coordinator so that it can interpret the merge result without adjustments
                        final Set<Integer> relevantShardIndices = new HashSet<>();
                        for (ScoreDoc scoreDoc : mergeResult.reducedTopDocs().scoreDocs) {
                            final int localIndex = scoreDoc.shardIndex;
                            scoreDoc.shardIndex = searchRequest.shards.get(localIndex).shardIndex;
                            relevantShardIndices.add(localIndex);
                        }
                        for (Object result : results) {
                            if (result instanceof QuerySearchResult q
                                && q.getContextId() != null
                                && relevantShardIndices.contains(q.getShardIndex()) == false
                                && q.hasSuggestHits() == false
                                && q.getRankShardResult() == null
                                && searchRequest.searchRequest.scroll() == null
                                && (AbstractSearchAsyncAction.isPartOfPIT(null, searchRequest.searchRequest, q.getContextId()) == false)) {
                                if (dependencies.searchService.freeReaderContext(q.getContextId())) {
                                    q.clearContextId();
                                }
                            }
                        }

                        ActionListener.respondAndRelease(
                            channelListener,
                            new NodeQueryResponse(mergeResult, results, queryPhaseResultConsumer.topDocsStats)
                        );
                        queryPhaseResultConsumer.buffer = null;
                    } catch (Throwable e) {
                        throw new AssertionError(e);
                    }
                }
            };
        }

        void consumeResult(QuerySearchResult queryResult) {
            if (queryResult.isNull() == false
                // disable sort optims for scroll requests because they keep track of the last bottom doc locally (per shard)
                && searchRequest.searchRequest.scroll() == null
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
        }
    }
}
