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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.BytesTransportResponse;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.IntUnaryOperator;

import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;
import static org.elasticsearch.search.sort.FieldSortBuilder.NAME;
import static org.elasticsearch.search.sort.FieldSortBuilder.hasPrimaryFieldSort;

public class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private static final Logger logger = LogManager.getLogger(SearchQueryThenFetchAsyncAction.class);

    private final SearchProgressListener progressListener;

    // informations to track the best bottom top doc globally.
    private final int topDocsSize;
    private final int trackTotalHitsUpTo;
    private volatile BottomSortValuesCollector bottomSortCollector;
    private final Client client;
    private final boolean batchQueryPhase;
    private final SearchService searchService;

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
        List<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters,
        Client client,
        SearchService searchService
    ) {
        super(
            "query",
            logger,
            namedWriteableRegistry,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            resultConsumer,
            request.getMaxConcurrentShardRequests(),
            clusters
        );
        this.topDocsSize = getTopDocsSize(request);
        this.trackTotalHitsUpTo = request.resolveTrackTotalHitsUpTo();
        this.progressListener = task.getProgressListener();
        this.client = client;
        this.batchQueryPhase = searchService.batchQueryPhase();
        this.searchService = searchService;
        // don't build the SearchShard list (can be expensive) if the SearchProgressListener won't use it
        if (progressListener != SearchProgressListener.NOOP) {
            notifyListShards(progressListener, clusters, request, shardsIts);
        }
    }

    @Override
    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final Transport.Connection connection,
        final SearchActionListener<SearchPhaseResult> listener
    ) {
        ShardSearchRequest request = tryRewriteWithUpdatedSortValue(
            bottomSortCollector,
            trackTotalHitsUpTo,
            super.buildShardSearchRequest(shardIt, listener.requestIndex)
        );
        getSearchTransport().sendExecuteQuery(connection, request, getTask(), listener);
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onShardResult(SearchPhaseResult result) {
        QuerySearchResult queryResult = result.queryResult();
        if (queryResult.isNull() == false
            // disable sort optims for scroll requests because they keep track of the last bottom doc locally (per shard)
            && getRequest().scroll() == null
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
        super.onShardResult(result);
    }

    static SearchPhase nextPhase(
        Client client,
        AbstractSearchAsyncAction<?> context,
        SearchPhaseResults<SearchPhaseResult> queryResults,
        AggregatedDfs aggregatedDfs
    ) {
        var rankFeaturePhaseCoordCtx = RankFeaturePhase.coordinatorContext(context.getRequest().source(), client);
        if (rankFeaturePhaseCoordCtx == null) {
            return new FetchSearchPhase(queryResults, aggregatedDfs, context, null);
        }
        return new RankFeaturePhase(queryResults, aggregatedDfs, context, rankFeaturePhaseCoordCtx);
    }

    @Override
    protected SearchPhase getNextPhase() {
        return nextPhase(client, this, results, null);
    }

    /**
     * Response to a query phase request, holding per-shard results that have been partially reduced as well as
     * the partial reduce result.
     */
    public static final class NodeQueryResponse extends TransportResponse {

        private final RefCounted refCounted = LeakTracker.wrap(new SimpleRefCounted());

        private final Object[] results;
        private final SearchPhaseController.TopDocsStats topDocsStats;
        private final QueryPhaseResultConsumer.MergeResult mergeResult;

        NodeQueryResponse(StreamInput in) throws IOException {
            this.results = in.readArray(i -> i.readBoolean() ? new QuerySearchResult(i) : i.readException(), Object[]::new);
            this.mergeResult = QueryPhaseResultConsumer.MergeResult.readFrom(in);
            this.topDocsStats = SearchPhaseController.TopDocsStats.readFrom(in);
        }

        // public for tests
        public Object[] getResults() {
            return results;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(results.length);
            for (Object result : results) {
                if (result instanceof Exception e) {
                    writePerShardException(out, e);
                } else {
                    writePerShardResult(out, (QuerySearchResult) result);
                }
            }
            writeMergeResult(out, mergeResult, topDocsStats);
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
                    if (results[i] instanceof QuerySearchResult r) {
                        r.decRef();
                    }
                    results[i] = null;
                }
                return true;
            }
            return false;
        }

        private static void writeMergeResult(
            StreamOutput out,
            QueryPhaseResultConsumer.MergeResult mergeResult,
            SearchPhaseController.TopDocsStats topDocsStats
        ) throws IOException {
            mergeResult.writeTo(out);
            topDocsStats.writeTo(out);
        }

        private static void writePerShardException(StreamOutput o, Exception e) throws IOException {
            o.writeBoolean(false);
            o.writeException(e);
        }

        private static void writePerShardResult(StreamOutput out, SearchPhaseResult result) throws IOException {
            out.writeBoolean(true);
            result.writeTo(out);
        }
    }

    /**
     * Request for starting the query phase for multiple shards.
     */
    public static final class NodeQueryRequest extends AbstractTransportRequest implements IndicesRequest {
        private final List<ShardToQuery> shards;
        private final SearchRequest searchRequest;
        private final Map<String, AliasFilter> aliasFilters;
        private final int totalShards;
        private final long absoluteStartMillis;
        private final String localClusterAlias;

        private NodeQueryRequest(SearchRequest searchRequest, int totalShards, long absoluteStartMillis, String localClusterAlias) {
            this.shards = new ArrayList<>();
            this.searchRequest = searchRequest;
            this.aliasFilters = new HashMap<>();
            this.totalShards = totalShards;
            this.absoluteStartMillis = absoluteStartMillis;
            this.localClusterAlias = localClusterAlias;
        }

        private NodeQueryRequest(StreamInput in) throws IOException {
            super(in);
            this.shards = in.readCollectionAsImmutableList(ShardToQuery::readFrom);
            this.searchRequest = new SearchRequest(in);
            this.aliasFilters = in.readImmutableMap(AliasFilter::readFrom);
            this.totalShards = in.readVInt();
            this.absoluteStartMillis = in.readLong();
            this.localClusterAlias = in.readOptionalString();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new SearchShardTask(id, type, action, "NodeQueryRequest", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(shards);
            searchRequest.writeTo(out, true);
            out.writeMap(aliasFilters, (o, v) -> v.writeTo(o));
            out.writeVInt(totalShards);
            out.writeLong(absoluteStartMillis);
            out.writeOptionalString(localClusterAlias);
        }

        @Override
        public String[] indices() {
            return shards.stream().flatMap(s -> Arrays.stream(s.originalIndices())).distinct().toArray(String[]::new);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return searchRequest.indicesOptions();
        }
    }

    private record ShardToQuery(float boost, String[] originalIndices, int shardIndex, ShardId shardId, ShardSearchContextId contextId)
        implements
            Comparable<ShardToQuery>,
            Writeable {

        static ShardToQuery readFrom(StreamInput in) throws IOException {
            return new ShardToQuery(
                in.readFloat(),
                in.readStringArray(),
                in.readVInt(),
                new ShardId(in),
                in.readOptionalWriteable(ShardSearchContextId::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeFloat(boost);
            out.writeStringArray(originalIndices);
            out.writeVInt(shardIndex);
            shardId.writeTo(out);
            out.writeOptionalWriteable(contextId);
        }

        @Override
        public int compareTo(ShardToQuery o) {
            return shardId.compareTo(o.shardId);
        }
    }

    /**
     * Check if, based on already collected results, a shard search can be updated with a lower search threshold than is current set.
     * When the query executes via batched execution, data nodes this take into account the results of queries run against shards local
     * to the datanode. On the coordinating node results received from all data nodes are taken into account.
     * <p>
     * See {@link BottomSortValuesCollector} for details.
     */
    private static ShardSearchRequest tryRewriteWithUpdatedSortValue(
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
            request.setRunCanMatchInQueryPhase(true);
        }

        // set the current best bottom field doc
        if (bottomSortCollector.getBottomSortValues() != null) {
            request.setBottomSortValues(bottomSortCollector.getBottomSortValues());
            request.setRunCanMatchInQueryPhase(true);
        }
        return request;
    }

    private static boolean isPartOfPIT(SearchRequest request, ShardSearchContextId contextId) {
        final PointInTimeBuilder pointInTimeBuilder = request.pointInTimeBuilder();
        if (pointInTimeBuilder != null) {
            return request.pointInTimeBuilder().getSearchContextId(null).contains(contextId);
        } else {
            return false;
        }
    }

    @Override
    protected void doRun(Map<SearchShardIterator, Integer> shardIndexMap) {
        if (this.batchQueryPhase == false) {
            super.doRun(shardIndexMap);
            return;
        }
        AbstractSearchAsyncAction.doCheckNoMissingShards(getName(), request, shardsIts);
        final Map<CanMatchPreFilterSearchPhase.SendingTarget, NodeQueryRequest> perNodeQueries = new HashMap<>();
        final var transportService = searchTransportService.transportService();
        final String localNodeId = transportService.getLocalNode().getId();
        final int numberOfShardsTotal = shardsIts.size();
        for (int i = 0; i < numberOfShardsTotal; i++) {
            final SearchShardIterator shardRoutings = shardsIts.get(i);
            assert shardRoutings.skip() == false;
            assert shardIndexMap.containsKey(shardRoutings);
            int shardIndex = shardIndexMap.get(shardRoutings);
            final SearchShardTarget routing = shardRoutings.nextOrNull();
            if (routing == null) {
                failOnUnavailable(shardIndex, shardRoutings);
            } else {
                final String nodeId = routing.getNodeId();
                // local requests don't need batching as there's no network latency
                var perNodeRequest = perNodeQueries.computeIfAbsent(
                    new CanMatchPreFilterSearchPhase.SendingTarget(routing.getClusterAlias(), nodeId),
                    t -> new NodeQueryRequest(request, numberOfShardsTotal, timeProvider.absoluteStartMillis(), t.clusterAlias())
                );
                final String indexUUID = routing.getShardId().getIndex().getUUID();
                perNodeRequest.shards.add(
                    new ShardToQuery(
                        concreteIndexBoosts.getOrDefault(indexUUID, DEFAULT_INDEX_BOOST),
                        getOriginalIndices(shardIndex).indices(),
                        shardIndex,
                        routing.getShardId(),
                        shardRoutings.getSearchContextId()
                    )
                );
                var filterForAlias = aliasFilter.getOrDefault(indexUUID, AliasFilter.EMPTY);
                if (filterForAlias != AliasFilter.EMPTY) {
                    perNodeRequest.aliasFilters.putIfAbsent(indexUUID, filterForAlias);
                }
            }
        }
        final var localTarget = new CanMatchPreFilterSearchPhase.SendingTarget(request.getLocalClusterAlias(), localNodeId);
        var localNodeRequest = perNodeQueries.remove(localTarget);
        if (localNodeRequest != null) {
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION).execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    var shards = localNodeRequest.shards;
                    if (shards.size() > 1 && hasPrimaryFieldSort(request.source())) {
                        @SuppressWarnings("rawtypes")
                        final MinAndMax[] minAndMax = new MinAndMax[shards.size()];
                        for (int i = 0; i < minAndMax.length; i++) {
                            // TODO: refactor to avoid building the search request twice, here and then when actually executing the query
                            minAndMax[i] = searchService.canMatch(buildShardSearchRequestForLocal(localNodeRequest, shards.get(i)))
                                .estimatedMinAndMax();
                        }

                        try {
                            final int[] indexes = CanMatchPreFilterSearchPhase.sortShards(shards, minAndMax, request.source());
                            final ShardToQuery[] orig = shards.toArray(new ShardToQuery[0]);
                            for (int i = 0; i < indexes.length; i++) {
                                shards.set(i, orig[indexes[i]]);
                            }
                        } catch (Exception e) {
                            // ignored, field type conflicts will be dealt with in upstream logic
                            // TODO: we should fail the query here, we're already seeing a field type conflict on the sort field,
                            // no need to actually execute the queries and go through a lot of work before we inevitably have to
                            // fail the search

                        }
                    }
                    executeWithoutBatching(localTarget, localNodeRequest);
                }

                @Override
                public void onFailure(Exception e) {
                    SearchQueryThenFetchAsyncAction.this.onPhaseFailure(NAME, "", e);
                }
            });
        }
        perNodeQueries.forEach((routing, request) -> {
            if (request.shards.size() == 1) {
                executeAsSingleRequest(routing, request.shards.getFirst());
                return;
            }
            final Transport.Connection connection;
            try {
                connection = getConnection(routing.clusterAlias(), routing.nodeId());
            } catch (Exception e) {
                onNodeQueryFailure(e, request, routing);
                return;
            }
            // must check both node and transport versions to correctly deal with BwC on proxy connections
            if (connectionSupportsBatchedExecution(connection) == false) {
                executeWithoutBatching(routing, request);
                return;
            }
            transportService.sendChildRequest(
                connection,
                NODE_SEARCH_ACTION_NAME,
                request,
                task,
                new TransportResponseHandler<NodeQueryResponse>() {
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
                            queryPhaseResultConsumer.addBatchedPartialResult(response.topDocsStats, response.mergeResult);
                        }
                        for (int i = 0; i < response.results.length; i++) {
                            var s = request.shards.get(i);
                            int shardIdx = s.shardIndex;
                            final SearchShardTarget target = new SearchShardTarget(routing.nodeId(), s.shardId, routing.clusterAlias());
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
                        if (e instanceof SendRequestTransportException || cause instanceof TaskCancelledException) {
                            // two possible special cases here where we do not want to fail the phase:
                            // failure to send out the request -> handle things the same way a shard would fail with unbatched execution
                            // as this could be a transient failure and partial results we may have are still valid
                            // cancellation of the whole batched request on the remote -> maybe we timed out or so, partial results may
                            // still be valid
                            onNodeQueryFailure(e, request, routing);
                        } else {
                            // Remote failure that wasn't due to networking or cancellation means that the data node was unable to reduce
                            // its local results. Failure to reduce always fails the phase without exception so we fail the phase here.
                            if (results instanceof QueryPhaseResultConsumer queryPhaseResultConsumer) {
                                queryPhaseResultConsumer.failure.compareAndSet(null, cause);
                            }
                            onPhaseFailure(getName(), "", cause);
                        }
                    }
                }
            );
        });
    }

    private static ShardSearchRequest buildShardSearchRequestForLocal(NodeQueryRequest nodeQueryRequest, ShardToQuery shardToQuery) {
        var shardId = shardToQuery.shardId;
        var searchRequest = nodeQueryRequest.searchRequest;
        var pitBuilder = searchRequest.pointInTimeBuilder();
        return buildShardSearchRequest(
            shardId,
            nodeQueryRequest.localClusterAlias,
            shardToQuery.shardIndex,
            shardToQuery.contextId,
            new OriginalIndices(shardToQuery.originalIndices, searchRequest.indicesOptions()),
            nodeQueryRequest.aliasFilters.getOrDefault(shardId.getIndex().getUUID(), AliasFilter.EMPTY),
            pitBuilder == null ? null : pitBuilder.getKeepAlive(),
            shardToQuery.boost,
            searchRequest,
            nodeQueryRequest.totalShards,
            nodeQueryRequest.absoluteStartMillis,
            false
        );
    }

    public static boolean connectionSupportsBatchedExecution(Transport.Connection connection) {
        return connection.getTransportVersion().onOrAfter(TransportVersions.BATCHED_QUERY_PHASE_VERSION)
            && connection.getNode().getVersionInformation().nodeVersion().onOrAfter(Version.V_9_1_0);
    }

    private void executeWithoutBatching(CanMatchPreFilterSearchPhase.SendingTarget targetNode, NodeQueryRequest request) {
        for (ShardToQuery shard : request.shards) {
            executeAsSingleRequest(targetNode, shard);
        }
    }

    private void executeAsSingleRequest(CanMatchPreFilterSearchPhase.SendingTarget targetNode, ShardToQuery shard) {
        final int sidx = shard.shardIndex;
        this.performPhaseOnShard(
            sidx,
            shardIterators[sidx],
            new SearchShardTarget(targetNode.nodeId(), shard.shardId, targetNode.clusterAlias())
        );
    }

    private void onNodeQueryFailure(Exception e, NodeQueryRequest request, CanMatchPreFilterSearchPhase.SendingTarget target) {
        for (ShardToQuery shard : request.shards) {
            int idx = shard.shardIndex;
            onShardFailure(idx, new SearchShardTarget(target.nodeId(), shard.shardId, target.clusterAlias()), shardIterators[idx], e);
        }
    }

    private static final String NODE_SEARCH_ACTION_NAME = "indices:data/read/search[query][n]";

    static void registerNodeSearchAction(
        SearchTransportService searchTransportService,
        SearchService searchService,
        SearchPhaseController searchPhaseController
    ) {
        var transportService = searchTransportService.transportService();
        var threadPool = transportService.getThreadPool();
        final Dependencies dependencies = new Dependencies(searchService, transportService, threadPool.executor(ThreadPool.Names.SEARCH));
        // Even though not all searches run on the search pool, we use the search pool size as the upper limit of shards to execute in
        // parallel to keep the implementation simple instead of working out the exact pool(s) a query will use up-front.
        final int searchPoolMax = threadPool.info(ThreadPool.Names.SEARCH).getMax();
        transportService.registerRequestHandler(
            NODE_SEARCH_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
            NodeQueryRequest::new,
            (request, channel, task) -> {
                final SearchRequest searchRequest = request.searchRequest;
                ShardSearchRequest[] shardSearchRequests = null;
                IntUnaryOperator shards = IntUnaryOperator.identity();
                final int shardCount = request.shards.size();
                if (shardCount > 1 && hasPrimaryFieldSort(searchRequest.source())) {
                    try {
                        shardSearchRequests = new ShardSearchRequest[shardCount];
                        @SuppressWarnings("rawtypes")
                        final MinAndMax[] minAndMax = new MinAndMax[shardCount];
                        for (int i = 0; i < minAndMax.length; i++) {
                            ShardSearchRequest r = buildShardSearchRequestForLocal(request, request.shards.get(i));
                            shardSearchRequests[i] = r;
                            var canMatch = searchService.canMatch(r);
                            if (canMatch.canMatch()) {
                                r.setRunCanMatchInQueryPhase(false);
                                minAndMax[i] = canMatch.estimatedMinAndMax();
                            }
                        }
                        int[] indexes = CanMatchPreFilterSearchPhase.sortShards(request.shards, minAndMax, searchRequest.source());
                        shards = pos -> indexes[pos];
                    } catch (Exception e) {
                        // TODO: ignored for now but we'll be guaranteed to fail the query phase at this point, fix things to fail here
                        // already
                    }
                }
                final CancellableTask cancellableTask = (CancellableTask) task;
                int workers = Math.min(searchRequest.getMaxConcurrentShardRequests(), Math.min(shardCount, searchPoolMax));
                final var state = new QueryPerNodeState(
                    new QueryPhaseResultConsumer(
                        searchRequest,
                        dependencies.executor,
                        searchService.getCircuitBreaker(),
                        searchPhaseController,
                        cancellableTask::isCancelled,
                        SearchProgressListener.NOOP,
                        shardCount,
                        e -> logger.error("failed to merge on data node", e)
                    ),
                    request,
                    shards,
                    cancellableTask,
                    channel,
                    dependencies,
                    shardSearchRequests
                );
                // TODO: log activating or otherwise limiting parallelism might be helpful here
                for (int i = 0; i < workers; i++) {
                    executeShardTasks(state);
                }
            }
        );
        TransportActionProxy.registerProxyAction(transportService, NODE_SEARCH_ACTION_NAME, true, NodeQueryResponse::new);
    }

    private static void releaseLocalContext(SearchService searchService, NodeQueryRequest request, SearchPhaseResult result) {
        var phaseResult = result.queryResult() != null ? result.queryResult() : result.rankFeatureResult();
        if (phaseResult != null
            && phaseResult.hasSearchContext()
            && request.searchRequest.scroll() == null
            && isPartOfPIT(request.searchRequest, phaseResult.getContextId()) == false) {
            searchService.freeReaderContext(phaseResult.getContextId());
        }
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

    private static void executeShardTasks(QueryPerNodeState state) {
        int idx;
        final NodeQueryRequest nodeQueryRequest = state.searchRequest;
        var shards = nodeQueryRequest.shards;
        final int totalShardCount = shards.size();
        while ((idx = state.currentShardIndex.getAndIncrement()) < totalShardCount) {
            final int dataNodeLocalIdx = idx;
            final ListenableFuture<Void> doneFuture = new ListenableFuture<>();
            try {
                final SearchRequest searchRequest = nodeQueryRequest.searchRequest;
                var pitBuilder = searchRequest.pointInTimeBuilder();
                int translatedIndex = state.shardsToQuery.applyAsInt(dataNodeLocalIdx);
                var shardToQuery = shards.get(translatedIndex);
                final var shardId = shardToQuery.shardId;
                ShardSearchRequest r = state.shardSearchRequests == null ? null : state.shardSearchRequests[translatedIndex];
                if (r == null) {
                    r = buildShardSearchRequest(
                        shardId,
                        nodeQueryRequest.localClusterAlias,
                        shardToQuery.shardIndex,
                        shardToQuery.contextId,
                        new OriginalIndices(shardToQuery.originalIndices, nodeQueryRequest.indicesOptions()),
                        nodeQueryRequest.aliasFilters.getOrDefault(shardId.getIndex().getUUID(), AliasFilter.EMPTY),
                        pitBuilder == null ? null : pitBuilder.getKeepAlive(),
                        shardToQuery.boost,
                        searchRequest,
                        nodeQueryRequest.totalShards,
                        nodeQueryRequest.absoluteStartMillis,
                        state.hasResponse.getAcquire()
                    );
                } else {
                    state.shardSearchRequests[translatedIndex] = null;
                }
                state.dependencies.searchService.executeQueryPhase(
                    tryRewriteWithUpdatedSortValue(state.bottomSortCollector, state.trackTotalHitsUpTo, r),
                    state.task,
                    new SearchActionListener<>(
                        new SearchShardTarget(null, shardToQuery.shardId, nodeQueryRequest.localClusterAlias),
                        dataNodeLocalIdx
                    ) {
                        @Override
                        protected void innerOnResponse(SearchPhaseResult searchPhaseResult) {
                            try {
                                state.consumeResult(searchPhaseResult.queryResult());
                            } catch (Exception e) {
                                setFailure(state, dataNodeLocalIdx, e);
                            } finally {
                                doneFuture.onResponse(null);
                            }
                        }

                        private void setFailure(QueryPerNodeState state, int dataNodeLocalIdx, Exception e) {
                            state.failures.put(dataNodeLocalIdx, e);
                            state.onShardDone();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // TODO: count down fully and just respond with an exception if partial results aren't allowed as an
                            // optimization
                            setFailure(state, dataNodeLocalIdx, e);
                            doneFuture.onResponse(null);
                        }
                    }
                );
            } catch (Exception e) {
                // TODO this could be done better now, we probably should only make sure to have a single loop running at
                // minimum and ignore + requeue rejections in that case
                state.failures.put(dataNodeLocalIdx, e);
                state.onShardDone();
                continue;
            }
            if (doneFuture.isDone() == false) {
                doneFuture.addListener(ActionListener.running(() -> executeShardTasks(state)));
                break;
            }
        }
    }

    private record Dependencies(SearchService searchService, TransportService transportService, Executor executor) {}

    private static final class QueryPerNodeState {

        private static final QueryPhaseResultConsumer.MergeResult EMPTY_PARTIAL_MERGE_RESULT = new QueryPhaseResultConsumer.MergeResult(
            List.of(),
            null,
            null,
            0L
        );

        private final AtomicInteger currentShardIndex = new AtomicInteger();
        private final QueryPhaseResultConsumer queryPhaseResultConsumer;
        private final NodeQueryRequest searchRequest;
        private final IntUnaryOperator shardsToQuery;
        private final CancellableTask task;
        private final ConcurrentHashMap<Integer, Exception> failures = new ConcurrentHashMap<>();
        private final Dependencies dependencies;
        private final AtomicBoolean hasResponse = new AtomicBoolean(false);
        private final int trackTotalHitsUpTo;
        private final int topDocsSize;
        private final CountDown countDown;
        private final @Nullable ShardSearchRequest[] shardSearchRequests;
        private final TransportChannel channel;
        private volatile BottomSortValuesCollector bottomSortCollector;

        private QueryPerNodeState(
            QueryPhaseResultConsumer queryPhaseResultConsumer,
            NodeQueryRequest searchRequest,
            IntUnaryOperator shardsToQuery,
            CancellableTask task,
            TransportChannel channel,
            Dependencies dependencies,
            @Nullable ShardSearchRequest[] shardSearchRequests
        ) {
            this.queryPhaseResultConsumer = queryPhaseResultConsumer;
            this.searchRequest = searchRequest;
            this.shardsToQuery = shardsToQuery;
            this.trackTotalHitsUpTo = searchRequest.searchRequest.resolveTrackTotalHitsUpTo();
            this.topDocsSize = getTopDocsSize(searchRequest.searchRequest);
            this.task = task;
            this.countDown = new CountDown(queryPhaseResultConsumer.getNumShards());
            this.channel = channel;
            this.dependencies = dependencies;
            this.shardSearchRequests = shardSearchRequests;
        }

        void onShardDone() {
            if (countDown.countDown() == false) {
                return;
            }
            RecyclerBytesStreamOutput out = null;
            boolean success = false;
            var channelListener = new ChannelActionListener<>(channel);
            try (queryPhaseResultConsumer) {
                var failure = queryPhaseResultConsumer.failure.get();
                if (failure != null) {
                    handleMergeFailure(failure, channelListener);
                    return;
                }
                final QueryPhaseResultConsumer.MergeResult mergeResult;
                try {
                    mergeResult = Objects.requireNonNullElse(
                        queryPhaseResultConsumer.consumePartialMergeResultDataNode(),
                        EMPTY_PARTIAL_MERGE_RESULT
                    );
                } catch (Exception e) {
                    handleMergeFailure(e, channelListener);
                    return;
                }
                // translate shard indices to those on the coordinator so that it can interpret the merge result without adjustments,
                // also collect the set of indices that may be part of a subsequent fetch operation here so that we can release all other
                // indices without a roundtrip to the coordinating node
                final BitSet relevantShardIndices = new BitSet(searchRequest.shards.size());
                if (mergeResult.reducedTopDocs() != null) {
                    for (ScoreDoc scoreDoc : mergeResult.reducedTopDocs().scoreDocs) {
                        final int localIndex = scoreDoc.shardIndex;
                        scoreDoc.shardIndex = searchRequest.shards.get(localIndex).shardIndex;
                        relevantShardIndices.set(localIndex);
                    }
                }
                final int resultCount = queryPhaseResultConsumer.getNumShards();
                out = dependencies.transportService.newNetworkBytesStream();
                out.setTransportVersion(channel.getVersion());
                try {
                    out.writeVInt(resultCount);
                    for (int i = 0; i < resultCount; i++) {
                        var result = queryPhaseResultConsumer.results.get(i);
                        if (result == null) {
                            NodeQueryResponse.writePerShardException(out, failures.remove(i));
                        } else {
                            // free context id and remove it from the result right away in case we don't need it anymore
                            maybeFreeContext(result, relevantShardIndices);
                            NodeQueryResponse.writePerShardResult(out, result);
                        }
                    }
                    NodeQueryResponse.writeMergeResult(out, mergeResult, queryPhaseResultConsumer.topDocsStats);
                    success = true;
                } catch (IOException e) {
                    handleMergeFailure(e, channelListener);
                    return;
                }
            } finally {
                if (success == false && out != null) {
                    out.close();
                }
            }
            ActionListener.respondAndRelease(channelListener, new BytesTransportResponse(out.moveToBytesReference()));
        }

        private void maybeFreeContext(SearchPhaseResult result, BitSet relevantShardIndices) {
            if (result instanceof QuerySearchResult q
                && q.getContextId() != null
                && relevantShardIndices.get(q.getShardIndex()) == false
                && q.hasSuggestHits() == false
                && q.getRankShardResult() == null
                && searchRequest.searchRequest.scroll() == null
                && isPartOfPIT(searchRequest.searchRequest, q.getContextId()) == false) {
                if (dependencies.searchService.freeReaderContext(q.getContextId())) {
                    q.clearContextId();
                }
            }
        }

        private void handleMergeFailure(Exception e, ChannelActionListener<TransportResponse> channelListener) {
            queryPhaseResultConsumer.getSuccessfulResults()
                .forEach(searchPhaseResult -> releaseLocalContext(dependencies.searchService, searchRequest, searchPhaseResult));
            channelListener.onFailure(e);
        }

        void consumeResult(QuerySearchResult queryResult) {
            // no need for any cache effects when we're already flipped to ture => plain read + set-release
            hasResponse.compareAndExchangeRelease(false, true);
            // TODO: dry up the bottom sort collector with the coordinator side logic in the top-level class here
            if (queryResult.isNull() == false
                // disable sort optims for scroll requests because they keep track of the last bottom doc locally (per shard)
                && searchRequest.searchRequest.scroll() == null
                // top docs are already consumed if the query was cancelled or in error.
                && queryResult.hasConsumedTopDocs() == false
                && queryResult.topDocs() != null
                && queryResult.topDocs().topDocs.getClass() == TopFieldDocs.class) {
                TopFieldDocs topDocs = (TopFieldDocs) queryResult.topDocs().topDocs;
                var bottomSortCollector = this.bottomSortCollector;
                if (bottomSortCollector == null) {
                    synchronized (this) {
                        bottomSortCollector = this.bottomSortCollector;
                        if (bottomSortCollector == null) {
                            bottomSortCollector = this.bottomSortCollector = new BottomSortValuesCollector(topDocsSize, topDocs.fields);
                        }
                    }
                }
                bottomSortCollector.consumeTopDocs(topDocs, queryResult.sortValueFormats());
            }
            queryPhaseResultConsumer.consumeResult(queryResult, this::onShardDone);
        }
    }
}
