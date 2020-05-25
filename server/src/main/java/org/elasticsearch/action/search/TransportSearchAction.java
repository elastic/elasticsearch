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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.search.sort.FieldSortBuilder.hasPrimaryFieldSort;

public class TransportSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    /** The maximum number of shards for a single search request. */
    public static final Setting<Long> SHARD_COUNT_LIMIT_SETTING = Setting.longSetting(
            "action.search.shard_count.limit", Long.MAX_VALUE, 1L, Property.Dynamic, Property.NodeScope);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final RemoteClusterService remoteClusterService;
    private final SearchPhaseController searchPhaseController;
    private final SearchService searchService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportSearchAction(ThreadPool threadPool, TransportService transportService, SearchService searchService,
                                 SearchTransportService searchTransportService, SearchPhaseController searchPhaseController,
                                 ClusterService clusterService, ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        super(SearchAction.NAME, transportService, actionFilters, (Writeable.Reader<SearchRequest>) SearchRequest::new);
        this.threadPool = threadPool;
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        SearchTransportService.registerRequestHandler(transportService, searchService);
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    private Map<String, AliasFilter> buildPerIndexAliasFilter(SearchRequest request, ClusterState clusterState,
                                                              Index[] concreteIndices, Map<String, AliasFilter> remoteAliasMap) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), indicesAndAliases);
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        aliasFilterMap.putAll(remoteAliasMap);
        return aliasFilterMap;
    }

    private Map<String, Float> resolveIndexBoosts(SearchRequest searchRequest, ClusterState clusterState) {
        if (searchRequest.source() == null) {
            return Collections.emptyMap();
        }

        SearchSourceBuilder source = searchRequest.source();
        if (source.indexBoosts() == null) {
            return Collections.emptyMap();
        }

        Map<String, Float> concreteIndexBoosts = new HashMap<>();
        for (SearchSourceBuilder.IndexBoost ib : source.indexBoosts()) {
            Index[] concreteIndices =
                indexNameExpressionResolver.concreteIndices(clusterState, searchRequest.indicesOptions(), ib.getIndex());

            for (Index concreteIndex : concreteIndices) {
                concreteIndexBoosts.putIfAbsent(concreteIndex.getUUID(), ib.getBoost());
            }
        }
        return Collections.unmodifiableMap(concreteIndexBoosts);
    }

    /**
     * Search operations need two clocks. One clock is to fulfill real clock needs (e.g., resolving
     * "now" to an index name). Another clock is needed for measuring how long a search operation
     * took. These two uses are at odds with each other. There are many issues with using a real
     * clock for measuring how long an operation took (they often lack precision, they are subject
     * to moving backwards due to NTP and other such complexities, etc.). There are also issues with
     * using a relative clock for reporting real time. Thus, we simply separate these two uses.
     */
    static final class SearchTimeProvider {

        private final long absoluteStartMillis;
        private final long relativeStartNanos;
        private final LongSupplier relativeCurrentNanosProvider;

        /**
         * Instantiates a new search time provider. The absolute start time is the real clock time
         * used for resolving index expressions that include dates. The relative start time is the
         * start of the search operation according to a relative clock. The total time the search
         * operation took can be measured against the provided relative clock and the relative start
         * time.
         *
         * @param absoluteStartMillis the absolute start time in milliseconds since the epoch
         * @param relativeStartNanos the relative start time in nanoseconds
         * @param relativeCurrentNanosProvider provides the current relative time
         */
        SearchTimeProvider(
                final long absoluteStartMillis,
                final long relativeStartNanos,
                final LongSupplier relativeCurrentNanosProvider) {
            this.absoluteStartMillis = absoluteStartMillis;
            this.relativeStartNanos = relativeStartNanos;
            this.relativeCurrentNanosProvider = relativeCurrentNanosProvider;
        }

        long getAbsoluteStartMillis() {
            return absoluteStartMillis;
        }

        long buildTookInMillis() {
            return TimeUnit.NANOSECONDS.toMillis(relativeCurrentNanosProvider.getAsLong() - relativeStartNanos);
        }
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        final long relativeStartNanos = System.nanoTime();
        final SearchTimeProvider timeProvider =
            new SearchTimeProvider(searchRequest.getOrCreateAbsoluteStartMillis(), relativeStartNanos, System::nanoTime);
        ActionListener<SearchSourceBuilder> rewriteListener = ActionListener.wrap(source -> {
            if (source != searchRequest.source()) {
                // only set it if it changed - we don't allow null values to be set but it might be already null. this way we catch
                // situations when source is rewritten to null due to a bug
                searchRequest.source(source);
            }
            final ClusterState clusterState = clusterService.state();
            final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(searchRequest.indicesOptions(),
                searchRequest.indices());
            OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            if (remoteClusterIndices.isEmpty()) {
                executeLocalSearch(task, timeProvider, searchRequest, localIndices, clusterState, listener);
            } else {
                if (shouldMinimizeRoundtrips(searchRequest)) {
                    ccsRemoteReduce(searchRequest, localIndices, remoteClusterIndices, timeProvider,
                            searchService.aggReduceContextBuilder(searchRequest),
                            remoteClusterService, threadPool, listener,
                            (r, l) -> executeLocalSearch(task, timeProvider, r, localIndices, clusterState, l));
                } else {
                    AtomicInteger skippedClusters = new AtomicInteger(0);
                    collectSearchShards(searchRequest.indicesOptions(), searchRequest.preference(), searchRequest.routing(),
                        skippedClusters, remoteClusterIndices, remoteClusterService, threadPool,
                        ActionListener.wrap(
                            searchShardsResponses -> {
                                List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
                                Map<String, AliasFilter> remoteAliasFilters = new HashMap<>();
                                BiFunction<String, String, DiscoveryNode> clusterNodeLookup = processRemoteShards(
                                    searchShardsResponses, remoteClusterIndices, remoteShardIterators, remoteAliasFilters);
                                int localClusters = localIndices == null ? 0 : 1;
                                int totalClusters = remoteClusterIndices.size() + localClusters;
                                int successfulClusters = searchShardsResponses.size() + localClusters;
                                executeSearch((SearchTask) task, timeProvider, searchRequest, localIndices,
                                    remoteShardIterators, clusterNodeLookup, clusterState, remoteAliasFilters, listener,
                                    new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters.get()));
                            },
                            listener::onFailure));
                }
            }
        }, listener::onFailure);
        if (searchRequest.source() == null) {
            rewriteListener.onResponse(searchRequest.source());
        } else {
            Rewriteable.rewriteAndFetch(searchRequest.source(), searchService.getRewriteContext(timeProvider::getAbsoluteStartMillis),
                rewriteListener);
        }
    }

    static boolean shouldMinimizeRoundtrips(SearchRequest searchRequest) {
        if (searchRequest.isCcsMinimizeRoundtrips() == false) {
            return false;
        }
        if (searchRequest.scroll() != null) {
            return false;
        }
        if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
            return false;
        }
        SearchSourceBuilder source = searchRequest.source();
        return source == null || source.collapse() == null || source.collapse().getInnerHits() == null ||
            source.collapse().getInnerHits().isEmpty();
    }

    static void ccsRemoteReduce(SearchRequest searchRequest, OriginalIndices localIndices, Map<String, OriginalIndices> remoteIndices,
                                SearchTimeProvider timeProvider, InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                RemoteClusterService remoteClusterService, ThreadPool threadPool, ActionListener<SearchResponse> listener,
                                BiConsumer<SearchRequest, ActionListener<SearchResponse>> localSearchConsumer) {

        if (localIndices == null && remoteIndices.size() == 1) {
            //if we are searching against a single remote cluster, we simply forward the original search request to such cluster
            //and we directly perform final reduction in the remote cluster
            Map.Entry<String, OriginalIndices> entry = remoteIndices.entrySet().iterator().next();
            String clusterAlias = entry.getKey();
            boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
            OriginalIndices indices = entry.getValue();
            SearchRequest ccsSearchRequest = SearchRequest.subSearchRequest(searchRequest, indices.indices(),
                clusterAlias, timeProvider.getAbsoluteStartMillis(), true);
            Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
            remoteClusterClient.search(ccsSearchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    Map<String, ProfileShardResult> profileResults = searchResponse.getProfileResults();
                    SearchProfileShardResults profile = profileResults == null || profileResults.isEmpty()
                        ? null : new SearchProfileShardResults(profileResults);
                    InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchResponse.getHits(),
                        (InternalAggregations) searchResponse.getAggregations(), searchResponse.getSuggest(), profile,
                        searchResponse.isTimedOut(), searchResponse.isTerminatedEarly(), searchResponse.getNumReducePhases());
                    listener.onResponse(new SearchResponse(internalSearchResponse, searchResponse.getScrollId(),
                        searchResponse.getTotalShards(), searchResponse.getSuccessfulShards(), searchResponse.getSkippedShards(),
                        timeProvider.buildTookInMillis(), searchResponse.getShardFailures(), new SearchResponse.Clusters(1, 1, 0)));
                }

                @Override
                public void onFailure(Exception e) {
                    if (skipUnavailable) {
                        listener.onResponse(SearchResponse.empty(timeProvider::buildTookInMillis, new SearchResponse.Clusters(1, 0, 1)));
                    } else {
                        listener.onFailure(wrapRemoteClusterFailure(clusterAlias, e));
                    }
                }
            });
        } else {
            SearchResponseMerger searchResponseMerger = createSearchResponseMerger(
                    searchRequest.source(), timeProvider, aggReduceContextBuilder);
            AtomicInteger skippedClusters = new AtomicInteger(0);
            final AtomicReference<Exception> exceptions = new AtomicReference<>();
            int totalClusters = remoteIndices.size() + (localIndices == null ? 0 : 1);
            final CountDown countDown = new CountDown(totalClusters);
            for (Map.Entry<String, OriginalIndices> entry : remoteIndices.entrySet()) {
                String clusterAlias = entry.getKey();
                boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
                OriginalIndices indices = entry.getValue();
                SearchRequest ccsSearchRequest = SearchRequest.subSearchRequest(searchRequest, indices.indices(),
                    clusterAlias, timeProvider.getAbsoluteStartMillis(), false);
                ActionListener<SearchResponse> ccsListener = createCCSListener(clusterAlias, skipUnavailable, countDown,
                    skippedClusters, exceptions, searchResponseMerger, totalClusters,  listener);
                Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
                remoteClusterClient.search(ccsSearchRequest, ccsListener);
            }
            if (localIndices != null) {
                ActionListener<SearchResponse> ccsListener = createCCSListener(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    false, countDown, skippedClusters, exceptions, searchResponseMerger, totalClusters, listener);
                SearchRequest ccsLocalSearchRequest = SearchRequest.subSearchRequest(searchRequest, localIndices.indices(),
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, timeProvider.getAbsoluteStartMillis(), false);
                localSearchConsumer.accept(ccsLocalSearchRequest, ccsListener);
            }
        }
    }

    static SearchResponseMerger createSearchResponseMerger(SearchSourceBuilder source, SearchTimeProvider timeProvider,
                                                           InternalAggregation.ReduceContextBuilder aggReduceContextBuilder) {
        final int from;
        final int size;
        final int trackTotalHitsUpTo;
        if (source == null) {
            from = SearchService.DEFAULT_FROM;
            size = SearchService.DEFAULT_SIZE;
            trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
        } else {
            from = source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
            size = source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
            trackTotalHitsUpTo = source.trackTotalHitsUpTo() == null
                ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO : source.trackTotalHitsUpTo();
            //here we modify the original source so we can re-use it by setting it to each outgoing search request
            source.from(0);
            source.size(from + size);
        }
        return new SearchResponseMerger(from, size, trackTotalHitsUpTo, timeProvider, aggReduceContextBuilder);
    }

    static void collectSearchShards(IndicesOptions indicesOptions, String preference, String routing, AtomicInteger skippedClusters,
                                    Map<String, OriginalIndices> remoteIndicesByCluster, RemoteClusterService remoteClusterService,
                                    ThreadPool threadPool, ActionListener<Map<String, ClusterSearchShardsResponse>> listener) {
        final CountDown responsesCountDown = new CountDown(remoteIndicesByCluster.size());
        final Map<String, ClusterSearchShardsResponse> searchShardsResponses = new ConcurrentHashMap<>();
        final AtomicReference<Exception> exceptions = new AtomicReference<>();
        for (Map.Entry<String, OriginalIndices> entry : remoteIndicesByCluster.entrySet()) {
            final String clusterAlias = entry.getKey();
            boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
            Client clusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
            final String[] indices = entry.getValue().indices();
            ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest(indices)
                .indicesOptions(indicesOptions).local(true).preference(preference).routing(routing);
            clusterClient.admin().cluster().searchShards(searchShardsRequest,
                new CCSActionListener<ClusterSearchShardsResponse, Map<String, ClusterSearchShardsResponse>>(
                    clusterAlias, skipUnavailable, responsesCountDown, skippedClusters, exceptions, listener) {
                    @Override
                    void innerOnResponse(ClusterSearchShardsResponse clusterSearchShardsResponse) {
                        searchShardsResponses.put(clusterAlias, clusterSearchShardsResponse);
                    }

                    @Override
                    Map<String, ClusterSearchShardsResponse> createFinalResponse() {
                        return searchShardsResponses;
                    }
                }
            );
        }
    }

    private static ActionListener<SearchResponse> createCCSListener(String clusterAlias, boolean skipUnavailable, CountDown countDown,
                                                             AtomicInteger skippedClusters, AtomicReference<Exception> exceptions,
                                                             SearchResponseMerger searchResponseMerger, int totalClusters,
                                                             ActionListener<SearchResponse> originalListener) {
        return new CCSActionListener<SearchResponse, SearchResponse>(clusterAlias, skipUnavailable, countDown, skippedClusters,
            exceptions, originalListener) {
            @Override
            void innerOnResponse(SearchResponse searchResponse) {
                searchResponseMerger.add(searchResponse);
            }

            @Override
            SearchResponse createFinalResponse() {
                SearchResponse.Clusters clusters = new SearchResponse.Clusters(totalClusters, searchResponseMerger.numResponses(),
                    skippedClusters.get());
                return searchResponseMerger.getMergedResponse(clusters);
            }
        };
    }

    private void executeLocalSearch(Task task, SearchTimeProvider timeProvider, SearchRequest searchRequest, OriginalIndices localIndices,
                                    ClusterState clusterState, ActionListener<SearchResponse> listener) {
        executeSearch((SearchTask)task, timeProvider, searchRequest, localIndices, Collections.emptyList(),
            (clusterName, nodeId) -> null, clusterState, Collections.emptyMap(), listener, SearchResponse.Clusters.EMPTY);
    }

    static BiFunction<String, String, DiscoveryNode> processRemoteShards(Map<String, ClusterSearchShardsResponse> searchShardsResponses,
                                                                         Map<String, OriginalIndices> remoteIndicesByCluster,
                                                                         List<SearchShardIterator> remoteShardIterators,
                                                                         Map<String, AliasFilter> aliasFilterMap) {
        Map<String, Map<String, DiscoveryNode>> clusterToNode = new HashMap<>();
        for (Map.Entry<String, ClusterSearchShardsResponse> entry : searchShardsResponses.entrySet()) {
            String clusterAlias = entry.getKey();
            ClusterSearchShardsResponse searchShardsResponse = entry.getValue();
            HashMap<String, DiscoveryNode> idToDiscoveryNode = new HashMap<>();
            clusterToNode.put(clusterAlias, idToDiscoveryNode);
            for (DiscoveryNode remoteNode : searchShardsResponse.getNodes()) {
                idToDiscoveryNode.put(remoteNode.getId(), remoteNode);
            }
            final Map<String, AliasFilter> indicesAndFilters = searchShardsResponse.getIndicesAndFilters();
            for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
                //add the cluster name to the remote index names for indices disambiguation
                //this ends up in the hits returned with the search response
                ShardId shardId = clusterSearchShardsGroup.getShardId();
                final AliasFilter aliasFilter;
                if (indicesAndFilters == null) {
                    aliasFilter = AliasFilter.EMPTY;
                } else {
                    aliasFilter = indicesAndFilters.get(shardId.getIndexName());
                    assert aliasFilter != null : "alias filter must not be null for index: " + shardId.getIndex();
                }
                String[] aliases = aliasFilter.getAliases();
                String[] finalIndices = aliases.length == 0 ? new String[] {shardId.getIndexName()} : aliases;
                // here we have to map the filters to the UUID since from now on we use the uuid for the lookup
                aliasFilterMap.put(shardId.getIndex().getUUID(), aliasFilter);
                final OriginalIndices originalIndices = remoteIndicesByCluster.get(clusterAlias);
                assert originalIndices != null : "original indices are null for clusterAlias: " + clusterAlias;
                SearchShardIterator shardIterator = new SearchShardIterator(clusterAlias, shardId,
                    Arrays.asList(clusterSearchShardsGroup.getShards()), new OriginalIndices(finalIndices,
                    originalIndices.indicesOptions()));
                remoteShardIterators.add(shardIterator);
            }
        }
        return (clusterAlias, nodeId) -> {
                Map<String, DiscoveryNode> clusterNodes = clusterToNode.get(clusterAlias);
                if (clusterNodes == null) {
                    throw new IllegalArgumentException("unknown remote cluster: " + clusterAlias);
                }
            return clusterNodes.get(nodeId);
        };
    }

    private Index[] resolveLocalIndices(OriginalIndices localIndices,
                                IndicesOptions indicesOptions,
                                ClusterState clusterState,
                                SearchTimeProvider timeProvider) {
        if (localIndices == null) {
            return Index.EMPTY_ARRAY; //don't search on any local index (happens when only remote indices were specified)
        }
        return indexNameExpressionResolver.concreteIndices(clusterState, indicesOptions, true,
            timeProvider.getAbsoluteStartMillis(), localIndices.indices());
    }

    private void executeSearch(SearchTask task, SearchTimeProvider timeProvider, SearchRequest searchRequest,
                               OriginalIndices localIndices, List<SearchShardIterator> remoteShardIterators,
                               BiFunction<String, String, DiscoveryNode> remoteConnections, ClusterState clusterState,
                               Map<String, AliasFilter> remoteAliasMap, ActionListener<SearchResponse> listener,
                               SearchResponse.Clusters clusters) {

        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        // TODO: I think startTime() should become part of ActionRequest and that should be used both for index name
        // date math expressions and $now in scripts. This way all apis will deal with now in the same way instead
        // of just for the _search api
        final Index[] indices = resolveLocalIndices(localIndices, searchRequest.indicesOptions(), clusterState, timeProvider);
        Map<String, AliasFilter> aliasFilter = buildPerIndexAliasFilter(searchRequest, clusterState, indices, remoteAliasMap);
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(),
            searchRequest.indices());
        routingMap = routingMap == null ? Collections.emptyMap() : Collections.unmodifiableMap(routingMap);
        String[] concreteIndices = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {
            concreteIndices[i] = indices[i].getName();
        }
        Map<String, Long> nodeSearchCounts = searchTransportService.getPendingSearchRequests();
        GroupShardsIterator<ShardIterator> localShardsIterator = clusterService.operationRouting().searchShards(clusterState,
                concreteIndices, routingMap, searchRequest.preference(), searchService.getResponseCollectorService(), nodeSearchCounts);
        GroupShardsIterator<SearchShardIterator> shardIterators = mergeShardsIterators(localShardsIterator, localIndices,
            searchRequest.getLocalClusterAlias(), remoteShardIterators);

        failIfOverShardCountLimit(clusterService, shardIterators.size());

        Map<String, Float> concreteIndexBoosts = resolveIndexBoosts(searchRequest, clusterState);

        // optimize search type for cases where there is only one shard group to search on
        if (shardIterators.size() == 1) {
            // if we only have one group, then we always want Q_T_F, no need for DFS, and no need to do THEN since we hit one shard
            searchRequest.searchType(QUERY_THEN_FETCH);
        }
        if (searchRequest.allowPartialSearchResults() == null) {
           // No user preference defined in search request - apply cluster service default
            searchRequest.allowPartialSearchResults(searchService.defaultAllowPartialSearchResults());
        }
        if (searchRequest.isSuggestOnly()) {
            // disable request cache if we have only suggest
            searchRequest.requestCache(false);
            switch (searchRequest.searchType()) {
                case DFS_QUERY_THEN_FETCH:
                    // convert to Q_T_F if we have only suggest
                    searchRequest.searchType(QUERY_THEN_FETCH);
                    break;
            }
        }

        final DiscoveryNodes nodes = clusterState.nodes();
        BiFunction<String, String, Transport.Connection> connectionLookup = buildConnectionLookup(searchRequest.getLocalClusterAlias(),
            nodes::get, remoteConnections, searchTransportService::getConnection);
        boolean preFilterSearchShards = shouldPreFilterSearchShards(clusterState, searchRequest, indices, shardIterators.size());
        searchAsyncAction(task, searchRequest, shardIterators, timeProvider, connectionLookup, clusterState,
            Collections.unmodifiableMap(aliasFilter), concreteIndexBoosts, routingMap, listener, preFilterSearchShards, clusters).start();
    }

    static BiFunction<String, String, Transport.Connection> buildConnectionLookup(String requestClusterAlias,
                                                              Function<String, DiscoveryNode> localNodes,
                                                              BiFunction<String, String, DiscoveryNode> remoteNodes,
                                                              BiFunction<String, DiscoveryNode, Transport.Connection> nodeToConnection) {
        return (clusterAlias, nodeId) -> {
            final DiscoveryNode discoveryNode;
            final boolean remoteCluster;
            if (clusterAlias == null || requestClusterAlias != null) {
                assert requestClusterAlias == null || requestClusterAlias.equals(clusterAlias);
                discoveryNode = localNodes.apply(nodeId);
                remoteCluster = false;
            } else {
                discoveryNode = remoteNodes.apply(clusterAlias, nodeId);
                remoteCluster = true;
            }
            if (discoveryNode == null) {
                throw new IllegalStateException("no node found for id: " + nodeId);
            }
            return nodeToConnection.apply(remoteCluster ? clusterAlias : null, discoveryNode);
        };
    }

    static boolean shouldPreFilterSearchShards(ClusterState clusterState,
                                               SearchRequest searchRequest,
                                               Index[] indices,
                                               int numShards) {
        SearchSourceBuilder source = searchRequest.source();
        Integer preFilterShardSize = searchRequest.getPreFilterShardSize();
        if (preFilterShardSize == null
                && (hasReadOnlyIndices(indices, clusterState) || hasPrimaryFieldSort(source))) {
            preFilterShardSize = 1;
        } else if (preFilterShardSize == null) {
            preFilterShardSize = SearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE;
        }
        return searchRequest.searchType() == QUERY_THEN_FETCH // we can't do this for DFS it needs to fan out to all shards all the time
                    && (SearchService.canRewriteToMatchNone(source) || hasPrimaryFieldSort(source))
                    && preFilterShardSize < numShards;
    }

    private static boolean hasReadOnlyIndices(Index[] indices, ClusterState clusterState) {
        for (Index index : indices) {
            ClusterBlockException writeBlock = clusterState.blocks().indexBlockedException(ClusterBlockLevel.WRITE, index.getName());
            if (writeBlock != null) {
                return true;
            }
        }
        return false;
    }

    static GroupShardsIterator<SearchShardIterator> mergeShardsIterators(GroupShardsIterator<ShardIterator> localShardsIterator,
                                                             OriginalIndices localIndices,
                                                             @Nullable String localClusterAlias,
                                                             List<SearchShardIterator> remoteShardIterators) {
        List<SearchShardIterator> shards = new ArrayList<>(remoteShardIterators);
        for (ShardIterator shardIterator : localShardsIterator) {
            shards.add(new SearchShardIterator(localClusterAlias, shardIterator.shardId(), shardIterator.getShardRoutings(), localIndices));
        }
        return GroupShardsIterator.sortAndCreate(shards);
    }

    private AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction(SearchTask task, SearchRequest searchRequest,
                                                        GroupShardsIterator<SearchShardIterator> shardIterators,
                                                        SearchTimeProvider timeProvider,
                                                        BiFunction<String, String, Transport.Connection> connectionLookup,
                                                        ClusterState clusterState,
                                                        Map<String, AliasFilter> aliasFilter,
                                                        Map<String, Float> concreteIndexBoosts,
                                                        Map<String, Set<String>> indexRoutings,
                                                        ActionListener<SearchResponse> listener,
                                                        boolean preFilter,
                                                        SearchResponse.Clusters clusters) {
        Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
        if (preFilter) {
            return new CanMatchPreFilterSearchPhase(logger, searchTransportService, connectionLookup,
                aliasFilter, concreteIndexBoosts, indexRoutings, executor, searchRequest, listener, shardIterators,
                timeProvider, clusterState, task, (iter) -> {
                AbstractSearchAsyncAction<? extends SearchPhaseResult> action = searchAsyncAction(
                    task,
                    searchRequest,
                    iter,
                    timeProvider,
                    connectionLookup,
                    clusterState,
                    aliasFilter,
                    concreteIndexBoosts,
                    indexRoutings,
                    listener,
                    false,
                    clusters);
                return new SearchPhase(action.getName()) {
                    @Override
                    public void run() {
                        action.start();
                    }
                };
            }, clusters);
        } else {
            AbstractSearchAsyncAction<? extends SearchPhaseResult> searchAsyncAction;
            switch (searchRequest.searchType()) {
                case DFS_QUERY_THEN_FETCH:
                    searchAsyncAction = new SearchDfsQueryThenFetchAsyncAction(logger, searchTransportService, connectionLookup,
                        aliasFilter, concreteIndexBoosts, indexRoutings, searchPhaseController, executor, searchRequest, listener,
                        shardIterators, timeProvider, clusterState, task, clusters);
                    break;
                case QUERY_THEN_FETCH:
                    searchAsyncAction = new SearchQueryThenFetchAsyncAction(logger, searchTransportService, connectionLookup,
                        aliasFilter, concreteIndexBoosts, indexRoutings, searchPhaseController, executor, searchRequest, listener,
                        shardIterators, timeProvider, clusterState, task, clusters);
                    break;
                default:
                    throw new IllegalStateException("Unknown search type: [" + searchRequest.searchType() + "]");
            }
            return searchAsyncAction;
        }
    }

    private static void failIfOverShardCountLimit(ClusterService clusterService, int shardCount) {
        final long shardCountLimit = clusterService.getClusterSettings().get(SHARD_COUNT_LIMIT_SETTING);
        if (shardCount > shardCountLimit) {
            throw new IllegalArgumentException("Trying to query " + shardCount + " shards, which is over the limit of "
                + shardCountLimit + ". This limit exists because querying many shards at the same time can make the "
                + "job of the coordinating node very CPU and/or memory intensive. It is usually a better idea to "
                + "have a smaller number of larger shards. Update [" + SHARD_COUNT_LIMIT_SETTING.getKey()
                + "] to a greater value if you really want to query that many shards at the same time.");
        }
    }

    abstract static class CCSActionListener<Response, FinalResponse> implements ActionListener<Response> {
        private final String clusterAlias;
        private final boolean skipUnavailable;
        private final CountDown countDown;
        private final AtomicInteger skippedClusters;
        private final AtomicReference<Exception> exceptions;
        private final ActionListener<FinalResponse> originalListener;

        CCSActionListener(String clusterAlias, boolean skipUnavailable, CountDown countDown, AtomicInteger skippedClusters,
                          AtomicReference<Exception> exceptions, ActionListener<FinalResponse> originalListener) {
            this.clusterAlias = clusterAlias;
            this.skipUnavailable = skipUnavailable;
            this.countDown = countDown;
            this.skippedClusters = skippedClusters;
            this.exceptions = exceptions;
            this.originalListener = originalListener;
        }

        @Override
        public final void onResponse(Response response) {
            innerOnResponse(response);
            maybeFinish();
        }

        abstract void innerOnResponse(Response response);

        @Override
        public final void onFailure(Exception e) {
            if (skipUnavailable) {
                skippedClusters.incrementAndGet();
            } else {
                Exception exception = e;
                if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
                    exception = wrapRemoteClusterFailure(clusterAlias, e);
                }
                if (exceptions.compareAndSet(null, exception) == false) {
                    exceptions.accumulateAndGet(exception, (previous, current) -> {
                        current.addSuppressed(previous);
                        return current;
                    });
                }
            }
            maybeFinish();
        }

        private void maybeFinish() {
            if (countDown.countDown()) {
                Exception exception = exceptions.get();
                if (exception == null) {
                    FinalResponse response;
                    try {
                        response = createFinalResponse();
                    } catch(Exception e) {
                        originalListener.onFailure(e);
                        return;
                    }
                    originalListener.onResponse(response);
                } else {
                    originalListener.onFailure(exceptions.get());
                }
            }
        }

        abstract FinalResponse createFinalResponse();
    }

    private static RemoteTransportException wrapRemoteClusterFailure(String clusterAlias, Exception e) {
        return new RemoteTransportException("error while communicating with remote cluster [" + clusterAlias + "]", e);
    }
}
