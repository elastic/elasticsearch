/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.persistent.PersistentSearchId;
import org.elasticsearch.search.persistent.PersistentSearchShard;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

public class TransportSubmitPersistentSearchAction extends HandledTransportAction<SearchRequest, SubmitPersistentSearchResponse> {
    private final NodeClient nodeClient;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final RemoteClusterService remoteClusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SearchShardTargetResolver searchShardTargetResolver;
    private final SearchTransportService searchTransportService;
    private final SearchService searchService;

    @Inject
    public TransportSubmitPersistentSearchAction(TransportService transportService,
                                                 ActionFilters actionFilters,
                                                 NodeClient nodeClient,
                                                 ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                                 SearchShardTargetResolver searchShardTargetResolver,
                                                 SearchTransportService searchTransportService,
                                                 SearchService searchService) {
        super(SubmitPersistentSearchAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.nodeClient = nodeClient;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.searchShardTargetResolver = searchShardTargetResolver;
        this.searchTransportService = searchTransportService;
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SubmitPersistentSearchResponse> listener) {
        // Register a new task, since we'll be responding directly with the persistent search ID and the task will be
        // unregistered afterwards
        SearchTask searchTask = (SearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), request);
        final long relativeStartNanos = System.nanoTime();
        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(request.getOrCreateAbsoluteStartMillis(), relativeStartNanos, System::nanoTime);

        final Map<String, OriginalIndices> resolvedIndices = remoteClusterService.groupIndices(request.indicesOptions(), request.indices());
        final OriginalIndices localIndices = resolvedIndices.remove(LOCAL_CLUSTER_GROUP_KEY);
        if (resolvedIndices.isEmpty() == false) {
            listener.onFailure(new RuntimeException("Unable to run a CCS under this mode"));
            return;
        }

        final ClusterState clusterState = clusterService.state();
        final Index[] indices =
            indexNameExpressionResolver.concreteIndices(clusterState, localIndices, timeProvider.getAbsoluteStartMillis());

        final Map<String, AliasFilter> aliasFilterMap = buildPerIndexAliasFilter(request, clusterState, indices);

        final String persistentSearchDocId = UUIDs.randomBase64UUID();

        final PersistentSearchId persistentSearchId =
            new PersistentSearchId(persistentSearchDocId, new TaskId(nodeClient.getLocalNodeId(), task.getId()));

        final List<PersistentSearchShard> resolvedShards = resolveShards(persistentSearchDocId, clusterState, indices);
        final Map<String, Float> indicesBoost = resolveIndexBoosts(request, clusterState);

        ShardSearchRequestProvider shardSearchRequestProvider =
            new ShardSearchRequestProvider(request, localIndices, timeProvider, aliasFilterMap, indicesBoost);

        final boolean canRewriteToMatchNone = SearchService.canRewriteToMatchNone(request.source());

        if (canRewriteToMatchNone) {
            StepListener<Collection<CanMatchShardIterator>> shardsResolverListener = new StepListener<>();
            StepListener<List<PersistentSearchShard>> canMatchPhaseListener = new StepListener<>();

            shardsResolverListener.whenComplete(canMatchShardIterators -> {
                new CanMatchPhase(request,
                    searchTask,
                    List.copyOf(canMatchShardIterators),
                    searchTransportService,
                    connectionProvider(),
                    canMatchPhaseListener).run();
            }, listener::onFailure);

            canMatchPhaseListener.whenComplete(persistentSearchShards -> {
                runAsyncPersistentSearch(persistentSearchShards,
                    request,
                    persistentSearchDocId,
                    searchTask,
                    localIndices,
                    shardSearchRequestProvider,
                    timeProvider
                );
            }, listener::onFailure);

            GroupedActionListener<CanMatchShardIterator> canMatchShardIteratorsListener =
                new GroupedActionListener<>(shardsResolverListener, resolvedShards.size());

            for (int i = 0; i < resolvedShards.size(); i++) {
                final PersistentSearchShard searchShard = resolvedShards.get(i);
                final int shardIndex = i;
                searchShardTargetResolver.resolve(searchShard.getSearchShard(), localIndices, new ActionListener<>() {
                    @Override
                    public void onResponse(SearchShardIterator searchShardIterator) {
                        ShardSearchRequest shardSearchRequest =
                            shardSearchRequestProvider.createRequest(searchShard.getShardId(), shardIndex, resolvedShards.size());
                        CanMatchShardIterator canMatchShardIterator =
                            new CanMatchShardIterator(searchShard, shardSearchRequest, searchShardIterator);
                        canMatchShardIteratorsListener.onResponse(canMatchShardIterator);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        canMatchShardIteratorsListener.onFailure(e);
                    }
                });
            }
        } else {
            runAsyncPersistentSearch(resolvedShards,
                request,
                persistentSearchDocId,
                searchTask,
                localIndices,
                shardSearchRequestProvider,
                timeProvider
            );
        }

        listener.onResponse(new SubmitPersistentSearchResponse(persistentSearchId));
    }

    private void runAsyncPersistentSearch(List<PersistentSearchShard> searchShards,
                                          SearchRequest request,
                                          String persistentSearchId,
                                          SearchTask searchTask,
                                          OriginalIndices localIndices,
                                          ShardSearchRequestProvider shardSearchRequestProvider,
                                          TransportSearchAction.SearchTimeProvider timeProvider) {
        final TimeValue expirationTime = TimeValue.timeValueMinutes(10);
        new AsyncPersistentSearch(
            persistentSearchId,
            request,
            searchTask,
            searchShards,
            localIndices,
            expirationTime,
            Integer.MAX_VALUE, // Unbounded for now
            searchShards.size(), // Run reduce at the end
            shardSearchRequestProvider,
            timeProvider,
            searchShardTargetResolver,
            searchTransportService,
            threadPool,
            connectionProvider(),
            clusterService,
            ActionListener.wrap(() -> taskManager.unregister(searchTask))
        ).start();
    }

    private Map<String, AliasFilter> buildPerIndexAliasFilter(SearchRequest request, ClusterState clusterState, Index[] concreteIndices) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), indicesAndAliases);
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        return aliasFilterMap;
    }

    private BiFunction<String, String, Transport.Connection> connectionProvider() {
        return (cluster, nodeId) -> {
            final DiscoveryNode node = clusterService.state().nodes().get(nodeId);
            return searchTransportService.getConnection(null, node);
        };
    }

    private List<PersistentSearchShard> resolveShards(String searchId, ClusterState clusterState, Index[] indices) {
        List<PersistentSearchShard> searchShards = new ArrayList<>();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().index(index);
            // We're reusing the same ClusterState that we used to expand the indices
            assert indexMetadata != null;
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                String shardId = UUIDs.randomBase64UUID();
                PersistentSearchShard persistentSearchShard =
                    new PersistentSearchShard(shardId, searchId, new SearchShard(null, new ShardId(index, i)));
                searchShards.add(persistentSearchShard);
            }
        }
        return Collections.unmodifiableList(searchShards);
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

    private static class CanMatchPhase {
        private final SearchRequest searchRequest;
        private final SearchTask searchTask;
        private final List<CanMatchShardIterator> shardIterators;
        private final SearchTransportService searchTransportService;
        private final BiFunction<String, String, Transport.Connection> connectionProvider;
        private final CountDown shardExecutions;
        private final BitSet canMatchShard;
        private final MinAndMax<?>[] minMaxValues;
        private final ActionListener<List<PersistentSearchShard>> listener;

        CanMatchPhase(SearchRequest searchRequest,
                      SearchTask searchTask,
                      List<CanMatchShardIterator> shardIterators,
                      SearchTransportService searchTransportService,
                      BiFunction<String, String, Transport.Connection> connectionProvider,
                      ActionListener<List<PersistentSearchShard>> listener) {
            this.searchRequest = searchRequest;
            this.searchTask = searchTask;
            this.shardIterators = shardIterators;
            this.searchTransportService = searchTransportService;
            this.connectionProvider = connectionProvider;
            this.shardExecutions = new CountDown(shardIterators.size());
            this.canMatchShard = new BitSet(shardIterators.size());
            this.minMaxValues = new MinAndMax[shardIterators.size()];
            this.listener = listener;
        }

        void run() {
            for (int i = 0; i < shardIterators.size(); i++) {
                executeCanMatchOnShard(shardIterators.get(i), i);
            }
        }

        void executeCanMatchOnShard(CanMatchShardIterator searchShard, int shardIndex) {
            final SearchShardTarget searchShardTarget = searchShard.nextOrNull();
            if (searchShardTarget == null) {
                onShardFailure(shardIndex);
                return;
            }

            Transport.Connection connection = connectionProvider.apply(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
            ShardSearchRequest shardSearchRequest = searchShard.getShardSearchRequest();
            searchTransportService.sendCanMatch(connection, shardSearchRequest, searchTask, ActionListener.wrap(response -> {
                    response.setShardIndex(shardIndex);
                    onShardSuccess(response);
                }, e -> executeCanMatchOnShard(searchShard, shardIndex)));
        }

        void onShardSuccess(SearchService.CanMatchResponse canMatchResponse) {
            recordCanMatchResponse(canMatchResponse.getShardIndex(), canMatchResponse.canMatch(), canMatchResponse.estimatedMinAndMax());
            onShardExecuted();
        }

        void onShardFailure(int shardIndex) {
            // By default try to match against that shard
            recordCanMatchResponse(shardIndex, true, null);
            onShardExecuted();
        }

        synchronized void recordCanMatchResponse(int shardIndex, boolean canMatch, @Nullable MinAndMax<?> minAndMax) {
            if (canMatch) {
                canMatchShard.set(shardIndex);
            }
            minMaxValues[shardIndex] = minAndMax;
        }

        void onShardExecuted() {
            if (shardExecutions.countDown()) {
                final List<PersistentSearchShard> sortedShards = getSortedAndSkippedShards()
                    .stream()
                    .map(CanMatchShardIterator::getPersistentSearchShard)
                    .collect(Collectors.toList());
                listener.onResponse(sortedShards);
            }
        }

        private List<CanMatchShardIterator> getSortedAndSkippedShards() {
            int skippedShards = 0;
            for (int i = 0; i < shardIterators.size(); i++) {
                final CanMatchShardIterator searchShardIterator = shardIterators.get(i);
                if (canMatchShard.get(i) == false) {
                    searchShardIterator.setCanBeSkipped(true);
                    skippedShards++;
                }
            }

            if (skippedShards == shardIterators.size()) {
                shardIterators.get(0).setCanBeSkipped(false);
            }

            if (shouldSortShards(minMaxValues) == false) {
                return Collections.unmodifiableList(shardIterators);
            }

            FieldSortBuilder fieldSort = FieldSortBuilder.getPrimaryFieldSortOrNull(searchRequest.source());
            return sortShards(shardIterators, minMaxValues, fieldSort.order());
        }

        private static List<CanMatchShardIterator> sortShards(List<CanMatchShardIterator> shardsIts,
                                                              MinAndMax<?>[] minAndMaxes,
                                                              SortOrder order) {
            return IntStream.range(0, shardsIts.size())
                .boxed()
                .sorted(shardComparator(shardsIts, minAndMaxes,  order))
                .map(shardsIts::get)
                .collect(Collectors.toList());
        }

        private static Comparator<Integer> shardComparator(List<CanMatchShardIterator> searchShards,
                                                           MinAndMax<?>[] minAndMaxes,
                                                           SortOrder order) {
            final Comparator<Integer> comparator = Comparator.comparing(index -> minAndMaxes[index], MinAndMax.getComparator(order));
            return comparator.thenComparing(searchShards::get);
        }

        private static boolean shouldSortShards(MinAndMax<?>[] minAndMaxes) {
            Class<?> clazz = null;
            for (MinAndMax<?> minAndMax : minAndMaxes) {
                if (clazz == null) {
                    clazz = minAndMax == null ? null : minAndMax.getMin().getClass();
                } else if (minAndMax != null && clazz != minAndMax.getMin().getClass()) {
                    // we don't support sort values that mix different types (e.g.: long/double, numeric/keyword).
                    return false;
                }
            }
            return clazz != null;
        }
    }

    private static class CanMatchShardIterator implements Comparable<CanMatchShardIterator> {
        private final PersistentSearchShard persistentSearchShard;
        private final ShardSearchRequest shardSearchRequest;
        private final SearchShardIterator searchShardIterator;

        private CanMatchShardIterator(PersistentSearchShard persistentSearchShard,
                              ShardSearchRequest shardSearchRequest,
                              SearchShardIterator searchShardIterator) {
            this.shardSearchRequest = shardSearchRequest;
            this.searchShardIterator = searchShardIterator;
            this.persistentSearchShard = persistentSearchShard;
        }

        SearchShardTarget nextOrNull() {
            return searchShardIterator.nextOrNull();
        }

        ShardSearchRequest getShardSearchRequest() {
            return shardSearchRequest;
        }

        PersistentSearchShard getPersistentSearchShard() {
            return persistentSearchShard;
        }

        void setCanBeSkipped(boolean canBeSkipped) {
            persistentSearchShard.setCanBeSkipped(canBeSkipped);
        }

        @Override
        public int compareTo(CanMatchShardIterator o) {
            return persistentSearchShard.compareTo(o.persistentSearchShard);
        }
    }
}
