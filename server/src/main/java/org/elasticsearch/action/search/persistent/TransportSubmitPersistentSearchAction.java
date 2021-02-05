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
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.persistent.PersistentSearchId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

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

        final List<SearchShard> searchShards = resolveShards(clusterState, indices);

        final String persistentSearchDocId = UUIDs.randomBase64UUID();
        final PersistentSearchId persistentSearchId =
            new PersistentSearchId(persistentSearchDocId, new TaskId(nodeClient.getLocalNodeId(), task.getId()));

        final Map<String, AliasFilter> aliasFilterMap = buildPerIndexAliasFilter(request, clusterState, indices);

        final TimeValue expirationTime = TimeValue.timeValueMinutes(10);
        new AsyncPersistentSearch(request,
            persistentSearchDocId,
            searchTask,
            searchShards,
            localIndices,
            aliasFilterMap,
            expirationTime,
            4,
            searchShards.size(),
            timeProvider,
            searchShardTargetResolver,
            searchTransportService,
            threadPool,
            connectionProvider(),
            clusterService,
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    taskManager.unregister(searchTask);
                }

                @Override
                public void onFailure(Exception e) {

                }
            }
        ).start();


        listener.onResponse(new SubmitPersistentSearchResponse(persistentSearchId));
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

    private List<SearchShard> resolveShards(ClusterState clusterState, Index[] indices) {
        List<SearchShard> searchShards = new ArrayList<>();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().index(index);
            // We're reusing the same ClusterState that we used to expand the indices
            assert indexMetadata != null;
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                searchShards.add(new SearchShard(null, new ShardId(index, i)));
            }
        }
        return Collections.unmodifiableList(searchShards);
    }
}
