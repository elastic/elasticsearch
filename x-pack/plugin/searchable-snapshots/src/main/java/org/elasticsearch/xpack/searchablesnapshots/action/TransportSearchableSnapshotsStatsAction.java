/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats.CacheDirectoryStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats.CacheIndexInputStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats.Counter;
import org.elasticsearch.xpack.searchablesnapshots.InMemoryNoOpCommitDirectory;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest.NodeStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsResponse.NodeStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.IndexInputStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY;

public class TransportSearchableSnapshotsStatsAction extends TransportNodesAction<SearchableSnapshotsStatsRequest,
    SearchableSnapshotsStatsResponse,
                                                                                NodeStatsRequest,
                                                                                NodeStatsResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportSearchableSnapshotsStatsAction(final ThreadPool threadPool,
                                                   final ClusterService clusterService,
                                                   final TransportService transportService,
                                                   final ActionFilters actionFilters,
                                                   final IndicesService indicesService) {
        super(SearchableSnapshotsStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
            SearchableSnapshotsStatsRequest::new, NodeStatsRequest::new, ThreadPool.Names.GENERIC, NodeStatsResponse.class);
        this.indicesService = Objects.requireNonNull(indicesService);
    }

    @Override
    protected SearchableSnapshotsStatsResponse newResponse(SearchableSnapshotsStatsRequest request, List<NodeStatsResponse> responses,
                                                           List<FailedNodeException> failures) {
        return new SearchableSnapshotsStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStatsRequest newNodeRequest(SearchableSnapshotsStatsRequest request) {
        return new NodeStatsRequest();
    }

    @Override
    protected NodeStatsResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeStatsResponse(in);
    }

    @Override
    protected NodeStatsResponse nodeOperation(final NodeStatsRequest request, final Task task) {
        final List<CacheDirectoryStats> directoryStats = new ArrayList<>();
        if (clusterService.localNode().isDataNode()) {
            for (IndexService indexService : indicesService) {
                Settings indexSettings = indexService.getIndexSettings().getSettings();
                if (INDEX_STORE_TYPE_SETTING.get(indexSettings).equals(SNAPSHOT_DIRECTORY_FACTORY_KEY)) {
                    for (IndexShard indexShard : indexService) {
                        CacheDirectory cacheDirectory = unwrap(indexShard.store().directory());
                        if (cacheDirectory != null) {
                            directoryStats.add(toDirectoryStats(cacheDirectory));
                        }
                    }
                }
            }
        }
        return new NodeStatsResponse(clusterService.localNode(), new SearchableSnapshotStats(directoryStats));
    }

    private static CacheDirectoryStats toDirectoryStats(final CacheDirectory cacheDirectory) {
        return new CacheDirectoryStats(cacheDirectory.getSnapshotId(), cacheDirectory.getIndexId(), cacheDirectory.getShardId(),
            cacheDirectory.getStats().entrySet().stream()
                .map(entry -> toCacheIndexInputStats(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));
    }

    private static CacheIndexInputStats toCacheIndexInputStats(final String fileName, final IndexInputStats inputStats) {
        return new CacheIndexInputStats(fileName, inputStats.getFileLength(),
            inputStats.getOpened().sum(), inputStats.getInnerOpened().sum(), inputStats.getClosed().sum(),
            toCounter(inputStats.getForwardSmallSeeks()), toCounter(inputStats.getBackwardSmallSeeks()),
            toCounter(inputStats.getForwardLargeSeeks()), toCounter(inputStats.getBackwardLargeSeeks()),
            toCounter(inputStats.getContiguousReads()), toCounter(inputStats.getNonContiguousReads()),
            toCounter(inputStats.getCachedBytesRead()), toCounter(inputStats.getCachedBytesWritten()),
            toCounter(inputStats.getDirectBytesRead()));
    }

    private static Counter toCounter(final IndexInputStats.Counter counter) {
        return new Counter(counter.count(), counter.total(), counter.min(), counter.max());
    }

    @Nullable
    private static CacheDirectory unwrap(Directory dir) {
        while (dir != null) {
            if (dir instanceof CacheDirectory) {
                return (CacheDirectory) dir;
            } else if (dir instanceof InMemoryNoOpCommitDirectory) {
                dir = ((InMemoryNoOpCommitDirectory) dir).getRealDirectory();
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        return null;
    }
}
