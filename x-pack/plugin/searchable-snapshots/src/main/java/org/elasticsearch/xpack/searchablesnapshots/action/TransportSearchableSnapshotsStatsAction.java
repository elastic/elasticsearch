/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.CacheIndexInputStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.Counter;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.TimedCounter;
import org.elasticsearch.xpack.searchablesnapshots.InMemoryNoOpCommitDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.IndexInputStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY;

public class TransportSearchableSnapshotsStatsAction extends TransportBroadcastByNodeAction<SearchableSnapshotsStatsRequest,
                                                                                            SearchableSnapshotsStatsResponse,
    SearchableSnapshotShardStats> {
    private final IndicesService indicesService;

    @Inject
    public TransportSearchableSnapshotsStatsAction(ClusterService clusterService, TransportService transportService,
                                                   IndicesService indicesService, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver indexNameExpressionResolver) {
        super(SearchableSnapshotsStatsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            SearchableSnapshotsStatsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SearchableSnapshotsStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, SearchableSnapshotsStatsRequest request, String[] indices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, indices);
    }

    @Override
    protected SearchableSnapshotShardStats readShardResult(StreamInput in) throws IOException {
        return new SearchableSnapshotShardStats(in);
    }

    @Override
    protected SearchableSnapshotsStatsResponse newResponse(SearchableSnapshotsStatsRequest request,
                                                           int totalShards, int successfulShards, int failedShards,
                                                           List<SearchableSnapshotShardStats> shardsStats,
                                                           List<DefaultShardOperationFailedException> shardFailures,
                                                           ClusterState clusterState) {
        return new SearchableSnapshotsStatsResponse(shardsStats, totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected SearchableSnapshotsStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new SearchableSnapshotsStatsRequest(in);
    }

    @Override
    protected ShardsIterator shards(ClusterState state, SearchableSnapshotsStatsRequest request, String[] concreteIndices) {
        final List<String> searchableSnapshotIndices = new ArrayList<>();
        for (String concreteIndex : concreteIndices) {
            IndexMetaData indexMetaData = state.metaData().index(concreteIndex);
            if (indexMetaData != null) {
                Settings indexSettings = indexMetaData.getSettings();
                if (INDEX_STORE_TYPE_SETTING.get(indexSettings).equals(SNAPSHOT_DIRECTORY_FACTORY_KEY)) {
                    searchableSnapshotIndices.add(concreteIndex);
                }
            }
        }
        if (searchableSnapshotIndices.isEmpty()) {
            throw new ResourceNotFoundException("No searchable snapshots indices found");
        }
        return state.routingTable().allShards(searchableSnapshotIndices.toArray(new String[0]));
    }

    @Override
    protected SearchableSnapshotShardStats shardOperation(SearchableSnapshotsStatsRequest request, ShardRouting shardRouting) {
        final IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.index()).getShard(shardRouting.id());
        final CacheDirectory cacheDirectory = unwrap(indexShard.store().directory());
        assert cacheDirectory != null;
        assert cacheDirectory.getShardId().equals(shardRouting.shardId());

        return new SearchableSnapshotShardStats(shardRouting, cacheDirectory.getSnapshotId(), cacheDirectory.getIndexId(),
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
            toCounter(inputStats.getCachedBytesRead()), toTimedCounter(inputStats.getCachedBytesWritten()),
            toTimedCounter(inputStats.getDirectBytesRead()));
    }

    private static Counter toCounter(final IndexInputStats.Counter counter) {
        return new Counter(counter.count(), counter.total(), counter.min(), counter.max());
    }

    private static TimedCounter toTimedCounter(final IndexInputStats.TimedCounter counter) {
        return new TimedCounter(counter.count(), counter.total(), counter.min(), counter.max(), counter.totalNanoseconds());
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
