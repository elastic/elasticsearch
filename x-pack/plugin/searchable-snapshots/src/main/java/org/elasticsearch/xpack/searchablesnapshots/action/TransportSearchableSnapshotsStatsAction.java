/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.CacheIndexInputStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.Counter;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.TimedCounter;
import org.elasticsearch.index.store.IndexInputStats;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TransportSearchableSnapshotsStatsAction extends AbstractTransportSearchableSnapshotsAction<
    SearchableSnapshotsStatsRequest,
    SearchableSnapshotsStatsResponse,
    SearchableSnapshotShardStats> {
    @Inject
    public TransportSearchableSnapshotsStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        XPackLicenseState licenseState
    ) {
        super(
            SearchableSnapshotsStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            SearchableSnapshotsStatsRequest::new,
            ThreadPool.Names.MANAGEMENT,
            indicesService,
            licenseState
        );
    }

    @Override
    protected SearchableSnapshotShardStats readShardResult(StreamInput in) throws IOException {
        return new SearchableSnapshotShardStats(in);
    }

    @Override
    protected SearchableSnapshotsStatsResponse newResponse(
        SearchableSnapshotsStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<SearchableSnapshotShardStats> shardsStats,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new SearchableSnapshotsStatsResponse(shardsStats, totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected SearchableSnapshotsStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new SearchableSnapshotsStatsRequest(in);
    }

    @Override
    protected SearchableSnapshotShardStats executeShardOperation(
        SearchableSnapshotsStatsRequest request,
        ShardRouting shardRouting,
        SearchableSnapshotDirectory directory
    ) {
        return new SearchableSnapshotShardStats(
            shardRouting,
            directory.getSnapshotId(),
            directory.getIndexId(),
            directory.getStats()
                .entrySet()
                .stream()
                .map(entry -> toCacheIndexInputStats(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList())
        );
    }

    private static CacheIndexInputStats toCacheIndexInputStats(final String fileName, final IndexInputStats inputStats) {
        return new CacheIndexInputStats(
            fileName,
            inputStats.getFileLength(),
            inputStats.getOpened().sum(),
            inputStats.getClosed().sum(),
            toCounter(inputStats.getForwardSmallSeeks()),
            toCounter(inputStats.getBackwardSmallSeeks()),
            toCounter(inputStats.getForwardLargeSeeks()),
            toCounter(inputStats.getBackwardLargeSeeks()),
            toCounter(inputStats.getContiguousReads()),
            toCounter(inputStats.getNonContiguousReads()),
            toCounter(inputStats.getCachedBytesRead()),
            toTimedCounter(inputStats.getCachedBytesWritten()),
            toTimedCounter(inputStats.getDirectBytesRead()),
            toTimedCounter(inputStats.getOptimizedBytesRead())
        );
    }

    private static Counter toCounter(final IndexInputStats.Counter counter) {
        return new Counter(counter.count(), counter.total(), counter.min(), counter.max());
    }

    private static TimedCounter toTimedCounter(final IndexInputStats.TimedCounter counter) {
        return new TimedCounter(counter.count(), counter.total(), counter.min(), counter.max(), counter.totalNanoseconds());
    }
}
