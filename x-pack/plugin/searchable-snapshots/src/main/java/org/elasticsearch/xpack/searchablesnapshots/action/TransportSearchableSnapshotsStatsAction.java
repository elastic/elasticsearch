/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.CacheIndexInputStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.Counter;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.TimedCounter;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
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
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT),
            indicesService,
            licenseState
        );
    }

    @Override
    protected SearchableSnapshotShardStats readShardResult(StreamInput in) throws IOException {
        return new SearchableSnapshotShardStats(in);
    }

    @Override
    protected ResponseFactory<SearchableSnapshotsStatsResponse, SearchableSnapshotShardStats> getResponseFactory(
        SearchableSnapshotsStatsRequest request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, shardsStats, shardFailures) -> new SearchableSnapshotsStatsResponse(
            shardsStats,
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
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

    private static CacheIndexInputStats toCacheIndexInputStats(final String fileExt, final IndexInputStats inputStats) {
        return new CacheIndexInputStats(
            fileExt,
            inputStats.getNumFiles(),
            ByteSizeValue.ofBytes(inputStats.getTotalSize()),
            ByteSizeValue.ofBytes(inputStats.getMinSize()),
            ByteSizeValue.ofBytes(inputStats.getMaxSize()),
            inputStats.getOpened().sum(),
            inputStats.getClosed().sum(),
            toCounter(inputStats.getForwardSmallSeeks()),
            toCounter(inputStats.getBackwardSmallSeeks()),
            toCounter(inputStats.getForwardLargeSeeks()),
            toCounter(inputStats.getBackwardLargeSeeks()),
            toCounter(inputStats.getContiguousReads()),
            toCounter(inputStats.getNonContiguousReads()),
            toCounter(inputStats.getCachedBytesRead()),
            toCounter(inputStats.getIndexCacheBytesRead()),
            toTimedCounter(inputStats.getCachedBytesWritten()),
            toTimedCounter(inputStats.getDirectBytesRead()),
            toTimedCounter(inputStats.getOptimizedBytesRead()),
            toCounter(inputStats.getBlobStoreBytesRequested()),
            toCounter(inputStats.getLuceneBytesRead()),
            inputStats.getCurrentIndexCacheFills()
        );
    }

    private static Counter toCounter(final IndexInputStats.Counter counter) {
        return new Counter(counter.count(), counter.total(), counter.min(), counter.max());
    }

    private static TimedCounter toTimedCounter(final IndexInputStats.TimedCounter counter) {
        return new TimedCounter(counter.count(), counter.total(), counter.min(), counter.max(), counter.totalNanoseconds());
    }
}
