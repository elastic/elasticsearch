/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.stream.StreamSupport;

public class IndicesWriteLoadStatsCollector implements IndexEventListener, ClusterStateListener {
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final ConcurrentMap<ShardId, ShardWriteLoadHistogram> histograms = ConcurrentCollections.newConcurrentMap();
    private final AtomicBoolean clusterStateReady = new AtomicBoolean();

    public IndicesWriteLoadStatsCollector(
        ClusterService clusterService,
        IndicesService indicesService,
        LongSupplier relativeTimeInNanosSupplier
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        clusterService.addListener(this);
    }

    void collectWriteLoadStats() {
        for (IndexShard indexShard : getDataStreamsWriteIndicesShards()) {
            final var parentDataStreamName = getParentDataStreamName(indexShard.shardId().getIndexName());
            final var shardWriteLoadHistogram = histograms.computeIfAbsent(
                indexShard.shardId(),
                shardId -> new ShardWriteLoadHistogram(
                    parentDataStreamName,
                    shardId,
                    indexShard.routingEntry().primary(),
                    relativeTimeInNanosSupplier
                )
            );
            long totalIndexingTime = Math.addExact(indexShard.getTotalIndexingTimeInNanos(), indexShard.getTotalDeleteTimeInNanos());
            long totalMergeTimeInMillis = indexShard.getTotalMergeTimeInMillis();
            long totalRefreshTimeInNanos = indexShard.getTotalRefreshTimeInNanos();
            shardWriteLoadHistogram.recordSample(totalIndexingTime, totalMergeTimeInMillis, totalRefreshTimeInNanos);
        }
    }

    List<IndexShard> getDataStreamsWriteIndicesShards() {
        // Wait until the node has access to the cluster state
        if (clusterStateReady.get() == false) {
            return Collections.emptyList();
        }

        return StreamSupport.stream(indicesService.spliterator(), false)
            .filter(indexService -> isDataStreamWriteIndex(indexService.index()))
            .flatMap(indexService -> StreamSupport.stream(indexService.spliterator(), false))
            .toList();
    }

    private void cleanRolledOverIndices() {
        histograms.entrySet().removeIf(entry -> {
            final var shardId = entry.getKey();
            return isDataStreamWriteIndex(shardId.getIndex()) == false;
        });
    }

    protected Iterable<IndexShard> getShardsForIndex(Index index) {
        // If the shards are allocated somewhere else, just return an empty list
        if (indicesService.hasIndex(index) == false) {
            return Collections.emptyList();
        }

        return indicesService.indexServiceSafe(index);
    }

    @Nullable
    String getParentDataStreamName(String indexName) {
        IndexAbstraction indexAbstraction = clusterService.state().getMetadata().getIndicesLookup().get(indexName);
        IndexAbstraction.DataStream parentDataStream = indexAbstraction.getParentDataStream();
        return parentDataStream != null ? parentDataStream.getName() : null;
    }

    private boolean isDataStreamWriteIndex(Index index) {
        final var indicesLookup = clusterService.state().metadata().getIndicesLookup();
        final var indexAbstraction = indicesLookup.get(index.getName());
        if (indexAbstraction == null) {
            return false;
        }

        final var parentDataStream = indexAbstraction.getParentDataStream();
        return parentDataStream != null && index.equals(parentDataStream.getWriteIndex());
    }

    public List<ShardWriteLoadDistribution> getWriteLoadDistributionAndReset() {
        final var shardWriteLoadDistributions = histograms.values()
            .stream()
            .map(ShardWriteLoadHistogram::getWriteLoadDistributionAndReset)
            .toList();

        cleanRolledOverIndices();

        return shardWriteLoadDistributions;
    }

    public List<ShardWriteLoadDistribution> getShardLoadDistributions() {
        return histograms.values().stream().map(ShardWriteLoadHistogram::getWriteLoadDistribution).toList();
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        histograms.remove(shardId);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (clusterStateReady.compareAndSet(false, true)) {
            clusterService.removeListener(this);
        }
    }

    private static class ShardWriteLoadHistogram {
        @Nullable
        private final String parentDataStreamName;
        private final ShardId shardId;
        private final boolean primary;
        private final LongSupplier relativeTimeInNanosSupplier;
        private final Object histogramsLock = new Object();
        private DoubleHistogram indexingTimeHistogram;
        private DoubleHistogram mergeTimeHistogram;
        private DoubleHistogram refreshTimeHistogram;
        private volatile long lastTotalIndexingTimeSample;
        private volatile long lastTotalMergeTimeSample;
        private volatile long lastTotalRefreshTimeSample;
        private volatile long lastSampleRelativeTimeInNanos;

        ShardWriteLoadHistogram(
            @Nullable String parentDataStreamName,
            ShardId shardId,
            boolean primary,
            LongSupplier relativeTimeInNanosSupplier
        ) {
            this.parentDataStreamName = parentDataStreamName;
            this.shardId = shardId;
            this.primary = primary;
            this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
            this.indexingTimeHistogram = createHistogram();
            this.mergeTimeHistogram = createHistogram();
            this.refreshTimeHistogram = createHistogram();
            this.lastSampleRelativeTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
        }

        void recordSample(long totalIndexingTimeInNanosSample, long totalMergeTimeInMillisSample, long totalRefreshTimeInNanos) {
            long sampleRelativeTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
            long samplingTimeInNanos = sampleRelativeTimeInNanos - lastSampleRelativeTimeInNanos;
            lastSampleRelativeTimeInNanos = sampleRelativeTimeInNanos;

            long indexingTimeDeltaInNanos = totalIndexingTimeInNanosSample - lastTotalIndexingTimeSample;
            lastTotalIndexingTimeSample = totalIndexingTimeInNanosSample;

            long totalMergeTimeInNanosSample = TimeUnit.MILLISECONDS.toNanos(totalMergeTimeInMillisSample);
            long mergeTimeDeltaInNanos = totalMergeTimeInNanosSample - lastTotalMergeTimeSample;
            lastTotalMergeTimeSample = totalMergeTimeInMillisSample;

            long refreshTimeDeltaInNanos = totalRefreshTimeInNanos - lastTotalRefreshTimeSample;
            lastTotalRefreshTimeSample = totalRefreshTimeInNanos;

            // Don't take a sample if the clock doesn't provide enough granularity between samples, or we're taking the first sample.
            if (samplingTimeInNanos <= 0 || indexingTimeDeltaInNanos == totalIndexingTimeInNanosSample) {
                return;
            }

            double indexingCPUs = indexingTimeDeltaInNanos / (double) samplingTimeInNanos;
            double mergeCPUs = mergeTimeDeltaInNanos / (double) samplingTimeInNanos;
            double refreshCPUs = refreshTimeDeltaInNanos / (double) samplingTimeInNanos;

            synchronized (histogramsLock) {
                indexingTimeHistogram.recordValue(indexingCPUs);
                mergeTimeHistogram.recordValue(mergeCPUs);
                refreshTimeHistogram.recordValue(refreshCPUs);
            }
        }

        ShardWriteLoadDistribution getWriteLoadDistributionAndReset() {
            synchronized (histogramsLock) {
                ShardWriteLoadDistribution indexLoadSummary = getWriteLoadDistribution();
                indexingTimeHistogram = createHistogram();
                mergeTimeHistogram = createHistogram();
                refreshTimeHistogram = createHistogram();
                return indexLoadSummary;
            }
        }

        ShardWriteLoadDistribution getWriteLoadDistribution() {
            synchronized (histogramsLock) {
                return new ShardWriteLoadDistribution(
                    System.currentTimeMillis(),
                    parentDataStreamName,
                    shardId,
                    primary,
                    indexingTimeHistogram.getValueAtPercentile(50),
                    indexingTimeHistogram.getValueAtPercentile(90),
                    indexingTimeHistogram.getMaxValue(),
                    mergeTimeHistogram.getValueAtPercentile(50),
                    mergeTimeHistogram.getValueAtPercentile(90),
                    mergeTimeHistogram.getMaxValue(),
                    refreshTimeHistogram.getValueAtPercentile(50),
                    refreshTimeHistogram.getValueAtPercentile(90),
                    refreshTimeHistogram.getMaxValue()
                );
            }
        }

        private static DoubleHistogram createHistogram() {
            return new DoubleHistogram(5);
        }
    }
}
