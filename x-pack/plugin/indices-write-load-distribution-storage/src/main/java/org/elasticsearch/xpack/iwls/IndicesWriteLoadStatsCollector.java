/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.iwls;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class IndicesWriteLoadStatsCollector implements IndexEventListener {
    private final ClusterService clusterService;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final ConcurrentMap<ShardId, ShardWriteLoadHistogram> histograms = ConcurrentCollections.newConcurrentMap();

    public IndicesWriteLoadStatsCollector(ClusterService clusterService, LongSupplier relativeTimeInNanosSupplier) {
        this.clusterService = clusterService;
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
    }

    public void collectWriteLoadStats() {
        for (ShardWriteLoadHistogram shardWriteLoadHistogram : histograms.values()) {
            shardWriteLoadHistogram.recordSample();
        }
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
    public void afterIndexShardStarted(IndexShard indexShard) {
        final var shardId = indexShard.shardId();
        if (indexShard.isDataStreamIndex()) {
            histograms.put(
                shardId,
                new ShardWriteLoadHistogram(
                    () -> getParentDataStreamName(shardId.getIndexName()),
                    indexShard,
                    indexShard.routingEntry().primary(),
                    relativeTimeInNanosSupplier
                )
            );
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        histograms.remove(shardId);
    }

    private void cleanRolledOverIndices() {
        histograms.entrySet().removeIf(entry -> {
            final var shardId = entry.getKey();
            return isDataStreamWriteIndex(shardId.getIndex()) == false;
        });
    }

    private static class ShardWriteLoadHistogram {
        @Nullable
        private final Supplier<String> parentDataStreamName;
        private final IndexShard indexShard;
        private final boolean primary;
        private final LongSupplier relativeTimeInNanosSupplier;
        private final Object histogramsLock = new Object();
        private DoubleHistogram indexingTimeHistogram;
        private DoubleHistogram mergeTimeHistogram;
        private DoubleHistogram refreshTimeHistogram;
        private long lastTotalIndexingTimeSample;
        private long lastTotalMergeTimeSample;
        private long lastTotalRefreshTimeSample;
        private long lastSampleRelativeTimeInNanos;

        ShardWriteLoadHistogram(
            Supplier<String> parentDataStreamName,
            IndexShard indexShard,
            boolean primary,
            LongSupplier relativeTimeInNanosSupplier
        ) {
            this.parentDataStreamName = parentDataStreamName;
            this.indexShard = indexShard;
            this.primary = primary;
            this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
            this.indexingTimeHistogram = createHistogram();
            this.mergeTimeHistogram = createHistogram();
            this.refreshTimeHistogram = createHistogram();
            this.lastSampleRelativeTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
        }

        void recordSample() {
            long totalIndexingTimeInNanosSample = Math.addExact(
                indexShard.getTotalIndexingTimeInNanos(),
                indexShard.getTotalDeleteTimeInNanos()
            );
            long totalMergeTimeInMillisSample = indexShard.getTotalMergeTimeInMillis();
            long totalRefreshTimeInNanos = indexShard.getTotalRefreshTimeInNanos();

            long sampleRelativeTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
            long samplingTimeInNanos = sampleRelativeTimeInNanos - lastSampleRelativeTimeInNanos;
            lastSampleRelativeTimeInNanos = sampleRelativeTimeInNanos;

            long indexingTimeDeltaInNanos = totalIndexingTimeInNanosSample - lastTotalIndexingTimeSample;
            lastTotalIndexingTimeSample = totalIndexingTimeInNanosSample;

            long totalMergeTimeInNanosSample = TimeUnit.MILLISECONDS.toNanos(totalMergeTimeInMillisSample);
            long mergeTimeDeltaInNanos = totalMergeTimeInNanosSample - lastTotalMergeTimeSample;
            lastTotalMergeTimeSample = totalMergeTimeInMillisSample;

            long refreshTimeDeltaInNanos = Math.max(0, totalRefreshTimeInNanos - lastTotalRefreshTimeSample);
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
                    parentDataStreamName.get(),
                    indexShard.shardId(),
                    primary,
                    LoadDistribution.fromHistogram(indexingTimeHistogram),
                    LoadDistribution.fromHistogram(mergeTimeHistogram),
                    LoadDistribution.fromHistogram(refreshTimeHistogram)
                );
            }
        }

        private static DoubleHistogram createHistogram() {
            return new DoubleHistogram(5);
        }
    }
}
