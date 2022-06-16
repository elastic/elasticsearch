/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.HdrHistogram.ShortCountsHistogram;
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

import static org.elasticsearch.xpack.indiceswriteloadtracker.IndicesWriteLoadTrackerPlugin.currentThreadIsWriterLoadCollectorThreadOrTestThread;

class IndicesWriteLoadStatsCollector implements IndexEventListener {
    private final ClusterService clusterService;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final ConcurrentMap<ShardId, ShardWriteLoadHistogram> histograms = ConcurrentCollections.newConcurrentMap();

    IndicesWriteLoadStatsCollector(ClusterService clusterService, LongSupplier relativeTimeInNanosSupplier) {
        this.clusterService = clusterService;
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
    }

    void collectWriteLoadStats() {
        assert currentThreadIsWriterLoadCollectorThreadOrTestThread() : Thread.currentThread().getName();

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

    List<ShardWriteLoadHistogramSnapshot> getWriteLoadHistogramSnapshotsAndReset() {
        final var shardWriteLoadDistributions = histograms.values()
            .stream()
            .map(ShardWriteLoadHistogram::getWriteLoadHistogramSnapshotAndReset)
            .toList();

        cleanRolledOverIndices();

        return shardWriteLoadDistributions;
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        final var shardId = indexShard.shardId();
        if (indexShard.isDataStreamIndex()) {
            histograms.put(
                shardId,
                new ShardWriteLoadHistogram(
                    // We have to get this lazily as we cannot access the cluster state
                    // on a cluster state applier thread.
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
        private final Supplier<String> parentDataStreamNameSupplier;
        private final IndexShard indexShard;
        private final boolean primary;
        private final LongSupplier relativeTimeInNanosSupplier;
        private final Object histogramsLock = new Object();
        private final Histogram indexingTimeHistogram;
        private final Histogram mergeTimeHistogram;
        private final Histogram refreshTimeHistogram;
        private long lastTotalIndexingTimeSample;
        private long lastTotalMergeTimeSample;
        private long lastTotalRefreshTimeSample;
        private long lastSampleRelativeTimeInNanos;

        ShardWriteLoadHistogram(
            Supplier<String> parentDataStreamNameSupplier,
            IndexShard indexShard,
            boolean primary,
            LongSupplier relativeTimeInNanosSupplier
        ) {
            this.parentDataStreamNameSupplier = parentDataStreamNameSupplier;
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
            // Even though we're using a MONOTONIC clock (at least System.nanoTime() relies on clock_gettime with CLOCK_MONOTONIC in linux)
            // it's possible that the clock do not have enough granularity, in that case we bail out early just to be cautious.
            if (samplingTimeInNanos <= 0) {
                return;
            }
            lastSampleRelativeTimeInNanos = sampleRelativeTimeInNanos;

            long indexingTimeDeltaInNanos = totalIndexingTimeInNanosSample - lastTotalIndexingTimeSample;
            lastTotalIndexingTimeSample = totalIndexingTimeInNanosSample;

            long totalMergeTimeInNanosSample = TimeUnit.MILLISECONDS.toNanos(totalMergeTimeInMillisSample);
            long mergeTimeDeltaInNanos = totalMergeTimeInNanosSample - lastTotalMergeTimeSample;
            lastTotalMergeTimeSample = totalMergeTimeInMillisSample;

            long refreshTimeDeltaInNanos = totalRefreshTimeInNanos - lastTotalRefreshTimeSample;
            lastTotalRefreshTimeSample = totalRefreshTimeInNanos;

            // Don't record the load if we're taking the first sample.
            if (indexingTimeDeltaInNanos == totalIndexingTimeInNanosSample) {
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

        ShardWriteLoadHistogramSnapshot getWriteLoadHistogramSnapshotAndReset() {
            synchronized (histogramsLock) {
                final var indexLoadSummary = new ShardWriteLoadHistogramSnapshot(
                    System.currentTimeMillis(),
                    parentDataStreamNameSupplier.get(),
                    indexShard.shardId(),
                    primary,
                    HistogramSnapshot.takeSnapshot(indexingTimeHistogram),
                    HistogramSnapshot.takeSnapshot(mergeTimeHistogram),
                    HistogramSnapshot.takeSnapshot(refreshTimeHistogram)
                );
                indexingTimeHistogram.reset();
                mergeTimeHistogram.reset();
                refreshTimeHistogram.reset();
                return indexLoadSummary;
            }
        }

        static Histogram createHistogram() {
            return new Histogram(2);
        }
    }

    public static class Histogram {
        private static final double ADJUST_FACTOR = 100.f;

        // This kind of histogram as a lower memory footprint than a DoubleHistogram,
        // and provides enough granularity for our use case
        private final ShortCountsHistogram delegate;

        Histogram(int numberOfSignificantValueDigits) {
            this.delegate = new ShortCountsHistogram(numberOfSignificantValueDigits);
        }

        void recordValue(double value) {
            delegate.recordValue((int) (value * ADJUST_FACTOR));
        }

        void reset() {
            delegate.reset();
        }

        public double getValueAtPercentile(double percentile) {
            return delegate.getValueAtPercentile(percentile) / ADJUST_FACTOR;
        }

        public double getMaxValue() {
            return delegate.getMaxValue() / ADJUST_FACTOR;
        }
    }
}
