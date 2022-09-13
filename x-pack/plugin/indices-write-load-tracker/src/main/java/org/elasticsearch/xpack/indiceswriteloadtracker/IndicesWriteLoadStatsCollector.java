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
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.indiceswriteloadtracker.IndicesWriteLoadTrackerPlugin.currentThreadIsWriterLoadCollectorThreadOrTestThread;

class IndicesWriteLoadStatsCollector implements IndexEventListener {
    private final ClusterService clusterService;
    private final String nodeId;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final ConcurrentMap<ShardId, ShardWriteLoadHistogram> histograms = ConcurrentCollections.newConcurrentMap();

    IndicesWriteLoadStatsCollector(ClusterService clusterService, String nodeId, LongSupplier relativeTimeInNanosSupplier) {
        this.clusterService = clusterService;
        this.nodeId = nodeId;
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
    }

    void collectWriteLoadStats() {
        assert currentThreadIsWriterLoadCollectorThreadOrTestThread() : Thread.currentThread().getName();

        long currentTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
        for (ShardWriteLoadHistogram shardWriteLoadHistogram : histograms.values()) {
            shardWriteLoadHistogram.recordSample(currentTimeInNanos);
        }
    }

    @Nullable
    private String getParentDataStreamName(String indexName) {
        IndexAbstraction indexAbstraction = clusterService.state().getMetadata().getIndicesLookup().get(indexName);
        IndexAbstraction.DataStream parentDataStream = indexAbstraction.getParentDataStream();
        return parentDataStream != null ? parentDataStream.getName() : null;
    }

    private static boolean isDataStreamWriteIndex(Index index, Map<String, IndexAbstraction> indicesLookup) {
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
        final var indicesLookup = clusterService.state().metadata().getIndicesLookup();
        histograms.entrySet().removeIf(entry -> {
            final var shardId = entry.getKey();
            return isDataStreamWriteIndex(shardId.getIndex(), indicesLookup) == false;
        });
    }

    private static class ShardWriteLoadHistogram {
        private final Supplier<String> parentDataStreamNameSupplier;

        private volatile String parentDataStreamName;

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
            this.lastTotalIndexingTimeSample = indexShard.getTotalIndexingTimeInNanos();
            this.lastTotalMergeTimeSample = indexShard.getTotalMergeTimeInNanos();
            this.lastTotalRefreshTimeSample = indexShard.getTotalRefreshTimeInNanos();
        }

        void recordSample(long relativeTimeInNanos) {
            long totalIndexingTimeInNanosSample = indexShard.getTotalIndexingTimeInNanos();
            long totalMergeTimeInNanosSample = indexShard.getTotalMergeTimeInNanos();
            long totalRefreshTimeInNanos = indexShard.getTotalRefreshTimeInNanos();

            long samplingTimeInNanos = relativeTimeInNanos - lastSampleRelativeTimeInNanos;
            // Even though we're using a MONOTONIC clock (at least System.nanoTime() relies on clock_gettime with CLOCK_MONOTONIC in linux)
            // it's possible that the clock do not have enough granularity, in that case we bail out early just to be cautious.
            if (samplingTimeInNanos <= 0) {
                return;
            }
            lastSampleRelativeTimeInNanos = relativeTimeInNanos;

            long indexingTimeDeltaInNanos = totalIndexingTimeInNanosSample - lastTotalIndexingTimeSample;
            lastTotalIndexingTimeSample = totalIndexingTimeInNanosSample;

            long mergeTimeDeltaInNanos = totalMergeTimeInNanosSample - lastTotalMergeTimeSample;
            lastTotalMergeTimeSample = totalMergeTimeInNanosSample;

            long refreshTimeDeltaInNanos = totalRefreshTimeInNanos - lastTotalRefreshTimeSample;
            lastTotalRefreshTimeSample = totalRefreshTimeInNanos;

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
                    getParentDataStreamName(),
                    indexShard.shardId(),
                    primary,
                    new WriteLoadHistogramSnapshot(
                        System.currentTimeMillis(),
                        HistogramSnapshot.takeSnapshot(indexingTimeHistogram),
                        HistogramSnapshot.takeSnapshot(mergeTimeHistogram),
                        HistogramSnapshot.takeSnapshot(refreshTimeHistogram)
                    )
                );
                indexingTimeHistogram.reset();
                mergeTimeHistogram.reset();
                refreshTimeHistogram.reset();
                return indexLoadSummary;
            }
        }

        private String getParentDataStreamName() {
            String parentDataStream = parentDataStreamName;
            if (parentDataStream == null) {
                return (parentDataStreamName = parentDataStreamNameSupplier.get());
            } else {
                return parentDataStreamName;
            }
        }

        static Histogram createHistogram() {
            return new Histogram(2);
        }
    }

    public static class Histogram {
        private static final double ADJUST_FACTOR = 100.f;

        // This kind of histogram has a lower memory footprint than a DoubleHistogram,
        // and provides enough granularity for our use case
        private final ShortCountsHistogram delegate;
        private final MeanMetric average;

        Histogram(int numberOfSignificantValueDigits) {
            this.delegate = new ShortCountsHistogram(numberOfSignificantValueDigits);
            this.average = new MeanMetric();
        }

        void recordValue(double value) {
            int recordedValue = (int) (value * ADJUST_FACTOR);
            delegate.recordValue(recordedValue);
            average.inc(recordedValue);
        }

        void reset() {
            delegate.reset();
            average.reset();
        }

        public double getValueAtPercentile(double percentile) {
            return delegate.getValueAtPercentile(percentile) / ADJUST_FACTOR;
        }

        public double getMaxValue() {
            return delegate.getMaxValue() / ADJUST_FACTOR;
        }

        public double getAverage() {
            return average.mean() / ADJUST_FACTOR;
        }
    }
}
