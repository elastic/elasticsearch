/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The progress counters of a single downsample shard run, independent of how the run is scheduled or tracked. It exists so
 * the shard indexer can report progress to whatever is running it (a persistent task, or a node-local run whose progress is
 * persisted elsewhere) while every consumer reads the same values, and {@link #toStatus()} produces the same
 * {@link DownsampleShardStatus} the task status API exposes.
 */
public class DownsampleShardProgress {
    private final ShardId shardId;
    private final long indexStartTimeMillis;
    private final long indexEndTimeMillis;
    private final long rollupStartTime;
    private volatile long totalShardDocCount;
    private volatile long docsProcessed;
    private final AtomicLong numReceived = new AtomicLong(0);
    private final AtomicLong numSent = new AtomicLong(0);
    private final AtomicLong numIndexed = new AtomicLong(0);
    private final AtomicLong numFailed = new AtomicLong(0);
    private final AtomicLong numSkippedAlreadyIndexed = new AtomicLong(0);
    private final AtomicLong lastSourceTimestamp = new AtomicLong(0);
    private final AtomicLong lastTargetTimestamp = new AtomicLong(0);
    private final AtomicLong lastIndexingTimestamp = new AtomicLong(0);
    private final AtomicReference<DownsampleShardIndexerStatus> downsampleShardIndexerStatus = new AtomicReference<>(
        DownsampleShardIndexerStatus.INITIALIZED
    );
    private final DownsampleBulkStats downsampleBulkStats = new DownsampleBulkStats();
    // Need to set initial values, because these atomic references can be read before bulk indexing started or when downsampling empty index
    private final AtomicReference<DownsampleBeforeBulkInfo> lastBeforeBulkInfo = new AtomicReference<>(
        new DownsampleBeforeBulkInfo(0, 0, 0, 0)
    );
    private final AtomicReference<DownsampleAfterBulkInfo> lastAfterBulkInfo = new AtomicReference<>(
        new DownsampleAfterBulkInfo(0, 0, 0, 0, false, 0)
    );

    public DownsampleShardProgress(ShardId shardId, long indexStartTimeMillis, long indexEndTimeMillis) {
        this.shardId = shardId;
        this.indexStartTimeMillis = indexStartTimeMillis;
        this.indexEndTimeMillis = indexEndTimeMillis;
        this.rollupStartTime = System.currentTimeMillis();
    }

    public ShardId shardId() {
        return shardId;
    }

    public long getTotalShardDocCount() {
        return totalShardDocCount;
    }

    public long getNumReceived() {
        return numReceived.get();
    }

    public long getNumSent() {
        return numSent.get();
    }

    public long getNumIndexed() {
        return numIndexed.get();
    }

    public long getNumFailed() {
        return numFailed.get();
    }

    /**
     * Downsampled documents that were rejected by the target index with a version conflict because the same bucket had
     * already been indexed by a previous, interrupted run of this task.
     */
    public long getNumSkippedAlreadyIndexed() {
        return numSkippedAlreadyIndexed.get();
    }

    public long getLastSourceTimestamp() {
        return lastSourceTimestamp.get();
    }

    public long getLastTargetTimestamp() {
        return lastTargetTimestamp.get();
    }

    public DownsampleBeforeBulkInfo getLastBeforeBulkInfo() {
        return lastBeforeBulkInfo.get();
    }

    public DownsampleAfterBulkInfo getLastAfterBulkInfo() {
        return lastAfterBulkInfo.get();
    }

    public long getDocsProcessed() {
        return docsProcessed;
    }

    public long getRollupStartTime() {
        return rollupStartTime;
    }

    public DownsampleShardIndexerStatus getDownsampleShardIndexerStatus() {
        return downsampleShardIndexerStatus.get();
    }

    public long getLastIndexingTimestamp() {
        return lastIndexingTimestamp.get();
    }

    public long getIndexStartTimeMillis() {
        return indexStartTimeMillis;
    }

    public long getIndexEndTimeMillis() {
        return indexEndTimeMillis;
    }

    public float getDocsProcessedPercentage() {
        return getTotalShardDocCount() <= 0 ? 0.0F : 100.0F * docsProcessed / totalShardDocCount;
    }

    public void addNumReceived(long count) {
        numReceived.addAndGet(count);
    }

    public void addNumSent(long count) {
        numSent.addAndGet(count);
    }

    public void addNumIndexed(long count) {
        numIndexed.addAndGet(count);
    }

    public void addNumFailed(long count) {
        numFailed.addAndGet(count);
    }

    public void addNumSkippedAlreadyIndexed(long count) {
        numSkippedAlreadyIndexed.addAndGet(count);
    }

    public void setLastSourceTimestamp(long timestamp) {
        lastSourceTimestamp.set(timestamp);
    }

    public void setLastTargetTimestamp(long timestamp) {
        lastTargetTimestamp.set(timestamp);
    }

    public void setLastIndexingTimestamp(long timestamp) {
        lastIndexingTimestamp.set(timestamp);
    }

    public void setBeforeBulkInfo(final DownsampleBeforeBulkInfo beforeBulkInfo) {
        lastBeforeBulkInfo.set(beforeBulkInfo);
    }

    public void setAfterBulkInfo(final DownsampleAfterBulkInfo afterBulkInfo) {
        lastAfterBulkInfo.set(afterBulkInfo);
    }

    public void setDownsampleShardIndexerStatus(final DownsampleShardIndexerStatus status) {
        this.downsampleShardIndexerStatus.set(status);
    }

    public void setTotalShardDocCount(int totalShardDocCount) {
        this.totalShardDocCount = totalShardDocCount;
    }

    public void setDocsProcessed(long docsProcessed) {
        this.docsProcessed = docsProcessed;
    }

    public void updateBulkInfo(long bulkIngestTookMillis, long bulkTookMillis) {
        this.downsampleBulkStats.update(bulkIngestTookMillis, bulkTookMillis);
    }

    public DownsampleBulkInfo getDownsampleBulkInfo() {
        return this.downsampleBulkStats.getRollupBulkInfo();
    }

    /**
     * A point in time snapshot of the progress. Note that the percentage is not guarded against an unknown (zero) total doc
     * count, to keep what the task status API reported before this class was extracted.
     */
    public DownsampleShardStatus toStatus() {
        return new DownsampleShardStatus(
            shardId,
            rollupStartTime,
            numReceived.get(),
            numSent.get(),
            numIndexed.get(),
            numFailed.get(),
            numSkippedAlreadyIndexed.get(),
            totalShardDocCount,
            lastSourceTimestamp.get(),
            lastTargetTimestamp.get(),
            lastIndexingTimestamp.get(),
            indexStartTimeMillis,
            indexEndTimeMillis,
            docsProcessed,
            100.0F * docsProcessed / totalShardDocCount,
            downsampleBulkStats.getRollupBulkInfo(),
            lastBeforeBulkInfo.get(),
            lastAfterBulkInfo.get(),
            downsampleShardIndexerStatus.get()
        );
    }
}
