/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class DownsampleShardTask extends AllocatedPersistentTask {
    public static final String TASK_NAME = "rollup-shard";
    private final String downsampleIndex;
    private volatile long totalShardDocCount;
    private volatile long docsProcessed;
    private final long indexStartTimeMillis;
    private final long indexEndTimeMillis;
    private final DownsampleConfig config;
    private final ShardId shardId;
    private final long rollupStartTime;
    private final AtomicLong numReceived = new AtomicLong(0);
    private final AtomicLong numSent = new AtomicLong(0);
    private final AtomicLong numIndexed = new AtomicLong(0);
    private final AtomicLong numFailed = new AtomicLong(0);
    private final AtomicLong lastSourceTimestamp = new AtomicLong(0);
    private final AtomicLong lastTargetTimestamp = new AtomicLong(0);
    private final AtomicLong lastIndexingTimestamp = new AtomicLong(0);
    private final AtomicReference<DownsampleShardIndexerStatus> downsampleShardIndexerStatus = new AtomicReference<>(
        DownsampleShardIndexerStatus.INITIALIZED
    );
    private final DownsampleBulkStats downsampleBulkStats;
    // Need to set initial values, because these atomic references can be read before bulk indexing started or when downsampling empty index
    private final AtomicReference<DownsampleBeforeBulkInfo> lastBeforeBulkInfo = new AtomicReference<>(
        new DownsampleBeforeBulkInfo(0, 0, 0, 0)
    );
    private final AtomicReference<DownsampleAfterBulkInfo> lastAfterBulkInfo = new AtomicReference<>(
        new DownsampleAfterBulkInfo(0, 0, 0, 0, false, 0)
    );

    public DownsampleShardTask(
        long id,
        final String type,
        final String action,
        final TaskId parentTask,
        final String downsampleIndex,
        long indexStartTimeMillis,
        long indexEndTimeMillis,
        final DownsampleConfig config,
        final Map<String, String> headers,
        final ShardId shardId
    ) {
        super(id, type, action, RollupField.NAME + "_" + downsampleIndex + "[" + shardId.id() + "]", parentTask, headers);
        this.downsampleIndex = downsampleIndex;
        this.indexStartTimeMillis = indexStartTimeMillis;
        this.indexEndTimeMillis = indexEndTimeMillis;
        this.config = config;
        this.shardId = shardId;
        this.rollupStartTime = System.currentTimeMillis();
        this.downsampleBulkStats = new DownsampleBulkStats();
    }

    @Override
    protected void init(
        final PersistentTasksService persistentTasksService,
        final TaskManager taskManager,
        final String persistentTaskId,
        final long allocationId
    ) {
        super.init(persistentTasksService, taskManager, persistentTaskId, allocationId);
    }

    // TODO: just for testing
    public void testInit(
        final PersistentTasksService persistentTasksService,
        final TaskManager taskManager,
        final String persistentTaskId,
        final long allocationId
    ) {
        init(persistentTasksService, taskManager, persistentTaskId, allocationId);
    }

    public String getDownsampleIndex() {
        return downsampleIndex;
    }

    public DownsampleConfig config() {
        return config;
    }

    public ShardId shardId() {
        return shardId;
    }

    public long getTotalShardDocCount() {
        return totalShardDocCount;
    }

    @Override
    public Status getStatus() {
        return new DownsampleShardStatus(
            shardId,
            rollupStartTime,
            numReceived.get(),
            numSent.get(),
            numIndexed.get(),
            numFailed.get(),
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
}
