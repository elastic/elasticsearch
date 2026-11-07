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

public class DownsampleShardTask extends AllocatedPersistentTask {
    public static final String TASK_NAME = "rollup-shard";
    private final String downsampleIndex;
    private final DownsampleConfig config;
    private final ShardId shardId;
    private final DownsampleShardProgress progress;

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
        this.config = config;
        this.shardId = shardId;
        this.progress = new DownsampleShardProgress(shardId, indexStartTimeMillis, indexEndTimeMillis);
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

    /**
     * The counters of this run. The accessors below delegate to it.
     */
    public DownsampleShardProgress progress() {
        return progress;
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
        return progress.getTotalShardDocCount();
    }

    @Override
    public Status getStatus() {
        return progress.toStatus();
    }

    public long getNumReceived() {
        return progress.getNumReceived();
    }

    public long getNumSent() {
        return progress.getNumSent();
    }

    public long getNumIndexed() {
        return progress.getNumIndexed();
    }

    public long getNumFailed() {
        return progress.getNumFailed();
    }

    /**
     * Downsampled documents that were rejected by the target index with a version conflict because the same bucket had
     * already been indexed by a previous, interrupted run of this task.
     */
    public long getNumSkippedAlreadyIndexed() {
        return progress.getNumSkippedAlreadyIndexed();
    }

    public long getLastSourceTimestamp() {
        return progress.getLastSourceTimestamp();
    }

    public long getLastTargetTimestamp() {
        return progress.getLastTargetTimestamp();
    }

    public DownsampleBeforeBulkInfo getLastBeforeBulkInfo() {
        return progress.getLastBeforeBulkInfo();
    }

    public DownsampleAfterBulkInfo getLastAfterBulkInfo() {
        return progress.getLastAfterBulkInfo();
    }

    public long getDocsProcessed() {
        return progress.getDocsProcessed();
    }

    public long getRollupStartTime() {
        return progress.getRollupStartTime();
    }

    public DownsampleShardIndexerStatus getDownsampleShardIndexerStatus() {
        return progress.getDownsampleShardIndexerStatus();
    }

    public long getLastIndexingTimestamp() {
        return progress.getLastIndexingTimestamp();
    }

    public long getIndexStartTimeMillis() {
        return progress.getIndexStartTimeMillis();
    }

    public long getIndexEndTimeMillis() {
        return progress.getIndexEndTimeMillis();
    }

    public float getDocsProcessedPercentage() {
        return progress.getDocsProcessedPercentage();
    }

    public void addNumReceived(long count) {
        progress.addNumReceived(count);
    }

    public void addNumSent(long count) {
        progress.addNumSent(count);
    }

    public void addNumIndexed(long count) {
        progress.addNumIndexed(count);
    }

    public void addNumFailed(long count) {
        progress.addNumFailed(count);
    }

    public void addNumSkippedAlreadyIndexed(long count) {
        progress.addNumSkippedAlreadyIndexed(count);
    }

    public void setLastSourceTimestamp(long timestamp) {
        progress.setLastSourceTimestamp(timestamp);
    }

    public void setLastTargetTimestamp(long timestamp) {
        progress.setLastTargetTimestamp(timestamp);
    }

    public void setLastIndexingTimestamp(long timestamp) {
        progress.setLastIndexingTimestamp(timestamp);
    }

    public void setBeforeBulkInfo(final DownsampleBeforeBulkInfo beforeBulkInfo) {
        progress.setBeforeBulkInfo(beforeBulkInfo);
    }

    public void setAfterBulkInfo(final DownsampleAfterBulkInfo afterBulkInfo) {
        progress.setAfterBulkInfo(afterBulkInfo);
    }

    public void setDownsampleShardIndexerStatus(final DownsampleShardIndexerStatus status) {
        progress.setDownsampleShardIndexerStatus(status);
    }

    public void setTotalShardDocCount(int totalShardDocCount) {
        progress.setTotalShardDocCount(totalShardDocCount);
    }

    public void setDocsProcessed(long docsProcessed) {
        progress.setDocsProcessed(docsProcessed);
    }

    public void updateBulkInfo(long bulkIngestTookMillis, long bulkTookMillis) {
        progress.updateBulkInfo(bulkIngestTookMillis, bulkTookMillis);
    }

    public DownsampleBulkInfo getDownsampleBulkInfo() {
        return progress.getDownsampleBulkInfo();
    }
}
