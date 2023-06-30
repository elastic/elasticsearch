/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RollupShardTask extends CancellableTask {
    private final String rollupIndex;
    private final DownsampleConfig config;
    private final ShardId shardId;
    private final long rollupStartTime;
    private final AtomicLong numReceived = new AtomicLong(0);
    private final AtomicLong numSent = new AtomicLong(0);
    private final AtomicLong numIndexed = new AtomicLong(0);
    private final AtomicLong numFailed = new AtomicLong(0);

    public RollupShardTask(
        long id,
        String type,
        String action,
        TaskId parentTask,
        String rollupIndex,
        DownsampleConfig config,
        Map<String, String> headers,
        ShardId shardId
    ) {
        super(id, type, action, RollupField.NAME + "_" + rollupIndex + "[" + shardId.id() + "]", parentTask, headers);
        this.rollupIndex = rollupIndex;
        this.config = config;
        this.shardId = shardId;
        this.rollupStartTime = System.currentTimeMillis();
    }

    public String getRollupIndex() {
        return rollupIndex;
    }

    public DownsampleConfig config() {
        return config;
    }

    @Override
    public Status getStatus() {
        return new RollupShardStatus(shardId, rollupStartTime, numReceived.get(), numSent.get(), numIndexed.get(), numFailed.get());
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
}
