/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.util.Map;

public class RollupShardTask extends CancellableTask {
    private String rollupIndex;
    private RollupActionConfig config;
    private volatile RollupShardStatus status;

    public RollupShardTask(
        long id,
        String type,
        String action,
        TaskId parentTask,
        String rollupIndex,
        RollupActionConfig config,
        Map<String, String> headers,
        ShardId shardId
    ) {
        super(id, type, action, RollupField.NAME + "_" + rollupIndex + "[" + shardId.id() + "]", parentTask, headers);
        this.rollupIndex = rollupIndex;
        this.config = config;
        this.status = new RollupShardStatus(shardId);
    }

    public String getRollupIndex() {
        return rollupIndex;
    }

    public RollupActionConfig config() {
        return config;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public void onCancelled() {
        status.setCancelled();
    }
}
