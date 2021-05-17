/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;

import java.util.Map;

/**
 * This class contains the high-level logic that drives the rollup job. The allocated task contains transient state
 * which drives the indexing, and periodically updates it's parent PersistentTask with the indexing's current position.
 */
public class RollupTask extends CancellableTask {
    private String rollupIndex;
    private RollupActionConfig config;
    private RollupJobStatus status;

    RollupTask(long id, String type, String action, TaskId parentTask, String rollupIndex, RollupActionConfig config,
               Map<String, String> headers) {
        super(id, type, action, RollupField.NAME + "_" + rollupIndex, parentTask, headers);
        this.rollupIndex = rollupIndex;
        this.config = config;
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
        // TODO(talevy): make things cancellable
    }
}
