/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.v2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;

import java.util.Map;

/**
 * This class contains the high-level logic that drives the rollup job. The allocated task contains transient state
 * which drives the indexing, and periodically updates it's parent PersistentTask with the indexing's current position.
 */
public class RollupTask extends CancellableTask {
    private static final Logger logger = LogManager.getLogger(RollupTask.class.getName());

    private RollupActionConfig config;
    private RollupJobStatus status;

    RollupTask(long id, String type, String action, TaskId parentTask, RollupActionConfig config, Map<String, String> headers) {
        super(id, type, action, RollupField.NAME + "_" + config.getRollupIndex(), parentTask, headers);
        this.config = config;
    }

    public RollupActionConfig config() {
        return config;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    @Override
    public void onCancelled() {
        // TODO(talevy): make things cancellable
    }
}
