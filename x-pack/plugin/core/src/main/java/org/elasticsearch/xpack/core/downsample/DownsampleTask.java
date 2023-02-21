/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.util.Map;

/**
 * This class contains the high-level logic that drives the rollup job. The allocated task contains transient state
 * which drives the indexing, and periodically updates it's parent PersistentTask with the indexing's current position.
 */
public class DownsampleTask extends CancellableTask {
    private final String downsampleIndex;
    private final DownsampleConfig config;

    DownsampleTask(
        long id,
        String type,
        String action,
        TaskId parentTask,
        String downsampleIndex,
        DownsampleConfig config,
        Map<String, String> headers
    ) {
        super(id, type, action, RollupField.NAME + "_" + downsampleIndex, parentTask, headers);
        this.downsampleIndex = downsampleIndex;
        this.config = config;
    }

    public String getDownsampleIndex() {
        return downsampleIndex;
    }

    public DownsampleConfig config() {
        return config;
    }

}
