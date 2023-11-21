/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.downsample;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

/**
 * This class contains the high-level logic that drives the rollup job. The allocated task contains transient state
 * which drives the indexing, and periodically updates it's parent PersistentTask with the indexing's current position.
 */
public class DownsampleTask extends CancellableTask {
    private static final String ROLLUP_FIELD_NAME = "rollup";
    private final String downsampleIndex;
    private final DownsampleConfig config;

    public DownsampleTask(
        long id,
        String type,
        String action,
        TaskId parentTask,
        String downsampleIndex,
        DownsampleConfig config,
        Map<String, String> headers
    ) {
        super(id, type, action, ROLLUP_FIELD_NAME + "_" + downsampleIndex, parentTask, headers);
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
