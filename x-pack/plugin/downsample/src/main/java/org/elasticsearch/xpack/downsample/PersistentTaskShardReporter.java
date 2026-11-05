/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;
import org.elasticsearch.xpack.core.downsample.DownsampleShardProgress;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;

import java.util.Objects;

/**
 * A {@link ShardTaskReporter} backed by the {@link DownsampleShardTask} persistent task: status changes are published
 * as the persistent task state, which is what the transport action waits on.
 */
final class PersistentTaskShardReporter implements ShardTaskReporter {
    private final DownsampleShardTask task;

    PersistentTaskShardReporter(DownsampleShardTask task) {
        this.task = Objects.requireNonNull(task);
    }

    @Override
    public DownsampleShardProgress progress() {
        return task.progress();
    }

    @Override
    public boolean isCancelled() {
        return task.isCancelled();
    }

    @Override
    public String id() {
        return task.getPersistentTaskId();
    }

    @Override
    public void publishStatus(DownsampleShardIndexerStatus status) {
        task.progress().setDownsampleShardIndexerStatus(status);
        // The resume _tsid is currently never populated, see DownsampleShardPersistentTaskExecutor#nodeOperation.
        task.updatePersistentTaskState(new DownsampleShardPersistentTaskState(status, null), ActionListener.noop());
    }
}
