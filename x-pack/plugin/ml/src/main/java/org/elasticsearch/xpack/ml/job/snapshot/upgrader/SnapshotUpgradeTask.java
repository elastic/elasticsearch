/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.snapshot.upgrader;

import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;

import java.util.Map;

public class SnapshotUpgradeTask extends AllocatedPersistentTask {

    private final String jobId;
    private final String snapshotId;

    public SnapshotUpgradeTask(String jobId,
                               String snapshotId,
                               long id,
                               String type,
                               String action,
                               TaskId parentTask,
                               Map<String, String> headers) {
        super(id, type, action, MlTasks.snapshotUpgradeTaskId(jobId, snapshotId), parentTask, headers);
        this.jobId = jobId;
        this.snapshotId = snapshotId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }
}
