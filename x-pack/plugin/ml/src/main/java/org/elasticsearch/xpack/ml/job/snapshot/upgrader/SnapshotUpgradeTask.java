/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.snapshot.upgrader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.license.LicensedAllocatedPersistentTask;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.JobModelSnapshotUpgrader;

import java.util.Map;

public class SnapshotUpgradeTask extends LicensedAllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(SnapshotUpgradeTask.class);

    private final String jobId;
    private final String snapshotId;
    // Not volatile as only used in synchronized methods
    private JobModelSnapshotUpgrader jobModelSnapshotUpgrader;

    public SnapshotUpgradeTask(
        String jobId,
        String snapshotId,
        long id,
        String type,
        String action,
        TaskId parentTask,
        Map<String, String> headers,
        XPackLicenseState licenseState
    ) {
        super(
            id,
            type,
            action,
            MlTasks.snapshotUpgradeTaskId(jobId, snapshotId),
            parentTask,
            headers,
            MachineLearning.ML_ANOMALY_JOBS_FEATURE,
            MlTasks.snapshotUpgradeTaskId(jobId, snapshotId),
            licenseState
        );
        this.jobId = jobId;
        this.snapshotId = snapshotId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    @Override
    protected synchronized void onCancelled() {
        if (jobModelSnapshotUpgrader != null) {
            String reason = getReasonCancelled();
            logger.trace("[{}] Cancelling snapshot upgrade [{}] task because: {}", jobId, snapshotId, reason);
            jobModelSnapshotUpgrader.killProcess(reason);
            jobModelSnapshotUpgrader = null;
        }
    }

    /**
     * @param jobModelSnapshotUpgrader the snapshot upgrader
     * @return false if the task has been canceled and upgrader not set
     */
    public synchronized boolean setJobModelSnapshotUpgrader(JobModelSnapshotUpgrader jobModelSnapshotUpgrader) {
        if (this.isCancelled()) {
            return false;
        }
        this.jobModelSnapshotUpgrader = jobModelSnapshotUpgrader;
        return true;
    }
}
