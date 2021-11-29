/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.license.LicensedAllocatedPersistentTask;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class JobTask extends LicensedAllocatedPersistentTask implements OpenJobAction.JobTaskMatcher {

    /**
     * We should only progress forwards through these states: close takes precedence over vacate
     */
    enum ClosingOrVacating {
        NEITHER,
        VACATING,
        CLOSING
    }

    private static final Logger logger = LogManager.getLogger(JobTask.class);

    private final String jobId;
    private final AtomicReference<ClosingOrVacating> closingOrVacating = new AtomicReference<>(ClosingOrVacating.NEITHER);
    private volatile AutodetectProcessManager autodetectProcessManager;

    protected JobTask(
        String jobId,
        long id,
        String type,
        String action,
        TaskId parentTask,
        Map<String, String> headers,
        XPackLicenseState licenseState
    ) {
        super(id, type, action, "job-" + jobId, parentTask, headers, MachineLearning.ML_ANOMALY_JOBS_FEATURE, "job-" + jobId, licenseState);
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    protected void onCancelled() {
        String reason = getReasonCancelled();
        logger.trace(() -> new ParameterizedMessage("[{}] Cancelling job task because: {}", jobId, reason));
        closingOrVacating.set(ClosingOrVacating.CLOSING);
        autodetectProcessManager.killProcess(this, false, reason);
    }

    public boolean isClosing() {
        return closingOrVacating.get() == ClosingOrVacating.CLOSING;
    }

    public boolean triggerVacate() {
        return closingOrVacating.compareAndSet(ClosingOrVacating.NEITHER, ClosingOrVacating.VACATING);
    }

    public boolean isVacating() {
        return closingOrVacating.get() == ClosingOrVacating.VACATING;
    }

    public void closeJob(String reason) {
        // If a job is vacating the node when a close request arrives, convert that vacate to a close.
        // This may be too late, if the vacate operation has already gone past the point of unassigning
        // the persistent task instead of completing it. But in general a close should take precedence
        // over a vacate.
        if (closingOrVacating.getAndSet(ClosingOrVacating.CLOSING) == ClosingOrVacating.VACATING) {
            logger.info("[{}] Close request for job while it was vacating the node", jobId);
        }
        autodetectProcessManager.closeJob(this, reason);
    }

    public void killJob(String reason) {
        closingOrVacating.set(ClosingOrVacating.CLOSING);
        autodetectProcessManager.killProcess(this, true, reason);
    }

    void setAutodetectProcessManager(AutodetectProcessManager autodetectProcessManager) {
        this.autodetectProcessManager = autodetectProcessManager;
    }
}
