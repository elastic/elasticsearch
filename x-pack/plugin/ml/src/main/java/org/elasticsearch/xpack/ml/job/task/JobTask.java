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
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class JobTask extends AllocatedPersistentTask implements OpenJobAction.JobTaskMatcher {

    private static final Logger LOGGER = LogManager.getLogger(JobTask.class);

    private final String jobId;
    private final AtomicBoolean isVacating = new AtomicBoolean();
    private volatile AutodetectProcessManager autodetectProcessManager;
    private volatile boolean isClosing = false;

    JobTask(String jobId, long id, String type, String action, TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, "job-" + jobId, parentTask, headers);
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    protected void onCancelled() {
        String reason = getReasonCancelled();
        LOGGER.trace(() -> new ParameterizedMessage("[{}] Cancelling job task because: {}", jobId, reason));
        isClosing = true;
        autodetectProcessManager.killProcess(this, false, reason);
    }

    public boolean isClosing() {
        return isClosing;
    }

    public boolean triggerVacate() {
        return isVacating.compareAndSet(false, true);
    }

    public boolean changeVacateToClose() {
        return isVacating.compareAndSet(true, false);
    }

    public boolean isVacating() {
        return isVacating.get();
    }

    public void closeJob(String reason) {
        isClosing = true;
        autodetectProcessManager.closeJob(this, reason);
    }

    public void killJob(String reason) {
        isClosing = true;
        autodetectProcessManager.killProcess(this, true, reason);
    }

    void setAutodetectProcessManager(AutodetectProcessManager autodetectProcessManager) {
        this.autodetectProcessManager = autodetectProcessManager;
    }
}
