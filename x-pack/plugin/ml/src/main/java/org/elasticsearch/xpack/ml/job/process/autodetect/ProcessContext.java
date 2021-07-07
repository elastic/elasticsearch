/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.task.JobTask;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The process context that encapsulates the job task, the process state and the autodetect communicator.
 */
final class ProcessContext {

    private static final Logger LOGGER = LogManager.getLogger(ProcessContext.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final JobTask jobTask;
    private volatile AutodetectCommunicator autodetectCommunicator;
    private volatile ProcessState state;

    ProcessContext(JobTask jobTask) {
        this.jobTask = jobTask;
        this.state = new ProcessNotRunningState();
    }

    JobTask getJobTask() {
        return jobTask;
    }

    AutodetectCommunicator getAutodetectCommunicator() {
        return autodetectCommunicator;
    }

    private void setAutodetectCommunicator(AutodetectCommunicator autodetectCommunicator) {
        this.autodetectCommunicator = autodetectCommunicator;
    }

    ProcessStateName getState() {
        return state.getName();
    }

    private void setState(ProcessState state) {
        this.state = state;
    }

    void tryLock() {
        try {
            if (lock.tryLock(MachineLearningField.STATE_PERSIST_RESTORE_TIMEOUT.getSeconds(), TimeUnit.SECONDS) == false) {
                LOGGER.error("Failed to acquire process lock for job [{}]", jobTask.getJobId());
                throw ExceptionsHelper.serverError("Failed to acquire process lock for job [" + jobTask.getJobId() + "]");
            }
        } catch (InterruptedException e) {
            throw new ElasticsearchException(e);
        }
    }

    void unlock() {
        lock.unlock();
    }

    void setRunning(AutodetectCommunicator autodetectCommunicator) {
        assert lock.isHeldByCurrentThread();
        state.setRunning(this, autodetectCommunicator);
    }

    boolean setDying() {
        assert lock.isHeldByCurrentThread();
        return state.setDying(this);
    }

    KillBuilder newKillBuilder() {
        return new ProcessContext.KillBuilder();
    }

    class KillBuilder {
        private boolean awaitCompletion;
        private boolean finish;
        private boolean silent;
        private boolean shouldFinalizeJob = true;
        private String reason;

        KillBuilder setAwaitCompletion(boolean awaitCompletion) {
            this.awaitCompletion = awaitCompletion;
            return this;
        }

        KillBuilder setFinish(boolean finish) {
            this.finish = finish;
            return this;
        }

        KillBuilder setSilent(boolean silent) {
            this.silent = silent;
            return this;
        }

        KillBuilder setReason(String reason) {
            this.reason = reason;
            return this;
        }

        KillBuilder setShouldFinalizeJob(boolean shouldFinalizeJob) {
            this.shouldFinalizeJob = shouldFinalizeJob;
            return this;
        }

        void kill() {
            if (autodetectCommunicator == null) {
                // Killing a connected process would also complete the persistent task if `finish` was true,
                // so we should do the same here even though the process wasn't yet connected at the time of
                // the kill
                if (finish) {
                    jobTask.markAsCompleted();
                }
                return;
            }
            String jobId = jobTask.getJobId();

            if (silent == false) {
                String extraInfo = (state.getName() == ProcessStateName.DYING) ? " while closing" : "";
                if (reason == null) {
                    LOGGER.info("Killing job [{}]{}", jobId, extraInfo);
                } else {
                    LOGGER.info("Killing job [{}]{}, because [{}]", jobId, extraInfo, reason);
                }
            }
            try {
                autodetectCommunicator.killProcess(awaitCompletion, finish, shouldFinalizeJob);
            } catch (IOException e) {
                LOGGER.error("[{}] Failed to kill autodetect process for job", jobId);
            }
        }
    }

    enum ProcessStateName {
        NOT_RUNNING, RUNNING, DYING
    }

    private interface ProcessState {
        /**
         * @return was a state change made?
         * */
        boolean setRunning(ProcessContext processContext, AutodetectCommunicator autodetectCommunicator);
        /**
         * @return was a state change made?
         */
        boolean setDying(ProcessContext processContext);
        ProcessStateName getName();
    }

    private static class ProcessNotRunningState implements ProcessState {
        @Override
        public boolean setRunning(ProcessContext processContext, AutodetectCommunicator autodetectCommunicator) {
            processContext.setAutodetectCommunicator(autodetectCommunicator);
            processContext.setState(new ProcessRunningState());
            return true;
        }

        @Override
        public boolean setDying(ProcessContext processContext) {
            processContext.setState(new ProcessDyingState());
            return true;
        }

        @Override
        public ProcessStateName getName() {
            return ProcessStateName.NOT_RUNNING;
        }
    }

    private static class ProcessRunningState implements ProcessState {

        @Override
        public boolean setRunning(ProcessContext processContext, AutodetectCommunicator autodetectCommunicator) {
            LOGGER.debug("Process set to [running] while it was already in that state");
            return false;
        }

        @Override
        public boolean setDying(ProcessContext processContext) {
            processContext.setState(new ProcessDyingState());
            return true;
        }

        @Override
        public ProcessStateName getName() {
            return ProcessStateName.RUNNING;
        }
    }

    private static class ProcessDyingState implements ProcessState {

        @Override
        public boolean setRunning(ProcessContext processContext, AutodetectCommunicator autodetectCommunicator) {
            LOGGER.debug("Process set to [running] while it was in [dying]");
            return false;
        }

        @Override
        public boolean setDying(ProcessContext processContext) {
            LOGGER.debug("Process set to [dying] while it was already in that state");
            return false;
        }

        @Override
        public ProcessStateName getName() {
            return ProcessStateName.DYING;
        }
    }
}
