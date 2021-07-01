/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ResetJobAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "cluster:admin/xpack/ml/job/reset";
    public static final ResetJobAction INSTANCE = new ResetJobAction();

    public static final Version VERSION_INTRODUCED = Version.V_7_14_0;

    private ResetJobAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private String jobId;

        /**
         * Internal parameter that allows resetting an open job
         * when a job is reallocated to a new node.
         */
        private boolean skipJobStateValidation;

        /**
         * Should this task store its result?
         */
        private boolean shouldStoreResult;

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            skipJobStateValidation = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(skipJobStateValidation);
        }

        public void setSkipJobStateValidation(boolean skipJobStateValidation) {
            this.skipJobStateValidation = skipJobStateValidation;
        }

        public boolean isSkipJobStateValidation() {
            return skipJobStateValidation;
        }

        /**
         * Should this task store its result after it has finished?
         */
        public void setShouldStoreResult(boolean shouldStoreResult) {
            this.shouldStoreResult = shouldStoreResult;
        }

        @Override
        public boolean getShouldStoreResult() {
            return shouldStoreResult;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, MlTasks.JOB_TASK_ID_PREFIX + jobId, parentTaskId, headers);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, skipJobStateValidation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || o.getClass() != getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(jobId, that.jobId) && skipJobStateValidation == that.skipJobStateValidation;
        }
    }
}
