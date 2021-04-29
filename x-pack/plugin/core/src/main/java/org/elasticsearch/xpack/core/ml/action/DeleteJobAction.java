/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeleteJobAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteJobAction INSTANCE = new DeleteJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/delete";
    public static final String DELETION_TASK_DESCRIPTION_PREFIX = "delete-job-";

    private DeleteJobAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private String jobId;
        private boolean force;

        /**
         * Should this task store its result?
         */
        private boolean shouldStoreResult;

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            force = in.readBoolean();
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public boolean isForce() {
            return force;
        }

        public void setForce(boolean force) {
            this.force = force;
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
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return DELETION_TASK_DESCRIPTION_PREFIX + jobId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(force);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, force);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            DeleteJobAction.Request other = (DeleteJobAction.Request) obj;
            return Objects.equals(jobId, other.jobId) && Objects.equals(force, other.force);
        }
    }
}
