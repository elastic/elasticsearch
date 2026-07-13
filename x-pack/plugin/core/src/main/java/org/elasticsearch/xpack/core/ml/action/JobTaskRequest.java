/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;

public class JobTaskRequest<R extends JobTaskRequest<R>> extends BaseTasksRequest<R> {

    String jobId;

    JobTaskRequest() {}

    JobTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.jobId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(jobId);
    }

    JobTaskRequest(String jobId) {
        this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public boolean match(Task task) {
        return OpenJobAction.JobTaskMatcher.match(task, jobId);
    }
}
