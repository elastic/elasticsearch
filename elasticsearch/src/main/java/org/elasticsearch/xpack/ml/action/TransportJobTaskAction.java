/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.JobStatus;
import org.elasticsearch.xpack.ml.job.manager.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.manager.JobManager;
import org.elasticsearch.xpack.ml.job.metadata.Allocation;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Base class that redirects a request to a node where the job task is running.
 */
// TODO: Hacking around here with TransportTasksAction. Ideally we should have another base class in core that
// redirects to a single node only
public abstract class TransportJobTaskAction<OperationTask extends Task, Request extends TransportJobTaskAction.JobTaskRequest<Request>,
        Response extends BaseTasksResponse & Writeable> extends TransportTasksAction<OperationTask, Request, Response, Response> {

    protected final JobManager jobManager;
    protected final AutodetectProcessManager processManager;
    private final Function<Request, String> jobIdFromRequest;

    TransportJobTaskAction(Settings settings, String actionName, ThreadPool threadPool, ClusterService clusterService,
                           TransportService transportService, ActionFilters actionFilters,
                           IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> requestSupplier,
                           Supplier<Response> responseSupplier, String nodeExecutor, JobManager jobManager,
                           AutodetectProcessManager processManager, Function<Request, String> jobIdFromRequest) {
        super(settings, actionName, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                requestSupplier, responseSupplier, nodeExecutor);
        this.jobManager = jobManager;
        this.processManager = processManager;
        this.jobIdFromRequest = jobIdFromRequest;
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
                                   List<FailedNodeException> failedNodeExceptions) {
        // no need to accumulate sub responses, since we only perform an operation on one task only
        // not ideal, but throwing exceptions here works, because higher up the stack there is a try-catch block delegating to
        // the actionlistener's onFailure
        if (tasks.isEmpty()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw new ElasticsearchException(taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw new ElasticsearchException(failedNodeExceptions.get(0).getCause());
            } else {
                // the same validation that exists in AutodetectProcessManager#processData(...) and flush(...) methods
                // is required here too because if the job hasn't been opened yet then no task exist for it yet and then
                // #taskOperation(...) method will not be invoked, returning an empty result to the client.
                // This ensures that we return an understandable error:
                String jobId = jobIdFromRequest.apply(request);
                jobManager.getJobOrThrowIfUnknown(jobId);
                Allocation allocation = jobManager.getJobAllocation(jobId);
                if (allocation.getStatus() != JobStatus.OPENED) {
                    throw new ElasticsearchStatusException("job [" + jobId + "] status is [" + allocation.getStatus() +
                            "], but must be [" + JobStatus.OPENED + "] to perform requested action", RestStatus.CONFLICT);
                } else {
                    throw new IllegalStateException("No errors or response");
                }
            }
        } else {
            if (tasks.size() > 1) {
                throw new IllegalStateException("Expected one node level response, but got [" + tasks.size() + "]");
            }
            return tasks.get(0);
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    public static class JobTaskRequest<R extends JobTaskRequest<R>> extends BaseTasksRequest<R> {

        String jobId;

        JobTaskRequest() {
        }

        JobTaskRequest(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
        }

        @Override
        public boolean match(Task task) {
            return InternalOpenJobAction.JobTask.match(task, jobId);
        }
    }
}
