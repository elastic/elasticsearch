/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Base class that redirects a request to a node where the job task is running.
 */
// TODO: Hacking around here with TransportTasksAction. Ideally we should have another base class in core that
// redirects to a single node only
public abstract class TransportJobTaskAction<OperationTask extends OpenJobAction.JobTask, Request extends TransportJobTaskAction.JobTaskRequest<Request>,
        Response extends BaseTasksResponse & Writeable> extends TransportTasksAction<OperationTask, Request, Response, Response> {

    protected final AutodetectProcessManager processManager;

    TransportJobTaskAction(Settings settings, String actionName, ThreadPool threadPool, ClusterService clusterService,
                           TransportService transportService, ActionFilters actionFilters,
                           IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> requestSupplier,
                           Supplier<Response> responseSupplier, String nodeExecutor, AutodetectProcessManager processManager) {
        super(settings, actionName, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                requestSupplier, responseSupplier, nodeExecutor);
        this.processManager = processManager;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        ClusterState state = clusterService.state();
        // We need to check whether there is at least an assigned task here, otherwise we cannot redirect to the
        // node running the job task.
        Set<String> executorNodes = new HashSet<>();
        for (String resolvedJobId : request.getResolvedJobIds()) {
            JobManager.getJobOrThrowIfUnknown(state, resolvedJobId);
            PersistentTasksCustomMetaData tasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
            PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlMetadata.getJobTask(resolvedJobId, tasks);
            if (jobTask == null || jobTask.isAssigned() == false) {
                String message = "Cannot perform requested action because job [" + resolvedJobId
                        + "] is not open";
                listener.onFailure(ExceptionsHelper.conflictStatusException(message));
                return;
            } else {
                executorNodes.add(jobTask.getExecutorNode());
            }
        }

        request.setNodes(executorNodes.toArray(new String[executorNodes.size()]));
        super.doExecute(task, request, listener);
    }

    @Override
    protected final void taskOperation(Request request, OperationTask task, ActionListener<Response> listener) {
        ClusterState state = clusterService.state();
        PersistentTasksCustomMetaData tasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        JobState jobState = MlMetadata.getJobState(task.getJobId(), tasks);
        if (jobState == JobState.OPENED) {
            innerTaskOperation(request, task, listener, state);
        } else {
            logger.warn("Unexpected job state based on cluster state version [{}]", state.getVersion());
            listener.onFailure(ExceptionsHelper.conflictStatusException("Cannot perform requested action because job [" +
                    request.getJobId() + "] is not open"));
        }
    }

    protected abstract void innerTaskOperation(Request request, OperationTask task, ActionListener<Response> listener, ClusterState state);

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
                                   List<FailedNodeException> failedNodeExceptions) {
        return selectFirst(tasks, taskOperationFailures, failedNodeExceptions);

    }

    static <Response extends BaseTasksResponse> Response selectFirst(List<Response> tasks,
                                                                     List<TaskOperationFailure> taskOperationFailures,
                                                                     List<FailedNodeException> failedNodeExceptions) {
        // no need to accumulate sub responses, since we only perform an operation on one task only
        // not ideal, but throwing exceptions here works, because higher up the stack there is a try-catch block delegating to
        // the actionlistener's onFailure
        if (tasks.isEmpty()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
            } else {
                throw new IllegalStateException("No errors or response");
            }
        } else {
            if (tasks.size() > 1) {
                throw new IllegalStateException(
                        "Expected one node level response, but got [" + tasks.size() + "]");
            }
            return tasks.get(0);
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }

    public static class JobTaskRequest<R extends JobTaskRequest<R>> extends BaseTasksRequest<R> {

        String jobId;
        String[] resolvedJobIds;

        JobTaskRequest() {
        }

        JobTaskRequest(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());

            // the default implementation just returns 1 jobId
            this.resolvedJobIds = new String[] { jobId };
        }

        public String getJobId() {
            return jobId;
        }

        protected String[] getResolvedJobIds() {
            return resolvedJobIds;
        }

        protected void setResolvedJobIds(String[] resolvedJobIds) {
            this.resolvedJobIds = resolvedJobIds;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            resolvedJobIds = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeStringArray(resolvedJobIds);
        }

        @Override
        public boolean match(Task task) {
            for (String id : resolvedJobIds) {
                if (OpenJobAction.JobTask.match(task, id)) {
                    return true;
                }
            }

            return false;
        }
    }
}
