/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.config.JobStatus;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

public class InternalOpenJobAction extends Action<InternalOpenJobAction.Request, InternalOpenJobAction.Response,
        InternalOpenJobAction.RequestBuilder> {

    public static final InternalOpenJobAction INSTANCE = new InternalOpenJobAction();
    public static final String NAME = "cluster:admin/ml/anomaly_detectors/internal_open";

    private InternalOpenJobAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends OpenJobAction.Request {

        public Request(String jobId) {
            super(jobId);
        }

        Request() {
            super();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new JobTask(getJobId(), id, type, action, parentTaskId);
        }

    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, InternalOpenJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse {

        Response() {}

    }

    public static class JobTask extends CancellableTask {

        private volatile Runnable cancelHandler;

        JobTask(String jobId, long id, String type, String action, TaskId parentTask) {
            super(id, type, action, "job-" + jobId, parentTask);
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return true;
        }

        @Override
        protected void onCancelled() {
            cancelHandler.run();
        }

        static boolean match(Task task, String expectedJobId) {
            String expectedDescription = "job-" + expectedJobId;
            return task instanceof JobTask && expectedDescription.equals(task.getDescription());
        }

    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final AutodetectProcessManager autodetectProcessManager;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               AutodetectProcessManager autodetectProcessManager) {
            super(settings, InternalOpenJobAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
            this.autodetectProcessManager = autodetectProcessManager;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            JobTask jobTask = (JobTask) task;
            autodetectProcessManager.setJobStatus(request.getJobId(), JobStatus.OPENING, aVoid -> {
                jobTask.cancelHandler = () -> autodetectProcessManager.closeJob(request.getJobId());
                autodetectProcessManager.openJob(request.getJobId(), request.isIgnoreDowntime(), e -> {
                    if (e == null) {
                        listener.onResponse(new Response());
                    } else {
                        listener.onFailure(e);
                    }
                });
            }, listener::onFailure);
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            throw new IllegalStateException("shouldn't get invoked");
        }

    }
}
