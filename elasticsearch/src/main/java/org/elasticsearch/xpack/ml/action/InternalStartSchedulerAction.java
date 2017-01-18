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
import org.elasticsearch.xpack.ml.scheduler.ScheduledJobRunner;

public class InternalStartSchedulerAction extends
        Action<InternalStartSchedulerAction.Request, InternalStartSchedulerAction.Response, InternalStartSchedulerAction.RequestBuilder> {

    public static final InternalStartSchedulerAction INSTANCE = new InternalStartSchedulerAction();
    public static final String NAME = "cluster:admin/ml/scheduler/internal_start";

    private InternalStartSchedulerAction() {
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

    public static class Request extends StartSchedulerAction.Request {

        Request(String schedulerId, long startTime) {
            super(schedulerId, startTime);
        }

        Request() {
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new SchedulerTask(id, type, action, parentTaskId, getSchedulerId());
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, InternalStartSchedulerAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse {

        Response() {
        }

    }

    public static class SchedulerTask extends CancellableTask {

        private volatile ScheduledJobRunner.Holder holder;

        public SchedulerTask(long id, String type, String action, TaskId parentTaskId, String schedulerId) {
            super(id, type, action, "scheduler-" + schedulerId, parentTaskId);
        }

        public void setHolder(ScheduledJobRunner.Holder holder) {
            this.holder = holder;
        }

        @Override
        protected void onCancelled() {
            stop();
        }

        /* public for testing */
        public void stop() {
            if (holder != null) {
                holder.stop(null);
            }
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final ScheduledJobRunner scheduledJobRunner;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ScheduledJobRunner scheduledJobRunner) {
            super(settings, InternalStartSchedulerAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
            this.scheduledJobRunner = scheduledJobRunner;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            SchedulerTask schedulerTask = (SchedulerTask) task;
            scheduledJobRunner.run(request.getSchedulerId(), request.getStartTime(), request.getEndTime(), schedulerTask, (error) -> {
                if (error != null) {
                    listener.onFailure(error);
                } else {
                    listener.onResponse(new Response());
                }
            });
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            throw new UnsupportedOperationException("the task parameter is required");
        }
    }
}
