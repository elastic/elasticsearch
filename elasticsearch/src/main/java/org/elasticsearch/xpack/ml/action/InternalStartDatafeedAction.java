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
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunner;

public class InternalStartDatafeedAction extends
        Action<InternalStartDatafeedAction.Request, InternalStartDatafeedAction.Response, InternalStartDatafeedAction.RequestBuilder> {

    public static final InternalStartDatafeedAction INSTANCE = new InternalStartDatafeedAction();
    public static final String NAME = "cluster:admin/ml/datafeed/internal_start";

    private InternalStartDatafeedAction() {
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

    public static class Request extends StartDatafeedAction.Request {

        Request(String datafeedId, long startTime) {
            super(datafeedId, startTime);
        }

        Request() {
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new DatafeedTask(id, type, action, parentTaskId, getDatafeedId());
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, InternalStartDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse {

        Response() {
        }

    }

    public static class DatafeedTask extends CancellableTask {

        private volatile DatafeedJobRunner.Holder holder;

        public DatafeedTask(long id, String type, String action, TaskId parentTaskId, String datafeedId) {
            super(id, type, action, "datafeed-" + datafeedId, parentTaskId);
        }

        public void setHolder(DatafeedJobRunner.Holder holder) {
            this.holder = holder;
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return true;
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

        private final DatafeedJobRunner datafeedJobRunner;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               DatafeedJobRunner datafeedJobRunner) {
            super(settings, InternalStartDatafeedAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
            this.datafeedJobRunner = datafeedJobRunner;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            DatafeedTask datafeedTask = (DatafeedTask) task;
            datafeedJobRunner.run(request.getDatafeedId(), request.getStartTime(), request.getEndTime(), datafeedTask, (error) -> {
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
