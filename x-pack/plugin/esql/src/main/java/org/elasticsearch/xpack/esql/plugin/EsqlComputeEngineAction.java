/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;

public class EsqlComputeEngineAction extends ActionType<ActionResponse.Empty> {
    public static final EsqlComputeEngineAction INSTANCE = new EsqlComputeEngineAction();
    public static final String NAME = "internal:data/read/esql_compute";

    private EsqlComputeEngineAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }

    public static class Request extends ActionRequest {
        private final Driver driver;

        public Request(Driver driver) {
            this.driver = driver;
        }

        public Request(StreamInput in) throws IOException {
            throw new UnsupportedOperationException("Compute request should never leave the current node");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("Compute request should never leave the current node");
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new Task(id, type, action, parentTaskId, headers, driver);
        }
    }

    public static class TransportAction extends HandledTransportAction<EsqlComputeEngineAction.Request, ActionResponse.Empty> {
        private final Executor executor;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
            super(NAME, transportService, actionFilters, in -> { throw new UnsupportedOperationException(); });
            this.executor = threadPool.executor(ThreadPool.Names.SEARCH);
        }

        @Override
        protected void doExecute(
            org.elasticsearch.tasks.Task task,
            EsqlComputeEngineAction.Request request,
            ActionListener<ActionResponse.Empty> listener
        ) {
            Driver.start(executor, request.driver, listener.map(nullValue -> new ActionResponse.Empty()));
        }
    }

    public static class Task extends CancellableTask {
        private final Driver driver;

        public Task(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers, Driver driver) {
            super(id, type, action, null, parentTaskId, headers);
            this.driver = driver;
        }

        @Override
        protected void onCancelled() {
            driver.cancel(getReasonCancelled());
        }

        @Override
        public String getDescription() {
            return driver.describe();
        }

        @Override
        public Status getStatus() {
            return driver.status();
        }
    }
}
