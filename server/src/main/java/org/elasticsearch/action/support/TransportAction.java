/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> {

    public final String actionName;
    private final ActionFilter[] filters;
    protected final TaskManager taskManager;
    /**
     * @deprecated declare your own logger.
     */
    @Deprecated
    protected Logger logger = LogManager.getLogger(getClass());

    protected TransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
        this.actionName = actionName;
        this.filters = actionFilters.filters();
        this.taskManager = taskManager;
    }

    /**
     * Use this method when the transport action should continue to run in the context of the current task
     */
    public final void execute(Task task, Request request, ActionListener<Response> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }

        if (task != null && request.getShouldStoreResult()) {
            listener = new TaskResultStoringActionListener<>(taskManager, task, listener);
        }

        RequestFilterChain<Request, Response> requestFilterChain = new RequestFilterChain<>(this, logger);
        requestFilterChain.proceed(task, actionName, request, listener);
    }

    protected abstract void doExecute(Task task, Request request, ActionListener<Response> listener);

    private static class RequestFilterChain<Request extends ActionRequest, Response extends ActionResponse>
            implements ActionFilterChain<Request, Response> {

        private final TransportAction<Request, Response> action;
        private final AtomicInteger index = new AtomicInteger();
        private final Logger logger;

        private RequestFilterChain(TransportAction<Request, Response> action, Logger logger) {
            this.action = action;
            this.logger = logger;
        }

        @Override
        public void proceed(Task task, String actionName, Request request, ActionListener<Response> listener) {
            int i = index.getAndIncrement();
            try {
                if (i < this.action.filters.length) {
                    this.action.filters[i].apply(task, actionName, request, listener, this);
                } else if (i == this.action.filters.length) {
                    this.action.doExecute(task, request, listener);
                } else {
                    listener.onFailure(new IllegalStateException("proceed was called too many times"));
                }
            } catch(Exception e) {
                logger.trace("Error during transport action execution.", e);
                listener.onFailure(e);
            }
        }

    }

    /**
     * Wrapper for an action listener that stores the result at the end of the execution
     */
    private static class TaskResultStoringActionListener<Response extends ActionResponse> implements ActionListener<Response> {
        private final ActionListener<Response> delegate;
        private final Task task;
        private final TaskManager taskManager;

        private TaskResultStoringActionListener(TaskManager taskManager, Task task, ActionListener<Response> delegate) {
            this.taskManager = taskManager;
            this.task = task;
            this.delegate = delegate;
        }

        @Override
        public void onResponse(Response response) {
            try {
                taskManager.storeResult(task, response, delegate);
            } catch (Exception e) {
                delegate.onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                taskManager.storeResult(task, e, delegate);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                delegate.onFailure(inner);
            }
        }
    }
}
