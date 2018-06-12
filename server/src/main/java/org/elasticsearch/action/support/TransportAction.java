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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;

public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    protected final ThreadPool threadPool;
    protected final String actionName;
    private final ActionFilter[] filters;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;
    protected final TaskManager taskManager;

    protected TransportAction(Settings settings, String actionName, ThreadPool threadPool, ActionFilters actionFilters,
                              IndexNameExpressionResolver indexNameExpressionResolver, TaskManager taskManager) {
        super(settings);
        this.threadPool = threadPool;
        this.actionName = actionName;
        this.filters = actionFilters.filters();
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.taskManager = taskManager;
    }

    public final ActionFuture<Response> execute(Request request) {
        PlainActionFuture<Response> future = newFuture();
        execute(request, future);
        return future;
    }

    /**
     * Use this method when the transport action call should result in creation of a new task associated with the call.
     *
     * This is a typical behavior.
     */
    public final Task execute(Request request, ActionListener<Response> listener) {
        /*
         * While this version of execute could delegate to the TaskListener
         * version of execute that'd add yet another layer of wrapping on the
         * listener and prevent us from using the listener bare if there isn't a
         * task. That just seems like too many objects. Thus the two versions of
         * this method.
         */
        Task task = taskManager.register("transport", actionName, request);
        if (task == null) {
            execute(null, request, listener);
        } else {
            execute(task, request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    taskManager.unregister(task);
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    taskManager.unregister(task);
                    listener.onFailure(e);
                }
            });
        }
        return task;
    }

    /**
     * Execute the transport action on the local node, returning the {@link Task} used to track its execution and accepting a
     * {@link TaskListener} which listens for the completion of the action.
     */
    public final Task execute(Request request, TaskListener<Response> listener) {
        Task task = taskManager.register("transport", actionName, request);
        execute(task, request, new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                if (task != null) {
                    taskManager.unregister(task);
                }
                listener.onResponse(task, response);
            }

            @Override
            public void onFailure(Exception e) {
                if (task != null) {
                    taskManager.unregister(task);
                }
                listener.onFailure(task, e);
            }
        });
        return task;
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

    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        doExecute(request, listener);
    }

    protected abstract void doExecute(Request request, ActionListener<Response> listener);

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
