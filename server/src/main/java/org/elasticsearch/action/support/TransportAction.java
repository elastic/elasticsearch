/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.Transport;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> {

    public final String actionName;
    private final ActionFilter[] filters;
    protected final TaskManager taskManager;
    protected final Transport.Connection localConnection;

    /**
     * @deprecated declare your own logger.
     */
    @Deprecated
    protected Logger logger = LogManager.getLogger(getClass());

    protected TransportAction(
        String actionName,
        ActionFilters actionFilters,
        Transport.Connection localConnection,
        TaskManager taskManager
    ) {
        this.actionName = actionName;
        this.filters = actionFilters.filters();
        this.localConnection = localConnection;
        this.taskManager = taskManager;
    }

    private Releasable registerChildNode(TaskId parentTask) {
        if (parentTask.isSet()) {
            return taskManager.registerChildConnection(parentTask.getId(), localConnection);
        } else {
            return () -> {};
        }
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
        final Releasable unregisterChildNode = registerChildNode(request.getParentTask());
        final Task task;
        try {
            task = taskManager.register("transport", actionName, request);
        } catch (TaskCancelledException e) {
            unregisterChildNode.close();
            throw e;
        }
        execute(task, request, new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                try {
                    Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                } finally {
                    listener.onResponse(response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                } finally {
                    listener.onFailure(e);
                }
            }
        });
        return task;
    }

    /**
     * Execute the transport action on the local node, returning the {@link Task} used to track its execution and accepting a
     * {@link TaskListener} which listens for the completion of the action.
     */
    public final Task execute(Request request, TaskListener<Response> listener) {
        final Releasable unregisterChildNode = registerChildNode(request.getParentTask());
        final Task task;
        try {
            task = taskManager.register("transport", actionName, request);
        } catch (TaskCancelledException e) {
            unregisterChildNode.close();
            throw e;
        }
        execute(task, request, new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                try {
                    Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                } finally {
                    listener.onResponse(task, response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    Releasables.close(unregisterChildNode, () -> taskManager.unregister(task));
                } finally {
                    listener.onFailure(task, e);
                }
            }
        });
        return task;
    }

    /**
     * Use this method when the transport action should continue to run in the context of the current task
     */
    public final void execute(Task task, Request request, ActionListener<Response> listener) {
        final ActionRequestValidationException validationException;
        try {
            validationException = request.validate();
        } catch (Exception e) {
            assert false : new AssertionError("validating of request [" + request + "] threw exception", e);
            logger.warn("validating of request [" + request + "] threw exception", e);
            listener.onFailure(e);
            return;
        }
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
        implements
            ActionFilterChain<Request, Response> {

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
            } catch (Exception e) {
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
