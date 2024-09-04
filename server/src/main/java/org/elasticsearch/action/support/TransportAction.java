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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> {

    public final String actionName;
    private final ActionFilter[] filters;
    protected final TaskManager taskManager;
    private final Executor executor;
    /**
     * @deprecated declare your own logger.
     */
    @Deprecated
    protected Logger logger = LogManager.getLogger(getClass());

    interface TransportActionHandler<Request extends ActionRequest, Response extends ActionResponse> {
        void execute(Task task, Request request, ActionListener<Response> listener);
    }

    protected TransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager, Executor executor) {
        this.actionName = actionName;
        this.filters = actionFilters.filters();
        this.taskManager = taskManager;
        this.executor = executor;
    }

    /**
     * Use this method when the transport action should continue to run in the context of the current task
     */
    protected final void executeDirect(Task task, Request request, ActionListener<Response> listener) {
        handleExecution(task, request, listener, this::doExecute);
    }

    public final void execute(Task task, Request request, ActionListener<Response> listener) {
        handleExecution(
            task,
            request,
            listener,
            executor == EsExecutors.DIRECT_EXECUTOR_SERVICE ? this::doExecute : this::doExecuteForking
        );
    }

    private void handleExecution(
        Task task,
        Request request,
        ActionListener<Response> listener,
        TransportActionHandler<Request, Response> handler
    ) {
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

        // Note on request refcounting: we can be sure that either we get to the end of the chain (and execute the actual action) or
        // we complete the response listener and short-circuit the outer chain, so we release our request ref on both paths, using
        // Releasables#releaseOnce to avoid a double-release.
        request.mustIncRef();
        final var releaseRef = Releasables.releaseOnce(request::decRef);
        RequestFilterChain<Request, Response> requestFilterChain = new RequestFilterChain<>(this, logger, handler, releaseRef);
        requestFilterChain.proceed(task, actionName, request, ActionListener.runBefore(listener, releaseRef::close));
    }

    private void doExecuteForking(Task task, Request request, ActionListener<Response> listener) {
        executor.execute(ActionRunnable.wrap(listener, l -> doExecute(task, request, listener)));
    }

    protected abstract void doExecute(Task task, Request request, ActionListener<Response> listener);

    private static class RequestFilterChain<Request extends ActionRequest, Response extends ActionResponse>
        implements
            ActionFilterChain<Request, Response> {

        private final TransportAction<Request, Response> action;
        private final TransportActionHandler<Request, Response> handler;
        private final AtomicInteger index = new AtomicInteger();
        private final Logger logger;
        private final Releasable releaseRef;

        private RequestFilterChain(
            TransportAction<Request, Response> action,
            Logger logger,
            TransportActionHandler<Request, Response> handler,
            Releasable releaseRef
        ) {
            this.action = action;
            this.logger = logger;
            this.handler = handler;
            this.releaseRef = releaseRef;
        }

        @Override
        public void proceed(Task task, String actionName, Request request, ActionListener<Response> listener) {
            int i = index.getAndIncrement();
            try {
                if (i < this.action.filters.length) {
                    this.action.filters[i].apply(task, actionName, request, listener, this);
                } else if (i == this.action.filters.length) {
                    try (releaseRef) {
                        handler.execute(task, request, listener);
                    }
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
            ActionListener.run(delegate, l -> taskManager.storeResult(task, response, l));
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

    /**
     * A method to use as a placeholder in implementations of {@link TransportAction} which only ever run on the local node, and therefore
     * do not need to serialize or deserialize any messages.
     */
    // TODO remove this when https://github.com/elastic/elasticsearch/issues/100111 is resolved
    public static <T> T localOnly() {
        assert false : "local-only action";
        throw new UnsupportedOperationException("local-only action");
    }
}
