/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTask;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.StoredAsyncResponse;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;

/**
 * Service for managing EQL requests
 */
public class AsyncTaskManagementService<
    Request extends TaskAwareRequest,
    Response extends ActionResponse,
    T extends StoredAsyncTask<Response>> {

    private static final Logger logger = LogManager.getLogger(AsyncTaskManagementService.class);

    private final TaskManager taskManager;
    private final String action;
    private final AsyncTaskIndexService<StoredAsyncResponse<Response>> asyncTaskIndexService;
    private final AsyncOperation<Request, Response, T> operation;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Class<T> taskClass;

    public interface AsyncOperation<
        Request extends TaskAwareRequest,
        Response extends ActionResponse,
        T extends CancellableTask & AsyncTask> {

        T createTask(
            Request request,
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            Map<String, String> headers,
            Map<String, String> originHeaders,
            AsyncExecutionId asyncExecutionId
        );

        void execute(Request request, T task, ActionListener<Response> listener);

        Response initialResponse(T task);

        Response readResponse(StreamInput inputStream) throws IOException;
    }

    /**
     * Wrapper for EqlSearchRequest that creates an async version of EqlSearchTask
     */
    private class AsyncRequestWrapper implements TaskAwareRequest {
        private final Request request;
        private final String doc;
        private final String node;

        AsyncRequestWrapper(Request request, String node) {
            this.request = request;
            this.doc = UUIDs.randomBase64UUID();
            this.node = node;
        }

        @Override
        public void setParentTask(TaskId taskId) {
            request.setParentTask(taskId);
        }

        @Override
        public TaskId getParentTask() {
            return request.getParentTask();
        }

        @Override
        public void setRequestId(long requestId) {
            request.setRequestId(requestId);
        }

        @Override
        public long getRequestId() {
            return request.getRequestId();
        }

        @Override
        public Task createTask(long id, String type, String actionName, TaskId parentTaskId, Map<String, String> headers) {
            Map<String, String> originHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
                threadPool.getThreadContext(),
                clusterService.state()
            );
            return operation.createTask(
                request,
                id,
                type,
                actionName,
                parentTaskId,
                headers,
                originHeaders,
                new AsyncExecutionId(doc, new TaskId(node, id))
            );
        }

        @Override
        public String getDescription() {
            return request.getDescription();
        }
    }

    public AsyncTaskManagementService(
        String index,
        Client client,
        String origin,
        NamedWriteableRegistry registry,
        TaskManager taskManager,
        String action,
        AsyncOperation<Request, Response, T> operation,
        Class<T> taskClass,
        ClusterService clusterService,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        this.taskManager = taskManager;
        this.action = action;
        this.operation = operation;
        this.taskClass = taskClass;
        this.asyncTaskIndexService = new AsyncTaskIndexService<>(
            index,
            clusterService,
            threadPool.getThreadContext(),
            client,
            origin,
            i -> new StoredAsyncResponse<>(operation::readResponse, i),
            registry,
            bigArrays
        );
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void asyncExecute(
        Request request,
        TimeValue waitForCompletionTimeout,
        TimeValue keepAlive,
        boolean keepOnCompletion,
        ActionListener<Response> listener
    ) {
        String nodeId = clusterService.localNode().getId();
        try (var ignored = threadPool.getThreadContext().newTraceContext()) {
            @SuppressWarnings("unchecked")
            T searchTask = (T) taskManager.register("transport", action + "[a]", new AsyncRequestWrapper(request, nodeId));
            boolean operationStarted = false;
            try {
                operation.execute(
                    request,
                    searchTask,
                    wrapStoringListener(searchTask, waitForCompletionTimeout, keepAlive, keepOnCompletion, listener)
                );
                operationStarted = true;
            } finally {
                // If we didn't start operation for any reason, we need to clean up the task that we have created
                if (operationStarted == false) {
                    taskManager.unregister(searchTask);
                }
            }
        }
    }

    private ActionListener<Response> wrapStoringListener(
        T searchTask,
        TimeValue waitForCompletionTimeout,
        TimeValue keepAlive,
        boolean keepOnCompletion,
        ActionListener<Response> listener
    ) {
        AtomicReference<ActionListener<Response>> exclusiveListener = new AtomicReference<>(listener);
        // This is will performed in case of timeout
        Scheduler.ScheduledCancellable timeoutHandler = threadPool.schedule(() -> {
            ActionListener<Response> acquiredListener = exclusiveListener.getAndSet(null);
            if (acquiredListener != null) {
                acquiredListener.onResponse(operation.initialResponse(searchTask));
            }
        }, waitForCompletionTimeout, ThreadPool.Names.SEARCH);
        // This will be performed at the end of normal execution
        return ActionListener.wrap(response -> {
            ActionListener<Response> acquiredListener = exclusiveListener.getAndSet(null);
            if (acquiredListener != null) {
                // We finished before timeout
                timeoutHandler.cancel();
                if (keepOnCompletion) {
                    storeResults(
                        searchTask,
                        new StoredAsyncResponse<>(response, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()),
                        ActionListener.running(() -> acquiredListener.onResponse(response))
                    );
                } else {
                    taskManager.unregister(searchTask);
                    searchTask.onResponse(response);
                    acquiredListener.onResponse(response);
                }
            } else {
                // We finished after timeout - saving results
                storeResults(searchTask, new StoredAsyncResponse<>(response, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()));
            }
        }, e -> {
            ActionListener<Response> acquiredListener = exclusiveListener.getAndSet(null);
            if (acquiredListener != null) {
                // We finished before timeout
                timeoutHandler.cancel();
                if (keepOnCompletion) {
                    storeResults(
                        searchTask,
                        new StoredAsyncResponse<>(e, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()),
                        ActionListener.running(() -> acquiredListener.onFailure(e))
                    );
                } else {
                    taskManager.unregister(searchTask);
                    searchTask.onFailure(e);
                    acquiredListener.onFailure(e);
                }
            } else {
                // We finished after timeout - saving exception
                storeResults(searchTask, new StoredAsyncResponse<>(e, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()));
            }
        });
    }

    private void storeResults(T searchTask, StoredAsyncResponse<Response> storedResponse) {
        storeResults(searchTask, storedResponse, null);
    }

    private void storeResults(T searchTask, StoredAsyncResponse<Response> storedResponse, ActionListener<Void> finalListener) {
        try {
            asyncTaskIndexService.createResponseForEQL(
                searchTask.getExecutionId().getDocId(),
                searchTask.getOriginHeaders(),
                storedResponse,
                ActionListener.wrap(
                    // We should only unregister after the result is saved
                    resp -> {
                        logger.trace(() -> "stored eql search results for [" + searchTask.getExecutionId().getEncoded() + "]");
                        taskManager.unregister(searchTask);
                        if (storedResponse.getException() != null) {
                            searchTask.onFailure(storedResponse.getException());
                        } else {
                            searchTask.onResponse(storedResponse.getResponse());
                        }
                        if (finalListener != null) {
                            finalListener.onResponse(null);
                        }
                    },
                    exc -> {
                        taskManager.unregister(searchTask);
                        searchTask.onFailure(exc);
                        Throwable cause = ExceptionsHelper.unwrapCause(exc);
                        if (cause instanceof DocumentMissingException == false
                            && cause instanceof VersionConflictEngineException == false) {
                            logger.error(
                                () -> format("failed to store eql search results for [%s]", searchTask.getExecutionId().getEncoded()),
                                exc
                            );
                        }
                        if (finalListener != null) {
                            finalListener.onFailure(exc);
                        }
                    }
                )
            );
        } catch (Exception exc) {
            taskManager.unregister(searchTask);
            searchTask.onFailure(exc);
            logger.error(() -> "failed to store eql search results for [" + searchTask.getExecutionId().getEncoded() + "]", exc);
        }
    }

    /**
     * Adds a self-unregistering listener to a task. It works as a normal listener except it retrieves a partial response and unregister
     * itself from the task if timeout occurs.
     */
    public static <Response extends ActionResponse, Task extends StoredAsyncTask<Response>> void addCompletionListener(
        ThreadPool threadPool,
        Task task,
        ActionListener<StoredAsyncResponse<Response>> listener,
        TimeValue timeout
    ) {
        if (timeout.getMillis() <= 0) {
            getCurrentResult(task, listener);
        } else {
            task.addCompletionListener(
                ListenerTimeouts.wrapWithTimeout(
                    threadPool,
                    timeout,
                    ThreadPool.Names.SEARCH,
                    ActionListener.wrap(
                        r -> listener.onResponse(new StoredAsyncResponse<>(r, task.getExpirationTimeMillis())),
                        e -> listener.onResponse(new StoredAsyncResponse<>(e, task.getExpirationTimeMillis()))
                    ),
                    wrapper -> {
                        // Timeout was triggered
                        task.removeCompletionListener(wrapper);
                        getCurrentResult(task, listener);
                    }
                )
            );
        }
    }

    private static <Response extends ActionResponse, Task extends StoredAsyncTask<Response>> void getCurrentResult(
        Task task,
        ActionListener<StoredAsyncResponse<Response>> listener
    ) {
        try {
            listener.onResponse(new StoredAsyncResponse<>(task.getCurrentResult(), task.getExpirationTimeMillis()));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
}
