/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncTaskManagementService<
    Request extends TaskAwareRequest,
    Response extends ActionResponse,
    T extends CancellableTask & AsyncTask> {

    private static final Logger logger = LogManager.getLogger(AsyncTaskManagementService.class);

    private final TaskManager taskManager;
    private final String action;
    private final AsyncTaskIndexService<StoredAsyncResponse<Response>> asyncTaskIndexService;
    private final AsyncOperation<Request, Response, T> operation;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Class<T> taskClass;

    public interface AsyncOperation<Request extends TaskAwareRequest,
        Response extends ActionResponse,
        T extends CancellableTask & AsyncTask> {

        T createTask(Request request, long id, String type, String action, TaskId parentTaskId, Map<String, String> headers,
                     Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId);

        void operation(Request request, T task, ActionListener<Response> listener);

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
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return operation.createTask(request, id, type, action, parentTaskId, headers, threadPool.getThreadContext().getHeaders(),
                new AsyncExecutionId(doc, new TaskId(node, id)));
        }

        @Override
        public String getDescription() {
            return request.getDescription();
        }
    }

    public AsyncTaskManagementService(String index, Client client, String origin, NamedWriteableRegistry registry, TaskManager taskManager,
                                      String action, AsyncOperation<Request, Response, T> operation, Class<T> taskClass,
                                      ClusterService clusterService,
                                      ThreadPool threadPool
    ) {
        this.taskManager = taskManager;
        this.action = action;
        this.operation = operation;
        this.taskClass = taskClass;
        this.asyncTaskIndexService = new AsyncTaskIndexService<>(index, clusterService, threadPool.getThreadContext(), client,
            origin, i -> new StoredAsyncResponse<>(operation::readResponse, i), registry);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void asyncExecute(Request request, TimeValue waitForCompletionTimeout, TimeValue keepAlive, boolean keepOnCompletion,
                             ActionListener<Response> listener) {
        String nodeId = clusterService.localNode().getId();
        @SuppressWarnings("unchecked")
        T searchTask = (T) taskManager.register("transport", action + "[a]", new AsyncRequestWrapper(request, nodeId));
        boolean operationStarted = false;
        try {
            operation.operation(request, searchTask,
                wrapStoringListener(searchTask, waitForCompletionTimeout, keepAlive, keepOnCompletion, listener));
            operationStarted = true;
        } finally {
            // If we didn't start operation for any reason, we need to clean up the task that we have created
            if (operationStarted == false) {
                taskManager.unregister(searchTask);
            }
        }
    }

    // TODO: For tests for now, will be merged into comprehensive get operation later
    T getTask(AsyncExecutionId asyncExecutionId) throws IOException {
        return asyncTaskIndexService.getTask(taskManager,asyncExecutionId, taskClass);
    }

    // TODO: For tests for now, will be removed when the final get operation is added
    void getResponse(AsyncExecutionId id, ActionListener<Response> listener) {
        asyncTaskIndexService.getResponse(id, true, ActionListener.wrap(r -> {
                if (r.getException() != null) {
                    listener.onFailure(r.getException());
                } else {
                    listener.onResponse(r.getResponse());
                }
            }, listener::onFailure));
    }

    private ActionListener<Response> wrapStoringListener(T searchTask,
                                                         TimeValue waitForCompletionTimeout,
                                                         TimeValue keepAlive,
                                                         boolean keepOnCompletion,
                                                         ActionListener<Response> listener) {
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
                    storeResults(searchTask,
                        new StoredAsyncResponse<>(response, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()),
                        ActionListener.wrap(() -> acquiredListener.onResponse(response)));
                } else {
                    taskManager.unregister(searchTask);
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
                    storeResults(searchTask,
                        new StoredAsyncResponse<>(e, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()),
                        ActionListener.wrap(() -> acquiredListener.onFailure(e)));
                } else {
                    taskManager.unregister(searchTask);
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
            asyncTaskIndexService.createResponse(searchTask.getExecutionId().getDocId(),
                threadPool.getThreadContext().getHeaders(), storedResponse, ActionListener.wrap(
                    // We should only unregister after the result is saved
                    resp -> {
                        taskManager.unregister(searchTask);
                        logger.trace(() -> new ParameterizedMessage("stored eql search results for [{}]",
                            searchTask.getExecutionId().getEncoded()));
                        if (finalListener != null) {
                            finalListener.onResponse(null);
                        }
                    },
                    exc -> {
                        taskManager.unregister(searchTask);
                        Throwable cause = ExceptionsHelper.unwrapCause(exc);
                        if (cause instanceof DocumentMissingException == false &&
                            cause instanceof VersionConflictEngineException == false) {
                            logger.error(() -> new ParameterizedMessage("failed to store eql search results for [{}]",
                                searchTask.getExecutionId().getEncoded()), exc);
                        }
                        if (finalListener != null) {
                            finalListener.onFailure(exc);
                        }
                    }));
        } catch (Exception exc) {
            taskManager.unregister(searchTask);
            logger.error(() -> new ParameterizedMessage("failed to store eql search results for [{}]",
                searchTask.getExecutionId().getEncoded()), exc);

        }
    }
}
