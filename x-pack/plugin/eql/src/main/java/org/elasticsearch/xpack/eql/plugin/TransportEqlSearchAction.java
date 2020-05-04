/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.action.EqlStoredResponse;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.session.Configuration;
import org.elasticsearch.xpack.eql.session.Results;

import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportEqlSearchAction extends HandledTransportAction<EqlSearchRequest, EqlSearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportEqlSearchAction.class);

    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;
    private final ThreadPool threadPool;
    private final AsyncTaskIndexService<EqlStoredResponse> asyncTaskIndexService;

    @Inject
    public TransportEqlSearchAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                    ThreadPool threadPool, ActionFilters actionFilters, PlanExecutor planExecutor,
                                    NamedWriteableRegistry registry, Client client) {
        super(EqlSearchAction.NAME, transportService, actionFilters, EqlSearchRequest::new);

        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.clusterService = clusterService;
        this.planExecutor = planExecutor;
        this.threadPool = threadPool;
        this.asyncTaskIndexService = new AsyncTaskIndexService<>(EqlPlugin.INDEX, clusterService, threadPool.getThreadContext(), client,
            ASYNC_SEARCH_ORIGIN, EqlStoredResponse::new, registry);
    }

    /**
     * Wrapper for EqlSearchRequest that creates an async version of EqlSearchTask
     */
    private static class AsyncEqlRequest implements TaskAwareRequest {
        private final EqlSearchRequest request;
        private final String doc;
        private final String node;

        AsyncEqlRequest(EqlSearchRequest request, String node) {
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
            return new EqlSearchTask(id, type, action, getDescription(), parentTaskId, headers,
                new AsyncExecutionId(doc, new TaskId(node, id)));
        }

        @Override
        public String getDescription() {
            return request.getDescription();
        }
    }

    @Override
    protected void doExecute(Task task, EqlSearchRequest request, ActionListener<EqlSearchResponse> listener) {
        if (request.waitForCompletionTimeout() != null && request.waitForCompletionTimeout().getMillis() >= 0) {
            String username = username(securityContext);
            String clusterName = clusterName(clusterService);
            String nodeId = clusterService.localNode().getId();

            EqlSearchTask searchTask = (EqlSearchTask) taskManager.register("transport", EqlSearchAction.INSTANCE.name() + "[a]",
                new AsyncEqlRequest(request, nodeId));

            boolean operationStarted = false;
            try {
                operation(planExecutor, searchTask, request, username, clusterName, nodeId,
                    wrapStoringListener(searchTask, request.waitForCompletionTimeout(), request.keepAlive(), request.keepOnCompletion(),
                        listener));
                operationStarted = true;
            } finally {
                // If we didn't start operation for any reason, we need to clean up the task that we have created
                if (operationStarted == false) {
                    taskManager.unregister(searchTask);
                }
            }
        } else {
            operation(planExecutor, (EqlSearchTask) task, request, username(securityContext), clusterName(clusterService),
                clusterService.localNode().getId(), listener);
        }
    }

    private ActionListener<EqlSearchResponse> wrapStoringListener(EqlSearchTask searchTask,
                                                                  TimeValue waitForCompletionTimeout,
                                                                  TimeValue keepAlive,
                                                                  boolean keepOnCompletion,
                                                                  ActionListener<EqlSearchResponse> listener) {
        AtomicReference<ActionListener<EqlSearchResponse>> exclusiveListener = new AtomicReference<>(listener);
        long started = threadPool.relativeTimeInMillis();
        // This is will performed in case of timeout
        Scheduler.ScheduledCancellable timeoutHandler = threadPool.schedule(() -> {
            ActionListener<EqlSearchResponse> acquiredListener = exclusiveListener.getAndSet(null);
            if (acquiredListener != null) {
                acquiredListener.onResponse(new EqlSearchResponse(EqlSearchResponse.Hits.EMPTY,
                    threadPool.relativeTimeInMillis() - started, false, searchTask.getAsyncExecutionId().getEncoded(), true, true));
            }
        }, waitForCompletionTimeout, ThreadPool.Names.SEARCH);
        // This will be performed at the end of normal execution
        return ActionListener.wrap(response -> {
            ActionListener<EqlSearchResponse> acquiredListener = exclusiveListener.getAndSet(null);
            if (acquiredListener != null) {
                // We finished before timeout
                timeoutHandler.cancel();
                if (keepOnCompletion) {
                    storeResults(searchTask,
                        new EqlStoredResponse(response, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()),
                        ActionListener.wrap(() -> acquiredListener.onResponse(response)));
                } else {
                    taskManager.unregister(searchTask);
                    acquiredListener.onResponse(response);
                }
            } else {
                // We finished after timeout - saving results
                storeResults(searchTask, new EqlStoredResponse(response, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()));
            }
        }, e -> {
            ActionListener<EqlSearchResponse> acquiredListener = exclusiveListener.getAndSet(null);
            if (acquiredListener != null) {
                // We finished before timeout
                timeoutHandler.cancel();
                if (keepOnCompletion) {
                    storeResults(searchTask,
                        new EqlStoredResponse(e, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()),
                        ActionListener.wrap(() -> acquiredListener.onFailure(e)));
                } else {
                    taskManager.unregister(searchTask);
                    acquiredListener.onFailure(e);
                }
            } else {
                // We finished after timeout - saving exception
                storeResults(searchTask, new EqlStoredResponse(e, threadPool.absoluteTimeInMillis() + keepAlive.getMillis()));
            }
        });
    }

    private void storeResults(EqlSearchTask searchTask, EqlStoredResponse storedResponse) {
        storeResults(searchTask, storedResponse, ActionListener.wrap(r -> {
        }, e -> {
        }));
    }

    private void storeResults(EqlSearchTask searchTask, EqlStoredResponse storedResponse, ActionListener<Void> finalListener) {
        try {
            asyncTaskIndexService.createResponse(searchTask.getAsyncExecutionId().getDocId(),
                threadPool.getThreadContext().getHeaders(), storedResponse, ActionListener.wrap(
                    // We should only unregister after the result is saved
                    resp -> {
                        taskManager.unregister(searchTask);
                        logger.trace(() -> new ParameterizedMessage("stored eql search results for [{}]",
                            searchTask.getAsyncExecutionId().getEncoded()));
                        finalListener.onResponse(null);
                    },
                    exc -> {
                        taskManager.unregister(searchTask);
                        Throwable cause = ExceptionsHelper.unwrapCause(exc);
                        if (cause instanceof DocumentMissingException == false &&
                            cause instanceof VersionConflictEngineException == false) {
                            logger.error(() -> new ParameterizedMessage("failed to store eql search results for [{}]",
                                searchTask.getAsyncExecutionId().getEncoded()), exc);
                        }
                        finalListener.onFailure(exc);
                    }));
        } catch (Exception exc) {
            taskManager.unregister(searchTask);
            logger.error(() -> new ParameterizedMessage("failed to store eql search results for [{}]",
                searchTask.getAsyncExecutionId().getEncoded()), exc);

        }
    }

    public static void operation(PlanExecutor planExecutor, EqlSearchTask task, EqlSearchRequest request, String username,
                                 String clusterName, String nodeId, ActionListener<EqlSearchResponse> listener) {
        // TODO: these should be sent by the client
        ZoneId zoneId = DateUtils.of("Z");
        QueryBuilder filter = request.filter();
        TimeValue timeout = TimeValue.timeValueSeconds(30);
        boolean includeFrozen = request.indicesOptions().ignoreThrottled() == false;
        String clientId = null;

        ParserParams params = new ParserParams()
            .fieldEventCategory(request.eventCategoryField())
            .fieldTimestamp(request.timestampField())
            .implicitJoinKey(request.implicitJoinKeyField());

        Configuration cfg = new Configuration(request.indices(), zoneId, username, clusterName, filter, timeout, request.fetchSize(),
            includeFrozen, clientId, new TaskId(nodeId, task.getId()), task);
        planExecutor.eql(cfg, request.query(), params,
            wrap(r -> listener.onResponse(createResponse(r, task.getAsyncExecutionId())), listener::onFailure));
    }

    static EqlSearchResponse createResponse(Results results, AsyncExecutionId id) {
        EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(results.searchHits(), results.sequences(), results.counts(), results
            .totalHits());
        if (id != null) {
            return new EqlSearchResponse(hits, results.tookTime().getMillis(), results.timedOut(), id.getEncoded(), false, false);
        } else {
            return new EqlSearchResponse(hits, results.tookTime().getMillis(), results.timedOut());
        }
    }

    static String username(SecurityContext securityContext) {
        return securityContext != null && securityContext.getUser() != null ? securityContext.getUser().principal() : null;
    }

    static String clusterName(ClusterService clusterService) {
        return clusterService.getClusterName().value();
    }
}
