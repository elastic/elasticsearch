/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.eql.util.RemoteClusterRegistry;
import org.elasticsearch.xpack.ql.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.ql.expression.Order;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.ql.plugin.TransportActionUtils.executeRequestWithRetryAttempt;

public class TransportEqlSearchAction extends HandledTransportAction<EqlSearchRequest, EqlSearchResponse>
    implements AsyncTaskManagementService.AsyncOperation<EqlSearchRequest, EqlSearchResponse, EqlSearchTask> {

    private static final Logger log = LogManager.getLogger(TransportEqlSearchAction.class);
    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final AsyncTaskManagementService<EqlSearchRequest, EqlSearchResponse, EqlSearchTask> asyncTaskManagementService;

    @Inject
    public TransportEqlSearchAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                    ThreadPool threadPool, ActionFilters actionFilters, PlanExecutor planExecutor,
                                    NamedWriteableRegistry registry, Client client, BigArrays bigArrays) {
        super(EqlSearchAction.NAME, transportService, actionFilters, EqlSearchRequest::new);

        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.clusterService = clusterService;
        this.planExecutor = planExecutor;
        this.threadPool = threadPool;
        this.transportService = transportService;

        this.asyncTaskManagementService = new AsyncTaskManagementService<>(XPackPlugin.ASYNC_RESULTS_INDEX, client, ASYNC_SEARCH_ORIGIN,
            registry, taskManager, EqlSearchAction.INSTANCE.name(), this, EqlSearchTask.class, clusterService, threadPool, bigArrays);
    }

    @Override
    public EqlSearchTask createTask(EqlSearchRequest request, long id, String type, String action, TaskId parentTaskId,
                                    Map<String, String> headers, Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId) {
        return new EqlSearchTask(id, type, action, request.getDescription(), parentTaskId, headers, originHeaders, asyncExecutionId,
            request.keepAlive());
    }

    @Override
    public void execute(EqlSearchRequest request, EqlSearchTask task, ActionListener<EqlSearchResponse> listener) {
        operation(planExecutor, task, request, username(securityContext), transportService, clusterService, listener);
    }

    @Override
    public EqlSearchResponse initialResponse(EqlSearchTask task) {
        return new EqlSearchResponse(EqlSearchResponse.Hits.EMPTY,
            TimeValue.nsecToMSec(task.getStartTimeNanos()) - threadPool.relativeTimeInMillis(), false, task.getExecutionId().getEncoded(),
            true, true);
    }

    @Override
    public EqlSearchResponse readResponse(StreamInput inputStream) throws IOException {
        return new EqlSearchResponse(inputStream);
    }

    @Override
    protected void doExecute(Task task, EqlSearchRequest request, ActionListener<EqlSearchResponse> listener) {
        if (request.waitForCompletionTimeout() != null && request.waitForCompletionTimeout().getMillis() >= 0) {
            asyncTaskManagementService.asyncExecute(request, request.waitForCompletionTimeout(), request.keepAlive(),
                request.keepOnCompletion(), listener);
        } else {
            operation(planExecutor, (EqlSearchTask) task, request, username(securityContext), transportService, clusterService, listener);
        }
    }

    public static void operation(PlanExecutor planExecutor, EqlSearchTask task, EqlSearchRequest request, String username,
                                 TransportService transportService, ClusterService clusterService,
                                 ActionListener<EqlSearchResponse> listener) {
        String nodeId = clusterService.localNode().getId();
        String clusterName = clusterName(clusterService);
        // TODO: these should be sent by the client
        ZoneId zoneId = DateUtils.of("Z");
        QueryBuilder filter = request.filter();
        List<FieldAndFormat> fetchFields = request.fetchFields();
        TimeValue timeout = TimeValue.timeValueSeconds(30);
        String clientId = null;

        ParserParams params = new ParserParams(zoneId)
            .fieldEventCategory(request.eventCategoryField())
            .fieldTimestamp(request.timestampField())
            .fieldTiebreaker(request.tiebreakerField())
            .resultPosition("tail".equals(request.resultPosition()) ? Order.OrderDirection.DESC : Order.OrderDirection.ASC)
            .size(request.size())
            .fetchSize(request.fetchSize());

        RemoteClusterRegistry remoteClusterRegistry = new RemoteClusterRegistry(transportService.getRemoteClusterService(),
            request.indicesOptions());
        EqlConfiguration cfg = new EqlConfiguration(request.indices(), zoneId, username, clusterName, filter,
                request.runtimeMappings(), fetchFields, timeout, request.indicesOptions(), request.fetchSize(),
                clientId, new TaskId(nodeId, task.getId()), task, remoteClusterRegistry::versionIncompatibleClusters);
        executeRequestWithRetryAttempt(clusterService, listener::onFailure,
            onFailure -> planExecutor.eql(cfg, request.query(), params,
                wrap(r -> listener.onResponse(createResponse(r, task.getExecutionId())), onFailure)),
            node -> transportService.sendRequest(node, EqlSearchAction.NAME, request,
                new ActionListenerResponseHandler<>(listener, EqlSearchResponse::new, ThreadPool.Names.SAME)),
            log);
    }

    static EqlSearchResponse createResponse(Results results, AsyncExecutionId id) {
        EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(results.events(), results.sequences(), results.totalHits());
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
