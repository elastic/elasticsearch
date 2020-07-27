/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
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
import org.elasticsearch.xpack.eql.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.Results;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportEqlSearchAction extends HandledTransportAction<EqlSearchRequest, EqlSearchResponse>
    implements AsyncTaskManagementService.AsyncOperation<EqlSearchRequest, EqlSearchResponse, EqlSearchTask> {

    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;
    private final ThreadPool threadPool;
    private final AsyncTaskManagementService<EqlSearchRequest, EqlSearchResponse, EqlSearchTask> asyncTaskManagementService;

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

        this.asyncTaskManagementService = new AsyncTaskManagementService<>(XPackPlugin.ASYNC_RESULTS_INDEX, client, ASYNC_SEARCH_ORIGIN,
            registry, taskManager, EqlSearchAction.INSTANCE.name(), this, EqlSearchTask.class, clusterService, threadPool);
    }

    @Override
    public EqlSearchTask createTask(EqlSearchRequest request, long id, String type, String action, TaskId parentTaskId,
                                    Map<String, String> headers, Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId) {
        return new EqlSearchTask(id, type, action, request.getDescription(), parentTaskId, headers, originHeaders, asyncExecutionId,
            request.keepAlive());
    }

    @Override
    public void execute(EqlSearchRequest request, EqlSearchTask task, ActionListener<EqlSearchResponse> listener) {
        operation(planExecutor, task, request, username(securityContext), clusterName(clusterService),
            clusterService.localNode().getId(), listener);
    }

    @Override
    public EqlSearchResponse initialResponse(EqlSearchTask task) {
        return new EqlSearchResponse(EqlSearchResponse.Hits.EMPTY,
            threadPool.relativeTimeInMillis() - task.getStartTime(), false, task.getExecutionId().getEncoded(), true, true);
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
            operation(planExecutor, (EqlSearchTask) task, request, username(securityContext), clusterName(clusterService),
                clusterService.localNode().getId(), listener);
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

        ParserParams params = new ParserParams(zoneId)
            .fieldEventCategory(request.eventCategoryField())
            .fieldTimestamp(request.timestampField())
            .fieldTiebreaker(request.tiebreakerField())
            .size(request.size())
            .fetchSize(request.fetchSize());

        EqlConfiguration cfg = new EqlConfiguration(request.indices(), zoneId, username, clusterName, filter, timeout, includeFrozen,
            request.isCaseSensitive(), clientId, new TaskId(nodeId, task.getId()), task);
        planExecutor.eql(cfg, request.query(), params, wrap(r -> listener.onResponse(createResponse(r, task.getExecutionId())),
            listener::onFailure));
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
