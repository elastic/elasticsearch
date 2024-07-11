/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlQueryTask;
import org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.session.Result;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportEsqlQueryAction extends HandledTransportAction<EsqlQueryRequest, EsqlQueryResponse>
    implements
        AsyncTaskManagementService.AsyncOperation<EsqlQueryRequest, EsqlQueryResponse, EsqlQueryTask> {

    private final PlanExecutor planExecutor;
    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final ClusterService clusterService;
    private final Executor requestExecutor;
    private final EnrichPolicyResolver enrichPolicyResolver;
    private final EnrichLookupService enrichLookupService;
    private final AsyncTaskManagementService<EsqlQueryRequest, EsqlQueryResponse, EsqlQueryTask> asyncTaskManagementService;

    @Inject
    @SuppressWarnings("this-escape")
    public TransportEsqlQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        SearchService searchService,
        ExchangeService exchangeService,
        ClusterService clusterService,
        ThreadPool threadPool,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        Client client,
        NamedWriteableRegistry registry

    ) {
        // TODO replace SAME when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(EsqlQueryAction.NAME, transportService, actionFilters, EsqlQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.planExecutor = planExecutor;
        this.clusterService = clusterService;
        this.requestExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        exchangeService.registerTransportHandler(transportService);
        this.exchangeService = exchangeService;
        this.enrichPolicyResolver = new EnrichPolicyResolver(clusterService, transportService, planExecutor.indexResolver());
        this.enrichLookupService = new EnrichLookupService(clusterService, searchService, transportService, bigArrays, blockFactory);
        this.computeService = new ComputeService(
            searchService,
            transportService,
            exchangeService,
            enrichLookupService,
            clusterService,
            threadPool,
            bigArrays,
            blockFactory
        );
        this.asyncTaskManagementService = new AsyncTaskManagementService<>(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            client,
            ASYNC_SEARCH_ORIGIN,
            registry,
            taskManager,
            EsqlQueryAction.INSTANCE.name(),
            this,
            EsqlQueryTask.class,
            clusterService,
            threadPool,
            bigArrays
        );
    }

    @Override
    protected void doExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        requestExecutor.execute(
            ActionRunnable.wrap(
                listener.<EsqlQueryResponse>delegateFailureAndWrap(ActionListener::respondAndRelease),
                l -> doExecuteForked(task, request, l)
            )
        );
    }

    private void doExecuteForked(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);
        if (requestIsAsync(request)) {
            asyncTaskManagementService.asyncExecute(
                request,
                request.waitForCompletionTimeout(),
                request.keepAlive(),
                request.keepOnCompletion(),
                listener
            );
        } else {
            innerExecute(task, request, listener);
        }
    }

    @Override
    public void execute(EsqlQueryRequest request, EsqlQueryTask task, ActionListener<EsqlQueryResponse> listener) {
        ActionListener.run(listener, l -> innerExecute(task, request, l));
    }

    private void innerExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        EsqlConfiguration configuration = new EsqlConfiguration(
            ZoneOffset.UTC,
            request.locale() != null ? request.locale() : Locale.US,
            // TODO: plug-in security
            null,
            clusterService.getClusterName().value(),
            request.pragmas(),
            clusterService.getClusterSettings().get(EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE),
            clusterService.getClusterSettings().get(EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE),
            request.query(),
            request.profile(),
            request.tables()
        );
        String sessionId = sessionID(task);
        BiConsumer<PhysicalPlan, ActionListener<Result>> runPhase = (physicalPlan, resultListener) -> computeService.execute(
            sessionId,
            (CancellableTask) task,
            physicalPlan,
            configuration,
            resultListener
        );

        planExecutor.esql(
            request,
            sessionId,
            configuration,
            enrichPolicyResolver,
            runPhase,
            listener.map(result -> toResponse(task, request, configuration, result))
        );
    }

    private EsqlQueryResponse toResponse(Task task, EsqlQueryRequest request, EsqlConfiguration configuration, Result result) {
        List<ColumnInfoImpl> columns = result.schema()
            .stream()
            .map(c -> new ColumnInfoImpl(c.qualifiedName(), c.dataType().outputType()))
            .toList();
        EsqlQueryResponse.Profile profile = configuration.profile() ? new EsqlQueryResponse.Profile(result.profiles()) : null;
        if (task instanceof EsqlQueryTask asyncTask && request.keepOnCompletion()) {
            String id = asyncTask.getExecutionId().getEncoded();
            return new EsqlQueryResponse(columns, result.pages(), profile, request.columnar(), id, false, request.async());
        }
        return new EsqlQueryResponse(columns, result.pages(), profile, request.columnar(), request.async());
    }

    /**
     * Returns the ID for this compute session. The ID is unique within the cluster, and is used
     * to identify the compute-session across nodes. The ID is just the TaskID of the task that
     * initiated the session.
     */
    final String sessionID(Task task) {
        return new TaskId(clusterService.localNode().getId(), task.getId()).toString();
    }

    public ExchangeService exchangeService() {
        return exchangeService;
    }

    public EnrichLookupService enrichLookupService() {
        return enrichLookupService;
    }

    @Override
    public EsqlQueryTask createTask(
        EsqlQueryRequest request,
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        Map<String, String> headers,
        Map<String, String> originHeaders,
        AsyncExecutionId asyncExecutionId
    ) {
        return new EsqlQueryTask(
            id,
            type,
            action,
            request.getDescription(),
            parentTaskId,
            headers,
            originHeaders,
            asyncExecutionId,
            request.keepAlive()
        );
    }

    @Override
    public EsqlQueryResponse initialResponse(EsqlQueryTask task) {
        return new EsqlQueryResponse(
            List.of(),
            List.of(),
            null,
            false,
            task.getExecutionId().getEncoded(),
            true, // is_running
            true // isAsync
        );
    }

    @Override
    public EsqlQueryResponse readResponse(StreamInput inputStream) throws IOException {
        throw new AssertionError("should not reach here");
    }

    private static boolean requestIsAsync(EsqlQueryRequest request) {
        return request.async();
    }
}
