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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
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
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;

public class TransportEsqlQueryAction extends HandledTransportAction<EsqlQueryRequest, EsqlQueryResponse> {

    private final PlanExecutor planExecutor;
    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final ClusterService clusterService;
    private final Executor requestExecutor;
    private final EnrichPolicyResolver enrichPolicyResolver;
    private final EnrichLookupService enrichLookupService;

    @Inject
    public TransportEsqlQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        SearchService searchService,
        ExchangeService exchangeService,
        ClusterService clusterService,
        ThreadPool threadPool,
        BigArrays bigArrays,
        BlockFactory blockFactory
    ) {
        // TODO replace SAME when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(EsqlQueryAction.NAME, transportService, actionFilters, EsqlQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.planExecutor = planExecutor;
        this.clusterService = clusterService;
        this.requestExecutor = threadPool.executor(EsqlPlugin.ESQL_THREAD_POOL_NAME);
        exchangeService.registerTransportHandler(transportService);
        this.exchangeService = exchangeService;
        this.enrichPolicyResolver = new EnrichPolicyResolver(clusterService, transportService, planExecutor.indexResolver());
        this.enrichLookupService = new EnrichLookupService(clusterService, searchService, transportService, bigArrays, blockFactory);
        this.computeService = new ComputeService(
            searchService,
            transportService,
            exchangeService,
            enrichLookupService,
            threadPool,
            bigArrays,
            blockFactory
        );
    }

    @Override
    protected void doExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        requestExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked(task, request, l)));
    }

    private void doExecuteForked(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        EsqlConfiguration configuration = new EsqlConfiguration(
            request.zoneId() != null ? request.zoneId() : ZoneOffset.UTC,
            request.locale() != null ? request.locale() : Locale.US,
            // TODO: plug-in security
            null,
            clusterService.getClusterName().value(),
            request.pragmas(),
            clusterService.getClusterSettings().get(EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE),
            clusterService.getClusterSettings().get(EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE),
            request.query()
        );
        String sessionId = sessionID(task);
        planExecutor.esql(
            request,
            sessionId,
            configuration,
            enrichPolicyResolver,
            listener.delegateFailureAndWrap(
                (delegate, physicalPlan) -> computeService.execute(
                    sessionId,
                    (CancellableTask) task,
                    physicalPlan,
                    configuration,
                    delegate.map(pages -> {
                        List<ColumnInfo> columns = physicalPlan.output()
                            .stream()
                            .map(c -> new ColumnInfo(c.qualifiedName(), EsqlDataTypes.outputType(c.dataType())))
                            .toList();
                        return new EsqlQueryResponse(columns, pages, request.columnar());
                    })
                )
            )
        );
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
}
