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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlStreamQueryAction;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.plan.physical.StreamingOutputExec;
import org.elasticsearch.xpack.esql.session.EsqlSession.PlanRunner;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction.getOrCreateSessionID;

/**
 * Transport action for the streaming ES|QL query endpoint ({@code POST /_query/stream}).
 * <p>
 * Mirrors {@link TransportEsqlQueryAction} but responds to the REST listener immediately
 * after analysis (with schema + publisher), before compute finishes. Pages flow directly
 * from the compute driver through {@link org.elasticsearch.compute.operator.TieredPageOperator}
 * into the {@link PageStreamPublisher}, which the REST listener subscribes to.
 * </p>
 */
@ServerlessScope(Scope.PUBLIC)
public class TransportEsqlStreamQueryAction extends HandledTransportAction<EsqlQueryRequest, EsqlStreamQueryAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportEsqlStreamQueryAction.class);

    private final ThreadPool threadPool;
    private final PlanExecutor planExecutor;
    private final ComputeService computeService;
    private final EnrichPolicyResolver enrichPolicyResolver;
    private final ViewResolver viewResolver;
    private final DatasetResolver datasetResolver;
    private final RemoteClusterService remoteClusterService;
    private final TransportActionServices services;
    private final ClusterService clusterService;
    private final Executor requestExecutor;
    private volatile boolean defaultAllowPartialResults;
    private volatile int resultTruncationMaxSize;
    private volatile int resultTruncationDefaultSize;
    private volatile int timeseriesResultTruncationMaxSize;
    private volatile int timeseriesResultTruncationDefaultSize;

    // TODO: Swap TransportEsqlQueryAction to the underlying services and computer service we actually need from it.
    @Inject
    @SuppressWarnings("this-escape")
    public TransportEsqlStreamQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        PlanExecutor planExecutor,
        TransportEsqlQueryAction transportEsqlQueryAction,
        ClusterService clusterService,
        ViewResolver viewResolver
    ) {
        super(EsqlStreamQueryAction.NAME, transportService, actionFilters, EsqlQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.planExecutor = planExecutor;
        this.services = transportEsqlQueryAction.services();
        this.computeService = transportEsqlQueryAction.computeService();
        this.clusterService = clusterService;
        this.viewResolver = viewResolver;
        this.requestExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.enrichPolicyResolver = transportEsqlQueryAction.enrichPolicyResolver();
        this.datasetResolver = transportEsqlQueryAction.datasetResolver();

        defaultAllowPartialResults = EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS, v -> defaultAllowPartialResults = v);
        resultTruncationMaxSize = AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.get(clusterService.getSettings());
        resultTruncationDefaultSize = AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.get(clusterService.getSettings());
        timeseriesResultTruncationMaxSize = AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.get(clusterService.getSettings());
        timeseriesResultTruncationDefaultSize = AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.get(
            clusterService.getSettings()
        );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE, v -> {
            resultTruncationMaxSize = v;
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE, v -> {
            resultTruncationDefaultSize = v;
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE, v -> {
            timeseriesResultTruncationMaxSize = v;
        });
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE, v -> {
                timeseriesResultTruncationDefaultSize = v;
            });
    }

    @Override
    protected void doExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlStreamQueryAction.Response> listener) {
        requestExecutor.execute(ActionRunnable.wrap(listener, l -> innerExecute(task, request, l)));
    }

    private void innerExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlStreamQueryAction.Response> listener) {
        if (request.allowPartialResults() == null) {
            request.allowPartialResults(defaultAllowPartialResults);
        }

        EsqlFlags flags = computeService.createFlags();
        String sessionId = getOrCreateSessionID(task);
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(
            clusterAlias -> remoteClusterService.shouldSkipOnFailure(clusterAlias, request.allowPartialResults()),
            EsqlExecutionInfo.IncludeExecutionMetadata.NEVER
        );

        PageStreamPublisher publisher = new PageStreamPublisher(request.pageSize());
        AtomicBoolean responded = new AtomicBoolean(false);

        PlanRunner planRunner = (plan, configuration, foldCtx, planTimeProfile, resultListener) -> {
            List<ColumnInfoImpl> columns = buildColumns(plan.output());
            StreamingOutputExec streamingPlan = new StreamingOutputExec(plan, publisher);

            // Signal the REST listener before compute starts so HTTP headers can be sent
            responded.set(true);
            listener.onResponse(new EsqlStreamQueryAction.Response(columns, publisher));

            computeService.execute(
                sessionId,
                (CancellableTask) task,
                flags,
                streamingPlan,
                configuration,
                foldCtx,
                executionInfo,
                planTimeProfile,
                resultListener
            );
        };

        planExecutor.esql(
            request,
            sessionId,
            clusterService.state().getMinTransportVersion(),
            new AnalyzerSettings(
                resultTruncationMaxSize,
                resultTruncationDefaultSize,
                timeseriesResultTruncationMaxSize,
                timeseriesResultTruncationDefaultSize
            ),
            enrichPolicyResolver,
            viewResolver,
            datasetResolver,
            executionInfo,
            remoteClusterService,
            planRunner,
            services,
            externalBlobStoreExecutor(),
            externalSourceConcurrency(),
            ((CancellableTask) task)::isCancelled,
            ActionListener.wrap(versionedResult -> {
                long tookMillis = executionInfo.overallTook() != null ? executionInfo.overallTook().millis() : 0L;
                List<String> warnings = extractWarnings();
                publisher.completeWithFooter(tookMillis, warnings);
                planExecutor.metrics().recordTook(tookMillis);
            }, ex -> {
                if (responded.get() == false) {
                    listener.onFailure(ex);
                } else {
                    publisher.failStream(ex);
                }
            })
        );
    }

    private static List<ColumnInfoImpl> buildColumns(List<Attribute> output) {
        return output.stream().map(c -> {
            List<String> originalTypes = null;
            if (c instanceof UnsupportedAttribute ua) {
                originalTypes = new ArrayList<>(ua.originalTypes());
                Collections.sort(originalTypes);
            }
            return new ColumnInfoImpl(c.name(), c.dataType(), originalTypes, null);
        }).toList();
    }

    private List<String> extractWarnings() {
        return threadPool.getThreadContext().getResponseHeaders().getOrDefault("Warning", List.of());
    }

    protected Executor externalBlobStoreExecutor() {
        return threadPool.executor(EsqlPlugin.externalBlobStorePool());
    }

    protected int externalSourceConcurrency() {
        return ExternalSourceSettings.blobStoreConcurrency(clusterService.getSettings());
    }
}
