/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlStreamQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlStreamQueryRequest;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.StreamingOutputExec;
import org.elasticsearch.xpack.esql.session.EsqlSession.PlanRunner;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction.getOrCreateSessionID;

/**
 * Transport action for the streaming ES|QL query endpoint ({@code POST /_query/stream}).
 * Mirrors {@link TransportEsqlQueryAction} but delivers the schema and publisher out-of-band
 * (via {@link EsqlStreamQueryRequest#streamStartListener()}) before compute finishes, so the
 * transport task stays registered for the full duration of the query. This keeps
 * {@link org.elasticsearch.rest.action.RestCancellableNodeClient} working correctly: the task
 * remains in its close set until compute is done, so a client disconnect issues a cancellation
 * and {@code ((CancellableTask) task)::isCancelled} flips as expected.
 */
public class TransportEsqlStreamQueryAction extends TransportAction<EsqlStreamQueryRequest, ActionResponse.Empty> {

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
    private final Client client;
    private volatile boolean defaultAllowPartialResults;
    private volatile int resultTruncationMaxSize;
    private volatile int resultTruncationDefaultSize;
    private volatile int timeseriesResultTruncationMaxSize;
    private volatile int timeseriesResultTruncationDefaultSize;

    // TODO: Depend on ComputeService/TransportActionServices/EnrichPolicyResolver/DatasetResolver
    // directly instead of on TransportEsqlQueryAction. Blocked on those four being constructed
    // inside TransportEsqlQueryAction's constructor rather than registered as injectable components.
    @Inject
    @SuppressWarnings("this-escape")
    public TransportEsqlStreamQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        PlanExecutor planExecutor,
        TransportEsqlQueryAction transportEsqlQueryAction,
        ClusterService clusterService,
        ViewResolver viewResolver,
        Client client
    ) {
        super(EsqlStreamQueryAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.planExecutor = planExecutor;
        this.services = transportEsqlQueryAction.services();
        this.computeService = transportEsqlQueryAction.computeService();
        this.enrichPolicyResolver = transportEsqlQueryAction.enrichPolicyResolver();
        this.datasetResolver = transportEsqlQueryAction.datasetResolver();
        this.clusterService = clusterService;
        this.viewResolver = viewResolver;
        this.requestExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.client = client;

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
    protected void doExecute(Task task, EsqlStreamQueryRequest request, ActionListener<ActionResponse.Empty> listener) {
        requestExecutor.execute(ActionRunnable.wrap(listener, l -> innerExecute(task, request, l)));
    }

    private void innerExecute(Task task, EsqlStreamQueryRequest request, ActionListener<ActionResponse.Empty> listener) {
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
        AtomicBoolean streamStarted = new AtomicBoolean(false);

        PlanRunner planRunner = (plan, configuration, foldCtx, planTimeProfile, resultListener) -> {
            List<ColumnInfoImpl> columns = buildColumns(plan.output());

            Consumer<boolean[]> startCompute = nullColumns -> {
                StreamingOutputExec streamingPlan = new StreamingOutputExec(plan, publisher);
                request.streamStartListener().onResponse(new EsqlStreamQueryAction.StreamStart(columns, publisher, nullColumns));
                Exception startFailure = publisher.failure();
                if (startFailure != null) {
                    resultListener.onFailure(startFailure);
                    return;
                }
                streamStarted.set(true);
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

            if (request.dropNullColumns()) {
                boolean[] noColumnsDropped = new boolean[columns.size()];
                Set<String> indexFieldNames = collectIndexFieldNames(plan.output());
                if (indexFieldNames.isEmpty()) {
                    startCompute.accept(noColumnsDropped);
                } else {
                    Set<String> indexPatterns = collectIndexPatterns(plan);
                    FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest();
                    fieldCapsRequest.indices(indexPatterns.toArray(String[]::new));
                    fieldCapsRequest.fields(indexFieldNames.toArray(String[]::new));
                    fieldCapsRequest.includeEmptyFields(false);
                    client.execute(TransportFieldCapabilitiesAction.TYPE, fieldCapsRequest, ActionListener.wrap(response -> {
                        Set<String> nonEmptyFields = response.get().keySet();
                        Set<String> emptyFieldNames = new HashSet<>(indexFieldNames);
                        emptyFieldNames.removeAll(nonEmptyFields);
                        startCompute.accept(classifyNullColumns(plan.output(), emptyFieldNames));
                    }, ex -> {
                        logger.warn("drop_null_columns: failed to check for empty fields; all columns will be shown", ex);
                        startCompute.accept(noColumnsDropped);
                    }));
                }
            } else {
                startCompute.accept(null);
            }
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
                markPartialFromCompletionInfo(versionedResult.inner());
                long tookMillis = executionInfo.overallTook() != null ? executionInfo.overallTook().millis() : 0L;
                List<String> warnings = extractWarnings();
                publisher.completeWithFooter(tookMillis, warnings, executionInfo.isPartial());
                planExecutor.metrics().recordTook(tookMillis);
                listener.onResponse(ActionResponse.Empty.INSTANCE);
            }, ex -> {
                if (streamStarted.get() == false) {
                    listener.onFailure(ex);
                } else {
                    publisher.failStream(ex);
                    listener.onFailure(ex);
                }
            })
        );
    }

    static List<ColumnInfoImpl> buildColumns(List<Attribute> output) {
        return output.stream().map(c -> {
            List<String> originalTypes = null;
            if (c instanceof UnsupportedAttribute ua) {
                originalTypes = new ArrayList<>(ua.originalTypes());
                Collections.sort(originalTypes);
            }
            return new ColumnInfoImpl(c.name(), c.dataType(), originalTypes, null);
        }).toList();
    }

    static Set<String> collectIndexFieldNames(List<Attribute> output) {
        Set<String> fieldNames = new HashSet<>();
        for (Attribute attr : output) {
            if (attr instanceof FieldAttribute fa && (attr instanceof UnsupportedAttribute) == false) {
                fieldNames.add(fa.fieldName().string());
            }
        }
        return fieldNames;
    }

    static Set<String> collectIndexPatterns(PhysicalPlan plan) {
        Set<String> patterns = new HashSet<>();
        plan.forEachDown(EsQueryExec.class, exec -> patterns.add(exec.indexPattern()));
        plan.forEachDown(EsSourceExec.class, exec -> patterns.add(exec.indexPattern()));
        plan.forEachDown(
            FragmentExec.class,
            frag -> frag.fragment().forEachDown(EsRelation.class, rel -> patterns.add(rel.indexPattern()))
        );
        return patterns;
    }

    static boolean[] classifyNullColumns(List<Attribute> output, Set<String> emptyFieldNames) {
        boolean[] nullColumns = new boolean[output.size()];
        for (int i = 0; i < output.size(); i++) {
            Attribute attr = output.get(i);
            if (attr instanceof FieldAttribute fa && (attr instanceof UnsupportedAttribute) == false) {
                nullColumns[i] = emptyFieldNames.contains(fa.fieldName().string());
            }
        }
        return nullColumns;
    }

    static void markPartialFromCompletionInfo(Result result) {
        if (result.completionInfo().partial()) {
            assert result.executionInfo() != null : "a partial completion must carry an executionInfo to surface is_partial";
            if (result.executionInfo() != null) {
                result.executionInfo().markPartial();
            }
        }
    }

    private List<String> extractWarnings() {
        return threadPool.getThreadContext()
            .getResponseHeaders()
            .getOrDefault("Warning", List.of())
            .stream()
            .map(w -> HeaderWarning.extractWarningValueFromWarningHeader(w, false))
            .toList();
    }

    protected Executor externalBlobStoreExecutor() {
        return threadPool.executor(EsqlPlugin.externalBlobStorePool());
    }

    protected int externalSourceConcurrency() {
        return ExternalSourceSettings.blobStoreConcurrency(clusterService.getSettings());
    }
}
