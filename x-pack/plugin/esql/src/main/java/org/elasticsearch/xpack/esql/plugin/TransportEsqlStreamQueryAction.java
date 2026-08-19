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
import org.elasticsearch.common.logging.activity.ActivityLogger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.compute.operator.PlanTimeProfile;
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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.StreamingOutputExec;
import org.elasticsearch.xpack.esql.querylog.EsqlLogContext;
import org.elasticsearch.xpack.esql.querylog.EsqlStreamLogContextBuilder;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession.PlanRunner;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    /**
     * ES|QL types for which a field_caps {@code include_empty_fields=false} omission is a faithful
     * "this column is all null" signal: the mapper writes Lucene structures under
     * {@link org.elasticsearch.index.mapper.MappedFieldType#name()} whenever a value is present,
     * and the block loader reads them via doc values.
     *
     * <p>Deliberately excludes {@code AGGREGATE_METRIC_DOUBLE}, which reports
     * {@code isAggregatable() == true} unconditionally while writing its doc values under
     * {@code <name>.min}/{@code .max}/..., and has no {@code fieldHasValue} override —
     * so field_caps reports it empty even when fully populated.
     * Adding a type here without confirming both properties is a data-loss bug.
     */
    private static final Set<DataType> FIELD_CAPS_EMPTINESS_IS_TRUSTWORTHY = Set.of(
        DataType.KEYWORD,
        DataType.BOOLEAN,
        DataType.LONG,
        DataType.INTEGER,
        DataType.SHORT,
        DataType.BYTE,
        DataType.UNSIGNED_LONG,
        DataType.DOUBLE,
        DataType.FLOAT,
        DataType.HALF_FLOAT,
        DataType.SCALED_FLOAT,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.IP,
        DataType.VERSION,
        DataType.GEO_POINT,
        DataType.CARTESIAN_POINT,
        DataType.GEO_SHAPE,
        DataType.CARTESIAN_SHAPE
    );

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
    private final ActivityLogger<EsqlLogContext> activityLogger;
    private final TransportEsqlQueryAction transportEsqlQueryAction;
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
        this.activityLogger = transportEsqlQueryAction.activityLogger();
        this.transportEsqlQueryAction = transportEsqlQueryAction;
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
        requestExecutor.execute(ActionRunnable.wrap(listener, l -> prepareAndExecuteWithLogging(task, request, l)));
    }

    private void prepareAndExecuteWithLogging(Task task, EsqlStreamQueryRequest request, ActionListener<ActionResponse.Empty> listener) {
        if (request.allowPartialResults() == null) {
            request.allowPartialResults(defaultAllowPartialResults);
        }
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(
            clusterAlias -> remoteClusterService.shouldSkipOnFailure(clusterAlias, request.allowPartialResults()),
            EsqlExecutionInfo.IncludeExecutionMetadata.NEVER
        );
        PageStreamPublisher publisher = new PageStreamPublisher(request.pageSize());
        AtomicReference<Result> resultRef = new AtomicReference<>();
        activityLogger.wrapAndRun(
            listener,
            new EsqlStreamLogContextBuilder(task, request, executionInfo, publisher, resultRef::get),
            l -> innerExecute(task, request, executionInfo, publisher, resultRef, l)
        );
    }

    private void innerExecute(
        Task task,
        EsqlStreamQueryRequest request,
        EsqlExecutionInfo executionInfo,
        PageStreamPublisher publisher,
        AtomicReference<Result> resultRef,
        ActionListener<ActionResponse.Empty> listener
    ) {
        EsqlFlags flags = computeService.createFlags();
        String sessionId = getOrCreateSessionID(task);
        AtomicBoolean streamStarted = new AtomicBoolean(false);
        AtomicBoolean outputRunSeen = new AtomicBoolean(false);
        AtomicReference<Map<NameId, Map<String, Object>>> columnMetadataRef = new AtomicReference<>();

        PlanRunner planRunner = new PlanRunner() {
            @Override
            public void columnMetadata(Map<NameId, Map<String, Object>> columnMetadata) {
                columnMetadataRef.set(columnMetadata);
            }

            @Override
            public void run(
                Role role,
                PhysicalPlan plan,
                Configuration configuration,
                FoldContext foldCtx,
                PlanTimeProfile planTimeProfile,
                ActionListener<Result> resultListener
            ) {
                if (role == PlanRunner.Role.INTERMEDIATE) {
                    computeService.execute(
                        sessionId,
                        (CancellableTask) task,
                        flags,
                        plan,
                        configuration,
                        foldCtx,
                        executionInfo,
                        planTimeProfile,
                        resultListener
                    );
                    return;
                }

                if (outputRunSeen.compareAndSet(false, true) == false) {
                    throw new IllegalStateException("a streaming query may only run one OUTPUT plan; got a second one");
                }

                assert columnMetadataRef.get() != null : "columnMetadata() must be called before run() for the OUTPUT plan";
                List<ColumnInfoImpl> columns = buildColumns(plan.output(), columnMetadataRef.get());

                Consumer<boolean[]> startCompute = nullColumns -> {
                    try {
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
                    } catch (Exception e) {
                        resultListener.onFailure(e);
                    }
                };

                if (request.dropNullColumns()) {
                    boolean[] noColumnsDropped = new boolean[columns.size()];
                    AttributeMap<Attribute> aliasSources = collectAliasSources(plan);
                    String[] fieldNames = resolveIndexFieldNames(plan.output(), aliasSources);
                    Set<String> indexNames = collectIndexNames(plan);
                    Set<String> indexFieldNames = new HashSet<>();
                    for (String name : fieldNames) {
                        if (name != null) {
                            indexFieldNames.add(name);
                        }
                    }
                    if (indexFieldNames.isEmpty() || indexNames.isEmpty()) {
                        startCompute.accept(noColumnsDropped);
                    } else {
                        FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest();
                        fieldCapsRequest.indices(indexNames.toArray(String[]::new));
                        fieldCapsRequest.fields(indexFieldNames.toArray(String[]::new));
                        fieldCapsRequest.includeEmptyFields(false);
                        fieldCapsRequest.indicesOptions(IndexResolver.DEFAULT_OPTIONS);
                        fieldCapsRequest.returnLocalAll(false);
                        fieldCapsRequest.filters("-nested");
                        client.execute(TransportFieldCapabilitiesAction.TYPE, fieldCapsRequest, ActionListener.wrap(response -> {
                            Set<String> nonEmptyFields = response.get().keySet();
                            boolean[] nullColumns = new boolean[fieldNames.length];
                            for (int i = 0; i < fieldNames.length; i++) {
                                if (fieldNames[i] != null && nonEmptyFields.contains(fieldNames[i]) == false) {
                                    nullColumns[i] = true;
                                }
                            }
                            startCompute.accept(nullColumns);
                        }, ex -> {
                            logger.warn("drop_null_columns: failed to check for empty fields; all columns will be shown", ex);
                            startCompute.accept(noColumnsDropped);
                        }));
                    }
                } else {
                    startCompute.accept(null);
                }
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
                transportEsqlQueryAction.recordCCSTelemetry(task, executionInfo, request, null);
                markPartialFromCompletionInfo(versionedResult.inner());
                resultRef.set(versionedResult.inner());
                long tookMillis = executionInfo.overallTook() != null ? executionInfo.overallTook().millis() : 0L;
                List<String> warnings = footerWarnings(threadPool.getThreadContext(), versionedResult.inner().completionInfo());
                publisher.completeWithFooter(tookMillis, warnings, executionInfo.isPartial());
                planExecutor.metrics().recordTook(tookMillis);
                listener.onResponse(ActionResponse.Empty.INSTANCE);
            }, ex -> {
                transportEsqlQueryAction.recordCCSTelemetry(task, executionInfo, request, ex);
                if (streamStarted.get()) {
                    publisher.failStream(ex);
                }
                listener.onFailure(ex);
            })
        );
    }

    static List<ColumnInfoImpl> buildColumns(List<Attribute> output, Map<NameId, Map<String, Object>> columnMetadata) {
        return output.stream().map(c -> {
            List<String> originalTypes = null;
            if (c instanceof UnsupportedAttribute ua) {
                originalTypes = new ArrayList<>(ua.originalTypes());
                Collections.sort(originalTypes);
            }
            return new ColumnInfoImpl(c.name(), c.dataType(), originalTypes, columnMetadata.get(c.id()));
        }).toList();
    }

    static AttributeMap<Attribute> collectAliasSources(PhysicalPlan plan) {
        AttributeMap.Builder<Attribute> builder = AttributeMap.builder();
        plan.forEachExpressionDown(Alias.class, alias -> {
            if (alias.child() instanceof Attribute attr) {
                builder.put(alias.toAttribute(), attr);
            }
        });
        plan.forEachDown(FragmentExec.class, frag -> frag.fragment().forEachExpressionDown(Alias.class, alias -> {
            if (alias.child() instanceof Attribute attr) {
                builder.put(alias.toAttribute(), attr);
            }
        }));
        return builder.build();
    }

    static String[] resolveIndexFieldNames(List<Attribute> output, AttributeMap<Attribute> aliasSources) {
        String[] fieldNames = new String[output.size()];
        for (int i = 0; i < output.size(); i++) {
            Attribute terminal = aliasSources.resolve(output.get(i), output.get(i));
            if (terminal instanceof FieldAttribute fa
                && (terminal instanceof UnsupportedAttribute) == false
                && fa.field().isAggregatable()
                && FIELD_CAPS_EMPTINESS_IS_TRUSTWORTHY.contains(fa.dataType())) {
                fieldNames[i] = fa.fieldName().string();
            }
        }
        return fieldNames;
    }

    static Set<String> collectIndexNames(PhysicalPlan plan) {
        Set<String> names = new HashSet<>();
        plan.forEachDown(
            FragmentExec.class,
            frag -> frag.fragment().forEachDown(EsRelation.class, rel -> names.addAll(rel.concreteQualifiedIndices()))
        );
        return names;
    }

    static void markPartialFromCompletionInfo(Result result) {
        if (result.completionInfo().partial()) {
            assert result.executionInfo() != null : "a partial completion must carry an executionInfo to surface is_partial";
            if (result.executionInfo() != null) {
                result.executionInfo().markPartial();
            }
        }
    }

    static List<String> footerWarnings(ThreadContext threadContext, DriverCompletionInfo completionInfo) {
        LinkedHashSet<String> warnings = new LinkedHashSet<>(completionInfo.warnings());
        for (String header : threadContext.getResponseHeaders().getOrDefault("Warning", List.of())) {
            warnings.add(HeaderWarning.decodeAndUnescape(HeaderWarning.extractWarningValueFromWarningHeader(header, false)));
        }
        return List.copyOf(warnings);
    }

    protected Executor externalBlobStoreExecutor() {
        return threadPool.executor(EsqlPlugin.externalBlobStorePool());
    }

    protected int externalSourceConcurrency() {
        return ExternalSourceSettings.blobStoreConcurrency(clusterService.getSettings());
    }
}
