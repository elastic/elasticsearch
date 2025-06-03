/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.stats.CCSUsage;
import org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlQueryTask;
import org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.enrich.AbstractLookupService;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession.PlanRunner;
import org.elasticsearch.xpack.esql.session.Result;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class TransportEsqlQueryAction extends HandledTransportAction<EsqlQueryRequest, EsqlQueryResponse>
    implements
        AsyncTaskManagementService.AsyncOperation<EsqlQueryRequest, EsqlQueryResponse, EsqlQueryTask> {

    private final ThreadPool threadPool;
    private final PlanExecutor planExecutor;
    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final ClusterService clusterService;
    private final Executor requestExecutor;
    private final EnrichPolicyResolver enrichPolicyResolver;
    private final EnrichLookupService enrichLookupService;
    private final LookupFromIndexService lookupFromIndexService;
    private final AsyncTaskManagementService<EsqlQueryRequest, EsqlQueryResponse, EsqlQueryTask> asyncTaskManagementService;
    private final RemoteClusterService remoteClusterService;
    private final UsageService usageService;
    private final TransportActionServices services;
    private volatile boolean defaultAllowPartialResults;

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
        BlockFactoryProvider blockFactoryProvider,
        Client client,
        NamedWriteableRegistry registry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        UsageService usageService
    ) {
        // TODO replace SAME when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(EsqlQueryAction.NAME, transportService, actionFilters, EsqlQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.planExecutor = planExecutor;
        this.clusterService = clusterService;
        this.requestExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        exchangeService.registerTransportHandler(transportService);
        this.exchangeService = exchangeService;
        this.enrichPolicyResolver = new EnrichPolicyResolver(clusterService, transportService, planExecutor.indexResolver());
        AbstractLookupService.LookupShardContextFactory lookupLookupShardContextFactory = AbstractLookupService.LookupShardContextFactory
            .fromSearchService(searchService);
        this.enrichLookupService = new EnrichLookupService(
            clusterService,
            searchService.getIndicesService(),
            lookupLookupShardContextFactory,
            transportService,
            indexNameExpressionResolver,
            bigArrays,
            blockFactoryProvider.blockFactory()
        );
        this.lookupFromIndexService = new LookupFromIndexService(
            clusterService,
            searchService.getIndicesService(),
            lookupLookupShardContextFactory,
            transportService,
            indexNameExpressionResolver,
            bigArrays,
            blockFactoryProvider.blockFactory()
        );
        this.computeService = new ComputeService(
            searchService,
            transportService,
            exchangeService,
            enrichLookupService,
            lookupFromIndexService,
            clusterService,
            threadPool,
            bigArrays,
            blockFactoryProvider.blockFactory()
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
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.usageService = usageService;

        this.services = new TransportActionServices(
            transportService,
            searchService,
            exchangeService,
            clusterService,
            indexNameExpressionResolver,
            usageService
        );
        defaultAllowPartialResults = EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS, v -> defaultAllowPartialResults = v);
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
        // set EsqlExecutionInfo on async-search task so that it is accessible to GET _query/async while the query is still running
        task.setExecutionInfo(createEsqlExecutionInfo(request));
        ActionListener.run(listener, l -> innerExecute(task, request, l));
    }

    private void innerExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        if (request.allowPartialResults() == null) {
            request.allowPartialResults(defaultAllowPartialResults);
        }
        Configuration configuration = new Configuration(
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
            request.tables(),
            System.nanoTime(),
            request.allowPartialResults()
        );
        String sessionId = sessionID(task);
        // async-query uses EsqlQueryTask, so pull the EsqlExecutionInfo out of the task
        // sync query uses CancellableTask which does not have EsqlExecutionInfo, so create one
        EsqlExecutionInfo executionInfo = getOrCreateExecutionInfo(task, request);
        FoldContext foldCtx = configuration.newFoldContext();
        PlanRunner planRunner = (plan, resultListener) -> computeService.execute(
            sessionId,
            (CancellableTask) task,
            plan,
            configuration,
            foldCtx,
            executionInfo,
            resultListener
        );
        planExecutor.esql(
            request,
            sessionId,
            configuration,
            foldCtx,
            enrichPolicyResolver,
            executionInfo,
            remoteClusterService,
            planRunner,
            services,
            ActionListener.wrap(result -> {
                recordCCSTelemetry(task, executionInfo, request, null);
                listener.onResponse(toResponse(task, request, configuration, result));
            }, ex -> {
                recordCCSTelemetry(task, executionInfo, request, ex);
                listener.onFailure(ex);
            })
        );

    }

    private void recordCCSTelemetry(Task task, EsqlExecutionInfo executionInfo, EsqlQueryRequest request, @Nullable Exception exception) {
        if (executionInfo.isCrossClusterSearch() == false) {
            return;
        }

        CCSUsage.Builder usageBuilder = new CCSUsage.Builder();
        usageBuilder.setClientFromTask(task);
        if (exception != null) {
            if (exception instanceof VerificationException ve) {
                CCSUsageTelemetry.Result failureType = classifyVerificationException(ve);
                if (failureType != CCSUsageTelemetry.Result.UNKNOWN) {
                    usageBuilder.setFailure(failureType);
                } else {
                    usageBuilder.setFailure(exception);
                }
            } else {
                usageBuilder.setFailure(exception);
            }
        }
        var took = executionInfo.overallTook();
        if (took != null) {
            usageBuilder.took(took.getMillis());
        }
        if (request.async()) {
            usageBuilder.setFeature(CCSUsageTelemetry.ASYNC_FEATURE);
        }

        AtomicInteger remotesCount = new AtomicInteger();
        executionInfo.getClusters().forEach((clusterAlias, cluster) -> {
            if (cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.SKIPPED) {
                usageBuilder.skippedRemote(clusterAlias);
            } else {
                usageBuilder.perClusterUsage(clusterAlias, cluster.getTook());
            }
            if (clusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false) {
                remotesCount.getAndIncrement();
            }
        });
        assert remotesCount.get() > 0 : "Got cross-cluster search telemetry without any remote clusters";
        usageBuilder.setRemotesCount(remotesCount.get());
        usageService.getEsqlUsageHolder().updateUsage(usageBuilder.build());
    }

    private CCSUsageTelemetry.Result classifyVerificationException(VerificationException exception) {
        if (exception.getDetailedMessage().contains("Unknown index")) {
            return CCSUsageTelemetry.Result.NOT_FOUND;
        }
        return CCSUsageTelemetry.Result.UNKNOWN;
    }

    private EsqlExecutionInfo getOrCreateExecutionInfo(Task task, EsqlQueryRequest request) {
        if (task instanceof EsqlQueryTask esqlQueryTask && esqlQueryTask.executionInfo() != null) {
            return esqlQueryTask.executionInfo();
        } else {
            return createEsqlExecutionInfo(request);
        }
    }

    private EsqlExecutionInfo createEsqlExecutionInfo(EsqlQueryRequest request) {
        return new EsqlExecutionInfo(clusterAlias -> remoteClusterService.isSkipUnavailable(clusterAlias), request.includeCCSMetadata());
    }

    private EsqlQueryResponse toResponse(Task task, EsqlQueryRequest request, Configuration configuration, Result result) {
        List<ColumnInfoImpl> columns = result.schema().stream().map(c -> {
            List<String> originalTypes;
            if (c.originalTypes() == null) {
                originalTypes = null;
            } else {
                // Sort the original types so they are easier to test against and prettier.
                originalTypes = new ArrayList<>(c.originalTypes());
                Collections.sort(originalTypes);
            }
            return new ColumnInfoImpl(c.name(), c.dataType().outputType(), originalTypes);
        }).toList();
        EsqlQueryResponse.Profile profile = configuration.profile() ? new EsqlQueryResponse.Profile(result.profiles()) : null;
        threadPool.getThreadContext().addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_IS_RUNNING_HEADER, "?0");
        if (task instanceof EsqlQueryTask asyncTask && request.keepOnCompletion()) {
            String asyncExecutionId = asyncTask.getExecutionId().getEncoded();
            threadPool.getThreadContext().addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_ID_HEADER, asyncExecutionId);
            return new EsqlQueryResponse(
                columns,
                result.pages(),
                profile,
                request.columnar(),
                asyncExecutionId,
                false,
                request.async(),
                result.executionInfo()
            );
        }
        return new EsqlQueryResponse(columns, result.pages(), profile, request.columnar(), request.async(), result.executionInfo());
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
        var asyncExecutionId = task.getExecutionId().getEncoded();
        threadPool.getThreadContext().addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_ID_HEADER, asyncExecutionId);
        threadPool.getThreadContext().addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_IS_RUNNING_HEADER, "?1");
        return new EsqlQueryResponse(
            List.of(),
            List.of(),
            null,
            false,
            asyncExecutionId,
            true, // is_running
            true, // isAsync
            task.executionInfo()
        );
    }

    @Override
    public EsqlQueryResponse readResponse(StreamInput inputStream) throws IOException {
        throw new AssertionError("should not reach here");
    }

    private static boolean requestIsAsync(EsqlQueryRequest request) {
        return request.async();
    }

    public LookupFromIndexService getLookupFromIndexService() {
        return lookupFromIndexService;
    }
}
