/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.common.FunctionList;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext.ProjectAfterTopN;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PhysicalSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

/**
 * Once query is parsed and validated it is scheduled for execution by {@code org.elasticsearch.xpack.esql.plugin.ComputeService#execute}
 * This method is responsible for splitting physical plan into coordinator and data node plans.
 * <p>
 * Coordinator plan is immediately executed locally (using {@code org.elasticsearch.xpack.esql.plugin.ComputeService#runCompute})
 * and is prepared to collect and merge pages from data nodes into the final query result.
 * <p>
 * Data node plan is passed to {@code org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler#startComputeOnDataNodes}
 * that is responsible for
 * <ul>
 * <li>
 *     Determining list of nodes that contain shards referenced by the query with
 *     {@code org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender#searchShards}
 * </li>
 * <li>
 *     Each node in the list processed in
 *     {@code org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler#startComputeOnDataNodes}
 *     in order to
 *     <ul>
 *     <li>
 *         Open ExchangeSink on the target data node and link it with local ExchangeSource for the query
 *         using `internal:data/read/esql/open_exchange` transport request.
 *         {@see org.elasticsearch.compute.operator.exchange.ExchangeService#openExchange}
 *     </li>
 *     <li>
 *         Start data node plan execution on the target data node
 *         using `indices:data/read/esql/data` transport request
 *         {@see org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler#messageReceived}
 *         {@see org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler#runComputeOnDataNode}
 *     </li>
 *     <li>
 *         While coordinator plan executor is running it will read data from ExchangeSource that will poll pages
 *         from linked ExchangeSink on target data nodes or notify them that data set is already completed
 *         (for example when running FROM * | LIMIT 10 type of query) or query is canceled
 *         using `internal:data/read/esql/exchange` transport requests.
 *         {@see org.elasticsearch.compute.operator.exchange.ExchangeService.ExchangeTransportAction#messageReceived}
 *     </li>
 *     </ul>
 * </li>
 * </ul>
 */
public class ComputeService {
    public static final String DATA_DESCRIPTION = "data";
    public static final String REDUCE_DESCRIPTION = "node_reduce";
    public static final String DATA_ACTION_NAME = EsqlQueryAction.NAME + "/data";
    public static final String CLUSTER_ACTION_NAME = EsqlQueryAction.NAME + "/cluster";
    private static final String LOCAL_CLUSTER = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);
    private final SearchService searchService;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;

    private final TransportService transportService;
    private final DriverTaskRunner driverRunner;
    private final EnrichLookupService enrichLookupService;
    private final LookupFromIndexService lookupFromIndexService;
    private final InferenceService inferenceService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final AtomicLong childSessionIdGenerator = new AtomicLong();
    private final DataNodeComputeHandler dataNodeComputeHandler;
    private final ClusterComputeHandler clusterComputeHandler;
    private final ExchangeService exchangeService;
    private final PhysicalSettings physicalSettings;

    @SuppressWarnings("this-escape")
    public ComputeService(
        TransportActionServices transportActionServices,
        EnrichLookupService enrichLookupService,
        LookupFromIndexService lookupFromIndexService,
        ThreadPool threadPool,
        BigArrays bigArrays,
        BlockFactory blockFactory
    ) {
        this.searchService = transportActionServices.searchService();
        this.transportService = transportActionServices.transportService();
        this.exchangeService = transportActionServices.exchangeService();
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.blockFactory = blockFactory;
        var esqlExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.driverRunner = new DriverTaskRunner(transportService, esqlExecutor);
        this.enrichLookupService = enrichLookupService;
        this.lookupFromIndexService = lookupFromIndexService;
        this.inferenceService = transportActionServices.inferenceService();
        this.clusterService = transportActionServices.clusterService();
        this.projectResolver = transportActionServices.projectResolver();
        this.dataNodeComputeHandler = new DataNodeComputeHandler(
            this,
            clusterService,
            projectResolver,
            searchService,
            transportService,
            exchangeService,
            esqlExecutor
        );
        this.clusterComputeHandler = new ClusterComputeHandler(
            this,
            exchangeService,
            transportService,
            esqlExecutor,
            dataNodeComputeHandler
        );
        this.physicalSettings = new PhysicalSettings(clusterService);
    }

    public void execute(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        PhysicalPlan physicalPlan,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        ProjectAfterTopN projectAfterTopN,
        ActionListener<Result> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(
            EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
            TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX,
            ThreadPool.Names.SYSTEM_READ,
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION,
            MachineLearning.NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME
        );
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(physicalPlan);

        List<PhysicalPlan> subplans = subplansAndMainPlan.v1();

        // we have no sub plans, so we can just execute the given plan
        if (subplans == null || subplans.isEmpty()) {
            executePlan(
                sessionId,
                rootTask,
                flags,
                physicalPlan,
                configuration,
                foldContext,
                execInfo,
                null,
                projectAfterTopN,
                listener,
                null
            );
            return;
        }

        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        PhysicalPlan mainPlan = new OutputExec(subplansAndMainPlan.v2(), collectedPages::add);

        listener = listener.delegateResponse((l, e) -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            l.onFailure(e);
        });

        var mainSessionId = newChildSession(sessionId);
        QueryPragmas queryPragmas = configuration.pragmas();

        ExchangeSourceHandler mainExchangeSource = new ExchangeSourceHandler(
            queryPragmas.exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );

        exchangeService.addExchangeSourceHandler(mainSessionId, mainExchangeSource);
        try (var ignored = mainExchangeSource.addEmptySink()) {
            var finalListener = ActionListener.runBefore(listener, () -> exchangeService.removeExchangeSourceHandler(sessionId));
            var computeContext = new ComputeContext(
                mainSessionId,
                "main.final",
                LOCAL_CLUSTER,
                flags,
                FunctionList.empty(),
                configuration,
                foldContext,
                mainExchangeSource::createExchangeSource,
                null
            );

            Runnable cancelQueryOnFailure = cancelQueryOnFailure(rootTask);

            try (
                ComputeListener localListener = new ComputeListener(
                    transportService.getThreadPool(),
                    cancelQueryOnFailure,
                    finalListener.map(profiles -> {
                        execInfo.markEndQuery();
                        return new Result(mainPlan.output(), collectedPages, profiles, execInfo);
                    })
                )
            ) {
                runCompute(rootTask, computeContext, mainPlan, projectAfterTopN, localListener.acquireCompute());

                for (int i = 0; i < subplans.size(); i++) {
                    var subplan = subplans.get(i);
                    var childSessionId = newChildSession(sessionId);
                    ExchangeSinkHandler exchangeSink = exchangeService.createSinkHandler(childSessionId, queryPragmas.exchangeBufferSize());
                    // funnel sub plan pages into the main plan exchange source
                    mainExchangeSource.addRemoteSink(exchangeSink::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
                    var subPlanListener = localListener.acquireCompute();

                    executePlan(
                        childSessionId,
                        rootTask,
                        flags,
                        subplan,
                        configuration,
                        foldContext,
                        execInfo,
                        "subplan-" + i,
                        projectAfterTopN,
                        ActionListener.wrap(result -> {
                            exchangeSink.addCompletionListener(
                                ActionListener.running(() -> { exchangeService.finishSinkHandler(childSessionId, null); })
                            );
                            subPlanListener.onResponse(result.completionInfo());
                        }, e -> {
                            exchangeService.finishSinkHandler(childSessionId, e);
                            subPlanListener.onFailure(e);
                        }),
                        () -> exchangeSink.createExchangeSink(() -> {})
                    );
                }
            }
        }
    }

    public void executePlan(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        PhysicalPlan physicalPlan,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        String profileQualifier,
        ProjectAfterTopN projectAfterTopN,
        ActionListener<Result> listener,
        Supplier<ExchangeSink> exchangeSinkSupplier
    ) {
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            physicalPlan,
            configuration
        );
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        listener = listener.delegateResponse((l, e) -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            l.onFailure(e);
        });
        PhysicalPlan coordinatorPlan = coordinatorAndDataNodePlan.v1();

        if (exchangeSinkSupplier == null) {
            coordinatorPlan = new OutputExec(coordinatorAndDataNodePlan.v1(), collectedPages::add);
        }

        PhysicalPlan dataNodePlan = coordinatorAndDataNodePlan.v2();
        if (dataNodePlan != null && dataNodePlan instanceof ExchangeSinkExec == false) {
            assert false : "expected data node plan starts with an ExchangeSink; got " + dataNodePlan;
            listener.onFailure(new IllegalStateException("expected data node plan starts with an ExchangeSink; got " + dataNodePlan));
            return;
        }
        Map<String, OriginalIndices> clusterToConcreteIndices = transportService.getRemoteClusterService()
            .groupIndices(SearchRequest.DEFAULT_INDICES_OPTIONS, PlannerUtils.planConcreteIndices(physicalPlan).toArray(String[]::new));
        QueryPragmas queryPragmas = configuration.pragmas();
        Runnable cancelQueryOnFailure = cancelQueryOnFailure(rootTask);
        if (dataNodePlan == null) {
            if (clusterToConcreteIndices.values().stream().allMatch(v -> v.indices().length == 0) == false) {
                String error = "expected no concrete indices without data node plan; got " + clusterToConcreteIndices;
                assert false : error;
                listener.onFailure(new IllegalStateException(error));
                return;
            }
            var computeContext = new ComputeContext(
                newChildSession(sessionId),
                profileDescription(profileQualifier, "single"),
                LOCAL_CLUSTER,
                flags,
                FunctionList.empty(),
                configuration,
                foldContext,
                null,
                exchangeSinkSupplier
            );
            updateShardCountForCoordinatorOnlyQuery(execInfo);
            try (
                var computeListener = new ComputeListener(
                    transportService.getThreadPool(),
                    cancelQueryOnFailure,
                    listener.map(completionInfo -> {
                        updateExecutionInfoAfterCoordinatorOnlyQuery(execInfo);
                        return new Result(physicalPlan.output(), collectedPages, completionInfo, execInfo);
                    })
                )
            ) {
                runCompute(rootTask, computeContext, coordinatorPlan, projectAfterTopN, computeListener.acquireCompute());
                return;
            }
        } else {
            if (clusterToConcreteIndices.values().stream().allMatch(v -> v.indices().length == 0)) {
                var error = "expected concrete indices with data node plan but got empty; data node plan " + dataNodePlan;
                assert false : error;
                listener.onFailure(new IllegalStateException(error));
                return;
            }
        }
        Map<String, OriginalIndices> clusterToOriginalIndices = transportService.getRemoteClusterService()
            .groupIndices(SearchRequest.DEFAULT_INDICES_OPTIONS, PlannerUtils.planOriginalIndices(physicalPlan));
        var localOriginalIndices = clusterToOriginalIndices.remove(LOCAL_CLUSTER);
        var localConcreteIndices = clusterToConcreteIndices.remove(LOCAL_CLUSTER);
        /*
         * Grab the output attributes here, so we can pass them to
         * the listener without holding on to a reference to the
         * entire plan.
         */
        List<Attribute> outputAttributes = physicalPlan.output();
        var exchangeSource = new ExchangeSourceHandler(
            queryPragmas.exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        listener = ActionListener.runBefore(listener, () -> exchangeService.removeExchangeSourceHandler(sessionId));
        exchangeService.addExchangeSourceHandler(sessionId, exchangeSource);
        try (
            var computeListener = new ComputeListener(
                transportService.getThreadPool(),
                cancelQueryOnFailure,
                listener.delegateFailureAndWrap((l, completionInfo) -> {
                    failIfAllShardsFailed(execInfo, collectedPages);
                    execInfo.markEndQuery();  // TODO: revisit this time recording model as part of INLINESTATS improvements
                    l.onResponse(new Result(outputAttributes, collectedPages, completionInfo, execInfo));
                })
            )
        ) {
            try (Releasable ignored = exchangeSource.addEmptySink()) {
                // run compute on the coordinator
                final AtomicBoolean localClusterWasInterrupted = new AtomicBoolean();
                try (
                    var localListener = new ComputeListener(
                        transportService.getThreadPool(),
                        cancelQueryOnFailure,
                        computeListener.acquireCompute().delegateFailure((l, completionInfo) -> {
                            if (execInfo.clusterInfo.containsKey(LOCAL_CLUSTER)) {
                                execInfo.swapCluster(LOCAL_CLUSTER, (k, v) -> {
                                    var tookTime = execInfo.tookSoFar();
                                    var builder = new EsqlExecutionInfo.Cluster.Builder(v).setTook(tookTime);
                                    if (v.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                                        final Integer failedShards = execInfo.getCluster(LOCAL_CLUSTER).getFailedShards();
                                        // Set the local cluster status (including the final driver) to partial if the query was stopped
                                        // or encountered resolution or execution failures.
                                        var status = localClusterWasInterrupted.get()
                                            || (failedShards != null && failedShards > 0)
                                            || v.getFailures().isEmpty() == false
                                                ? EsqlExecutionInfo.Cluster.Status.PARTIAL
                                                : EsqlExecutionInfo.Cluster.Status.SUCCESSFUL;
                                        builder.setStatus(status);
                                    }
                                    return builder.build();
                                });
                            }
                            l.onResponse(completionInfo);
                        })
                    )
                ) {
                    runCompute(
                        rootTask,
                        new ComputeContext(
                            sessionId,
                            profileDescription(profileQualifier, "final"),
                            LOCAL_CLUSTER,
                            flags,
                            FunctionList.empty(),
                            configuration,
                            foldContext,
                            exchangeSource::createExchangeSource,
                            exchangeSinkSupplier
                        ),
                        coordinatorPlan,
                        projectAfterTopN,
                        localListener.acquireCompute()
                    );
                    // starts computes on data nodes on the main cluster
                    if (localConcreteIndices != null && localConcreteIndices.indices().length > 0) {
                        final var dataNodesListener = localListener.acquireCompute();
                        dataNodeComputeHandler.startComputeOnDataNodes(
                            sessionId,
                            LOCAL_CLUSTER,
                            rootTask,
                            flags,
                            configuration,
                            dataNodePlan,
                            Set.of(localConcreteIndices.indices()),
                            localOriginalIndices,
                            exchangeSource,
                            cancelQueryOnFailure,
                            ActionListener.wrap(r -> {
                                localClusterWasInterrupted.set(execInfo.isStopped());
                                execInfo.swapCluster(
                                    LOCAL_CLUSTER,
                                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(r.getTotalShards())
                                        .setSuccessfulShards(r.getSuccessfulShards())
                                        .setSkippedShards(r.getSkippedShards())
                                        .setFailedShards(r.getFailedShards())
                                        .addFailures(r.failures)
                                        .build()
                                );
                                dataNodesListener.onResponse(r.getCompletionInfo());
                            }, e -> {
                                if (configuration.allowPartialResults() && EsqlCCSUtils.canAllowPartial(e)) {
                                    execInfo.swapCluster(
                                        LOCAL_CLUSTER,
                                        (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(
                                            EsqlExecutionInfo.Cluster.Status.PARTIAL
                                        ).addFailures(List.of(new ShardSearchFailure(e))).build()
                                    );
                                    dataNodesListener.onResponse(DriverCompletionInfo.EMPTY);
                                } else {
                                    dataNodesListener.onFailure(e);
                                }
                            })
                        );
                    }
                }
                // starts computes on remote clusters
                final var remoteClusters = clusterComputeHandler.getRemoteClusters(clusterToConcreteIndices, clusterToOriginalIndices);
                for (ClusterComputeHandler.RemoteCluster cluster : remoteClusters) {
                    if (execInfo.getCluster(cluster.clusterAlias()).getStatus() != EsqlExecutionInfo.Cluster.Status.RUNNING) {
                        // if the cluster is already in the terminal state from the planning stage, no need to call it
                        continue;
                    }
                    clusterComputeHandler.startComputeOnRemoteCluster(
                        sessionId,
                        rootTask,
                        configuration,
                        dataNodePlan,
                        exchangeSource,
                        cluster,
                        cancelQueryOnFailure,
                        execInfo,
                        computeListener.acquireCompute().delegateResponse((l, ex) -> {
                            /*
                             * At various points, when collecting failures before sending a response, we manually check
                             * if an ex is a transport error and if it is, we unwrap it. Because we're wrapping an ex
                             * in RemoteException, the checks fail and unwrapping does not happen. We offload the
                             * unwrapping to here.
                             *
                             * Note: The other error we explicitly check for is TaskCancelledException which is never
                             * wrapped.
                             */
                            if (ex instanceof TransportException te) {
                                l.onFailure(new RemoteException(cluster.clusterAlias(), FailureCollector.unwrapTransportException(te)));
                            } else {
                                l.onFailure(new RemoteException(cluster.clusterAlias(), ex));
                            }
                        })
                    );
                }
            }
        }
    }

    // For queries like: FROM logs* | LIMIT 0 (including cross-cluster LIMIT 0 queries)
    private static void updateShardCountForCoordinatorOnlyQuery(EsqlExecutionInfo execInfo) {
        if (execInfo.isCrossClusterSearch()) {
            for (String clusterAlias : execInfo.clusterAliases()) {
                execInfo.swapCluster(
                    clusterAlias,
                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(0)
                        .setSuccessfulShards(0)
                        .setSkippedShards(0)
                        .setFailedShards(0)
                        .build()
                );
            }
        }
    }

    // For queries like: FROM logs* | LIMIT 0 (including cross-cluster LIMIT 0 queries)
    private static void updateExecutionInfoAfterCoordinatorOnlyQuery(EsqlExecutionInfo execInfo) {
        execInfo.markEndQuery();  // TODO: revisit this time recording model as part of INLINESTATS improvements
        if (execInfo.isCrossClusterSearch()) {
            assert execInfo.planningTookTime() != null : "Planning took time should be set on EsqlExecutionInfo but is null";
            for (String clusterAlias : execInfo.clusterAliases()) {
                execInfo.swapCluster(clusterAlias, (k, v) -> {
                    var builder = new EsqlExecutionInfo.Cluster.Builder(v).setTook(execInfo.overallTook());
                    if (v.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                        builder.setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                    }
                    return builder.build();
                });
            }
        }
    }

    /**
     * If all of target shards excluding the skipped shards failed from the local or remote clusters, then we should fail the entire query
     * regardless of the partial_results configuration or skip_unavailable setting. This behavior doesn't fully align with the search API,
     * which doesn't consider the failures from the remote clusters when skip_unavailable is true.
     */
    static void failIfAllShardsFailed(EsqlExecutionInfo execInfo, List<Page> finalResults) {
        // do not fail if any final result has results
        if (finalResults.stream().anyMatch(p -> p.getPositionCount() > 0)) {
            return;
        }
        int totalFailedShards = 0;
        for (EsqlExecutionInfo.Cluster cluster : execInfo.clusterInfo.values()) {
            final Integer successfulShards = cluster.getSuccessfulShards();
            if (successfulShards != null && successfulShards > 0) {
                return;
            }
            if (cluster.getFailedShards() != null) {
                totalFailedShards += cluster.getFailedShards();
            }
        }
        if (totalFailedShards == 0) {
            return;
        }
        final var failureCollector = new FailureCollector();
        for (EsqlExecutionInfo.Cluster cluster : execInfo.clusterInfo.values()) {
            var failedShards = cluster.getFailedShards();
            if (failedShards != null && failedShards > 0) {
                assert cluster.getFailures().isEmpty() == false : "expected failures for cluster [" + cluster.getClusterAlias() + "]";
                for (ShardSearchFailure failure : cluster.getFailures()) {
                    if (failure.getCause() instanceof Exception e) {
                        failureCollector.unwrapAndCollect(e);
                    } else {
                        assert false : "unexpected failure: " + new AssertionError(failure.getCause());
                        failureCollector.unwrapAndCollect(failure);
                    }
                }
            }
        }
        ExceptionsHelper.reThrowIfNotNull(failureCollector.getFailure());
    }

    void runCompute(
        CancellableTask task,
        ComputeContext context,
        PhysicalPlan plan,
        ProjectAfterTopN projectAfterTopN,
        ActionListener<DriverCompletionInfo> listener
    ) {
        var shardContexts = context.searchContexts().map(ComputeSearchContext::shardContext);
        EsPhysicalOperationProviders physicalOperationProviders = new EsPhysicalOperationProviders(
            context.foldCtx(),
            shardContexts,
            searchService.getIndicesService().getAnalysis(),
            physicalSettings
        );

        try {
            LocalExecutionPlanner planner = new LocalExecutionPlanner(
                context.sessionId(),
                context.clusterAlias(),
                task,
                bigArrays,
                blockFactory,
                clusterService.getSettings(),
                context.configuration(),
                context.exchangeSourceSupplier(),
                context.exchangeSinkSupplier(),
                enrichLookupService,
                lookupFromIndexService,
                inferenceService,
                physicalOperationProviders,
                shardContexts
            );

            LOGGER.debug("Received physical plan for {}:\n{}", context.description(), plan);

            var localPlan = PlannerUtils.localPlan(
                context.flags(),
                context.searchExecutionContexts(),
                context.configuration(),
                context.foldCtx(),
                projectAfterTopN,
                plan
            );
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Local plan for {}:\n{}", context.description(), localPlan);
            }
            // the planner will also set the driver parallelism in LocalExecutionPlanner.LocalExecutionPlan (used down below)
            // it's doing this in the planning of EsQueryExec (the source of the data)
            // see also EsPhysicalOperationProviders.sourcePhysicalOperation
            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(
                context.description(),
                context.foldCtx(),
                localPlan,
                context.description().equals(REDUCE_DESCRIPTION) ? DriverContext.Phase.REDUCE : DriverContext.Phase.OTHER
            );
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Local execution plan for {}:\n{}", context.description(), localExecutionPlan.describe());
            }
            var drivers = localExecutionPlan.createDrivers(context.sessionId());
            // After creating the drivers (and therefore, the operators), we can safely decrement the reference count since the reader
            // operators will hold a reference to the contexts where relevant. However, we should only do this for the data computations,
            // because in other cases the drivers won't increment the reference count of the contexts (no readers).
            if (context.description().equals(DATA_DESCRIPTION)) {
                shardContexts.forEach(RefCounted::decRef);
            }
            if (drivers.isEmpty()) {
                throw new IllegalStateException("no drivers created");
            }
            LOGGER.debug("using {} drivers", drivers.size());
            ActionListener<Void> driverListener = listener.map(ignored -> {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "finished {}",
                        DriverCompletionInfo.includingProfiles(
                            drivers,
                            context.description(),
                            clusterService.getClusterName().value(),
                            transportService.getLocalNode().getName(),
                            localPlan.toString()
                        )
                    );
                }
                if (context.configuration().profile()) {
                    return DriverCompletionInfo.includingProfiles(
                        drivers,
                        context.description(),
                        clusterService.getClusterName().value(),
                        transportService.getLocalNode().getName(),
                        localPlan.toString()
                    );
                } else {
                    return DriverCompletionInfo.excludingProfiles(drivers);
                }
            });
            driverRunner.executeDrivers(
                task,
                drivers,
                transportService.getThreadPool().executor(ESQL_WORKER_THREAD_POOL_NAME),
                ActionListener.releaseAfter(driverListener, () -> Releasables.close(drivers))
            );
        } catch (Exception e) {
            Releasables.close(context.searchContexts());
            LOGGER.fatal("Error in ComputeService.runCompute for : " + context.description());
            listener.onFailure(e);
        }
    }

    static PhysicalPlan reductionPlan(
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        ExchangeSinkExec plan,
        ReductionPlanFeatures features
    ) {
        PhysicalPlan source = new ExchangeSourceExec(plan.source(), plan.output(), plan.isIntermediateAgg());
        if (features == ReductionPlanFeatures.DISABLED) {
            return plan.replaceChild(source);
        }

        PhysicalPlan newPlan = switch (PlannerUtils.reductionPlan(plan)) {
            case PlannerUtils.SimplePlanReduction.NO_REDUCTION -> source;
            case PlannerUtils.SimplePlanReduction.TOP_N ->
                // In the case of TopN, the source output type is replaced since we're pulling the FieldExtractExec to the reduction node,
                // so essential we are splitting the TopNExec into two parts, similar to other aggregations, but unlike other aggregations,
                // we also need the original plan, since we add the project in the reduction node.
                fixTopNSource(flags, configuration, foldCtx, plan).filter(unused -> features == ReductionPlanFeatures.ALL)
                    .orElseGet(() -> plan.replaceChildren(List.of(source)));
            case PlannerUtils.ReducedPlan rp -> rp.plan().replaceChildren(List.of(source));
        };
        return plan.replaceChild(newPlan);
    }

    enum ReductionPlanFeatures {
        ALL,
        WITHOUT_TOP_N,
        DISABLED
    }

    /** Returns {@code Optional.empty()} if the plan is not a TopN source, or if it references multiple indices. */
    private static Optional<PhysicalPlan> fixTopNSource(
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        ExchangeSinkExec plan
    ) {
        FragmentExec fragment = plan.child() instanceof FragmentExec fe ? fe : null;
        if (fragment == null) {
            return Optional.empty();
        }
        if (isTopNCompatible(fragment) == false) {
            // If the fragment references multiple indices, we don't need to do anything special.
            return Optional.empty();
        }
        var fakeSearchStats = new SearchStatsHacks();
        final var logicalOptimizer = new LocalLogicalPlanOptimizer(
            new LocalLogicalOptimizerContext(configuration, foldCtx, fakeSearchStats)
        );
        ProjectAfterTopN projectAfterTopN = ProjectAfterTopN.KEEP;
        var physicalOptimizer = new LocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(flags, configuration, foldCtx, fakeSearchStats, projectAfterTopN)
        ) {
            @Override
            protected List<Batch<PhysicalPlan>> batches() {
                return LocalPhysicalPlanOptimizer.rules(false /* optimizeForEsSource */, projectAfterTopN);
            }
        };
        var localPlan = PlannerUtils.localPlan(plan, logicalOptimizer, physicalOptimizer);
        PhysicalPlan result = localPlan.transformUp(TopNExec.class, topN -> {
            var child = topN.child();
            return topN.replaceChild(new ExchangeSourceExec(topN.source(), child.output(), false /* isIntermediateAgg */));
        });
        return Optional.of(((UnaryExec) result).child());
    }

    /**
     * Since we can't predict the actual optimized plan without the proper index stats, which we don't have during the reduce planning
     * phase, we choose (for now) to take the easy route of just turning off this optimization for multi-index queries. A similar check
     * is done during the planning in {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.RemoveProjectAfterTopN}.
     */
    private static boolean isTopNCompatible(FragmentExec fragmentExec) {
        LogicalPlan fragment = fragmentExec.fragment();
        // FIXME(gal, NOCOMMIT) Document enrich as well
        return fragment.anyMatch(p -> p instanceof EsRelation relation && relation.indexNameWithModes().size() > 1) == false
            && fragment.anyMatch(p -> p instanceof Enrich) == false;
    }

    // A hack to avoid the ReplaceFieldWithConstantOrNull optimization, since we don't have search stats during the reduce planning phase.
    private static class SearchStatsHacks implements SearchStats {
        @Override
        public boolean exists(FieldAttribute.FieldName field) {
            return true;
        }

        @Override
        public boolean isIndexed(FieldAttribute.FieldName field) {
            return false;
        }

        @Override
        public boolean hasDocValues(FieldAttribute.FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasExactSubfield(FieldAttribute.FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(FieldAttribute.FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(FieldAttribute.FieldName field, BytesRef value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object min(FieldAttribute.FieldName field) {
            return null;
        }

        @Override
        public Object max(FieldAttribute.FieldName field) {
            return null;
        }

        @Override
        public boolean isSingleValue(FieldAttribute.FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canUseEqualityOnSyntheticSourceDelegate(FieldAttribute.FieldName name, String value) {
            throw new UnsupportedOperationException();
        }
    }

    String newChildSession(String session) {
        return session + "/" + childSessionIdGenerator.incrementAndGet();
    }

    String profileDescription(String qualifier, String label) {
        return qualifier == null ? label : qualifier + "." + label;
    }

    Runnable cancelQueryOnFailure(CancellableTask task) {
        return new RunOnce(() -> {
            LOGGER.debug("cancelling ESQL task {} on failure", task);
            transportService.getTaskManager().cancelTaskAndDescendants(task, "cancelled on failure", false, ActionListener.noop());
        });
    }

    CancellableTask createGroupTask(Task parentTask, Supplier<String> description) throws TaskCancelledException {
        final TaskManager taskManager = transportService.getTaskManager();
        try (var ignored = transportService.getThreadPool().getThreadContext().newTraceContext()) {
            return (CancellableTask) taskManager.register(
                "transport",
                "esql_compute_group",
                new ComputeGroupTaskRequest(parentTask.taskInfo(transportService.getLocalNode().getId(), false).taskId(), description)
            );
        }
    }

    public EsqlFlags createFlags() {
        return new EsqlFlags(clusterService.getClusterSettings());
    }

    private static class ComputeGroupTaskRequest extends AbstractTransportRequest {
        private final Supplier<String> parentDescription;

        ComputeGroupTaskRequest(TaskId parentTask, Supplier<String> description) {
            this.parentDescription = description;
            setParentTask(parentTask);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            assert parentTaskId.isSet();
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "group [" + parentDescription.get() + "]";
        }
    }
}
