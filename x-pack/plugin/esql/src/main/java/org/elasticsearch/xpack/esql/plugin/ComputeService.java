/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.SearchExecutionContext;
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
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS;
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
    private final PlannerSettings.Holder plannerSettings;

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
        this.plannerSettings = transportActionServices.plannerSettings();
    }

    PlannerSettings.Holder plannerSettings() {
        return plannerSettings;
    }

    public void execute(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        PhysicalPlan physicalPlan,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(
            EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
            ThreadPool.Names.SYSTEM_READ,
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION
        );
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(physicalPlan);

        List<PhysicalPlan> subplans = subplansAndMainPlan.v1();

        // take a snapshot of the initial cluster statuses, this is the status after index resolutions,
        // and it will be checked before executing data node plan on remote clusters
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses = new HashMap<>(execInfo.clusterInfo.size());
        for (Map.Entry<String, EsqlExecutionInfo.Cluster> entry : execInfo.clusterInfo.entrySet()) {
            initialClusterStatuses.put(entry.getKey(), entry.getValue().getStatus());
        }

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
                listener,
                null,
                initialClusterStatuses,
                planTimeProfile
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
                EmptyIndexedByShardId.instance(),
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
                        return new Result(mainPlan.output(), collectedPages, configuration, profiles, execInfo);
                    })
                )
            ) {
                runCompute(rootTask, computeContext, mainPlan, plannerSettings.get(), planTimeProfile, localListener.acquireCompute());

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
                        ActionListener.wrap(result -> {
                            exchangeSink.addCompletionListener(
                                ActionListener.running(() -> { exchangeService.finishSinkHandler(childSessionId, null); })
                            );
                            subPlanListener.onResponse(result.completionInfo());
                        }, e -> {
                            exchangeService.finishSinkHandler(childSessionId, e);
                            subPlanListener.onFailure(e);
                        }),
                        () -> exchangeSink.createExchangeSink(() -> {}),
                        initialClusterStatuses,
                        configuration.profile() ? new PlanTimeProfile() : null
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
        ActionListener<Result> listener,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses,
        PlanTimeProfile planTimeProfile
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
        Map<String, OriginalIndices> clusterToConcreteIndices = getIndices(physicalPlan, EsRelation::concreteIndices);
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
                EmptyIndexedByShardId.instance(),
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
                        return new Result(physicalPlan.output(), collectedPages, configuration, completionInfo, execInfo);
                    })
                )
            ) {
                runCompute(
                    rootTask,
                    computeContext,
                    coordinatorPlan,
                    plannerSettings.get(),
                    planTimeProfile,
                    computeListener.acquireCompute()
                );
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
        Map<String, OriginalIndices> clusterToOriginalIndices = getIndices(physicalPlan, EsRelation::originalIndices);
        var localOriginalIndices = clusterToOriginalIndices.remove(LOCAL_CLUSTER);
        var localConcreteIndices = clusterToConcreteIndices.remove(LOCAL_CLUSTER);
        /*
         * Grab the output attributes here, so we can pass them to
         * the listener without holding on to a reference to the
         * entire plan.
         */
        List<Attribute> outputAttributes = physicalPlan.output();
        var exchangeSource = new ExchangeSourceHandler(
            configuration.pragmas().exchangeBufferSize(),
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
                    execInfo.markEndQuery();
                    l.onResponse(new Result(outputAttributes, collectedPages, configuration, completionInfo, execInfo));
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
                                    var tookTime = execInfo.queryProfile().total().timeSinceStarted();
                                    var builder = new EsqlExecutionInfo.Cluster.Builder(v).setTook(tookTime);
                                    if (execInfo.isMainPlan() && v.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
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
                            EmptyIndexedByShardId.instance(),
                            configuration,
                            foldContext,
                            exchangeSource::createExchangeSource,
                            exchangeSinkSupplier
                        ),
                        coordinatorPlan,
                        plannerSettings.get(),
                        planTimeProfile,
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
                    String clusterAlias = cluster.clusterAlias();
                    // Check the initial cluster status set by planning phase before executing the data node plan on remote clusters,
                    // only if this is a fork or subquery branch (exchangeSinkSupplier is not null).
                    EsqlExecutionInfo.Cluster.Status clusterStatus = exchangeSinkSupplier != null
                        ? initialClusterStatuses.get(clusterAlias)
                        : execInfo.getCluster(clusterAlias).getStatus();
                    if (clusterStatus != EsqlExecutionInfo.Cluster.Status.RUNNING) {
                        // if the cluster is already in the terminal state from the planning stage, no need to call it
                        // the initial cluster status is collected before the query is executed
                        LOGGER.trace(
                            "skipping execution on remote cluster [{}] since its initial status is [{}]",
                            clusterAlias,
                            clusterStatus
                        );
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
        if (execInfo.isCrossClusterSearch() || execInfo.includeExecutionMetadata() == ALWAYS) {
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
        execInfo.markEndQuery();
        if ((execInfo.isCrossClusterSearch() || execInfo.includeExecutionMetadata() == ALWAYS) && execInfo.isMainPlan()) {
            assert execInfo.queryProfile().planning().timeTook() != null
                : "Planning took time should be set on EsqlExecutionInfo but is null";
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
        PlannerSettings plannerSettings,
        PlanTimeProfile planTimeProfile,
        ActionListener<DriverCompletionInfo> listener
    ) {
        var shardContexts = context.searchContexts().map(ComputeSearchContext::shardContext);
        EsPhysicalOperationProviders physicalOperationProviders = new EsPhysicalOperationProviders(
            context.foldCtx(),
            shardContexts,
            searchService.getIndicesService().getAnalysis(),
            plannerSettings
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
                physicalOperationProviders
            );

            LOGGER.debug("Received physical plan for {}:\n{}", context.description(), plan);

            List<SearchExecutionContext> localContexts = new ArrayList<>();
            context.searchExecutionContexts().iterable().forEach(localContexts::add);
            var localPlan = PlannerUtils.localPlan(
                plannerSettings,
                context.flags(),
                localContexts,
                context.configuration(),
                context.foldCtx(),
                plan,
                planTimeProfile
            );
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Local plan for {}:\n{}", context.description(), localPlan);
            }
            // the planner will also set the driver parallelism in LocalExecutionPlanner.LocalExecutionPlan (used down below)
            // it's doing this in the planning of EsQueryExec (the source of the data)
            // see also EsPhysicalOperationProviders.sourcePhysicalOperation
            var localExecutionPlan = planner.plan(context.description(), context.foldCtx(), plannerSettings, localPlan, shardContexts);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Local execution plan for {}:\n{}", context.description(), localExecutionPlan.describe());
            }
            var drivers = localExecutionPlan.createDrivers(context.sessionId());
            // Note that the drivers themselves do not hold a reference to the search contexts, but rather, these are held (and therefore
            // incremented) by the source operators, and the DocVectors. Since The contexts are pre-created with a count of 1, and then
            // incremented by the relevant source operators, after creating the *data* drivers (and therefore, the source operators), we can
            // safely decrement the reference count so only the source operators and doc vectors control when these will be released.
            // Note that only the data drivers will increment the reference count when created, hence the if below.
            if (context.description().equals(DATA_DESCRIPTION)) {
                shardContexts.iterable().forEach(RefCounted::decRef);
            }
            if (drivers.isEmpty()) {
                throw new IllegalStateException("no drivers created");
            }
            LOGGER.debug("using {} drivers", drivers.size());
            ActionListener<Void> driverListener = addCompletionInfo(listener, drivers, context, localPlan, planTimeProfile);
            driverRunner.executeDrivers(
                task,
                drivers,
                transportService.getThreadPool().executor(ESQL_WORKER_THREAD_POOL_NAME),
                ActionListener.releaseAfter(driverListener, () -> Releasables.close(drivers))
            );
        } catch (Exception e) {
            Releasables.close(context.searchContexts().iterable());
            LOGGER.debug("Error in ComputeService.runCompute for : " + context.description());
            listener.onFailure(e);
        }
    }

    ActionListener<Void> addCompletionInfo(
        ActionListener<DriverCompletionInfo> listener,
        List<Driver> drivers,
        ComputeContext context,
        PhysicalPlan localPlan,
        PlanTimeProfile planTimeProfile
    ) {
        /*
         * We *really* don't want to close over the localPlan because it can
         * be quite large, and it isn't tracked.
         */
        boolean needPlanString = LOGGER.isDebugEnabled() || context.configuration().profile();
        String planString = needPlanString ? localPlan.toString() : null;
        return listener.map(ignored -> {
            if (LOGGER.isDebugEnabled() || context.configuration().profile()) {
                DriverCompletionInfo driverCompletionInfo = DriverCompletionInfo.includingProfiles(
                    drivers,
                    context.description(),
                    clusterService.getClusterName().value(),
                    transportService.getLocalNode().getName(),
                    planString,
                    planTimeProfile
                );
                LOGGER.debug("finished {}", driverCompletionInfo);
                if (context.configuration().profile()) {
                    /*
                     * planString *might* be null if we *just* set DEBUG to *after*
                     * we built the listener but before we got here. That's something
                     * we can live with.
                     */
                    return driverCompletionInfo;
                }
            }

            return DriverCompletionInfo.excludingProfiles(drivers);
        });
    }

    static ReductionPlan reductionPlan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        ExchangeSinkExec originalPlan,
        boolean runNodeLevelReduction,
        boolean reduceNodeLateMaterialization,
        PlanTimeProfile planTimeProfile
    ) {
        long startTime = planTimeProfile == null ? 0 : System.nanoTime();
        PhysicalPlan source = new ExchangeSourceExec(originalPlan.source(), originalPlan.output(), originalPlan.isIntermediateAgg());
        ReductionPlan defaultResult = new ReductionPlan(originalPlan.replaceChild(source), originalPlan);
        if (reduceNodeLateMaterialization == false && runNodeLevelReduction == false) {
            return defaultResult;
        }

        Function<PhysicalPlan, ReductionPlan> placePlanBetweenExchanges = p -> new ReductionPlan(
            originalPlan.replaceChild(p.replaceChildren(List.of(source))),
            originalPlan
        );
        // The default plan is just the exchange source piped directly into the exchange sink.
        ReductionPlan reductionPlan = switch (PlannerUtils.reductionPlan(originalPlan)) {
            case PlannerUtils.TopNReduction topN when reduceNodeLateMaterialization ->
                // In the case of TopN, the source output type is replaced since we're pulling the FieldExtractExec to the reduction node,
                // so essential we are splitting the TopNExec into two parts, similar to other aggregations, but unlike other aggregations,
                // we also need the original plan, since we add the project in the reduction node.
                LateMaterializationPlanner.planReduceDriverTopN(
                    stats -> new LocalPhysicalOptimizerContext(plannerSettings, flags, configuration, foldCtx, stats),
                    originalPlan
                )
                    // Fallback to the behavior listed below, i.e., a regular top n reduction without loading new fields.
                    .orElseGet(() -> runNodeLevelReduction ? placePlanBetweenExchanges.apply(topN.plan()) : defaultResult);
            case PlannerUtils.TopNReduction topN when runNodeLevelReduction -> placePlanBetweenExchanges.apply(topN.plan());
            case PlannerUtils.ReducedPlan rp when runNodeLevelReduction -> placePlanBetweenExchanges.apply(rp.plan());
            default -> defaultResult;
        };
        if (planTimeProfile != null) {
            planTimeProfile.addReductionPlanNanos(System.nanoTime() - startTime);
        }
        return reductionPlan;
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

    private static Map<String, OriginalIndices> getIndices(PhysicalPlan plan, Function<EsRelation, Map<String, List<String>>> getter) {
        var holder = new Holder<Map<String, OriginalIndices>>();
        PlannerUtils.forEachRelation(plan, esRelation -> {
            holder.set(Maps.transformValues(getter.apply(esRelation), v -> {
                return new OriginalIndices(v.toArray(String[]::new), SearchRequest.DEFAULT_INDICES_OPTIONS);
            }));
        });
        return holder.getOrDefault(Map::of);
    }
}
