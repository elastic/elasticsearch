/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

/**
 * Computes the result of a {@link PhysicalPlan}.
 */
public class ComputeService {
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
    private final ClusterService clusterService;
    private final AtomicLong childSessionIdGenerator = new AtomicLong();
    private final DataNodeComputeHandler dataNodeComputeHandler;
    private final ClusterComputeHandler clusterComputeHandler;
    private final ExchangeService exchangeService;

    private volatile DataPartitioning defaultDataPartitioning;

    @SuppressWarnings("this-escape")
    public ComputeService(
        SearchService searchService,
        TransportService transportService,
        ExchangeService exchangeService,
        EnrichLookupService enrichLookupService,
        LookupFromIndexService lookupFromIndexService,
        ClusterService clusterService,
        ThreadPool threadPool,
        BigArrays bigArrays,
        BlockFactory blockFactory
    ) {
        this.searchService = searchService;
        this.transportService = transportService;
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.blockFactory = blockFactory;
        var esqlExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.driverRunner = new DriverTaskRunner(transportService, esqlExecutor);
        this.enrichLookupService = enrichLookupService;
        this.lookupFromIndexService = lookupFromIndexService;
        this.clusterService = clusterService;
        this.dataNodeComputeHandler = new DataNodeComputeHandler(
            this,
            clusterService,
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
        this.exchangeService = exchangeService;
        clusterService.getClusterSettings().initializeAndWatch(EsqlPlugin.DEFAULT_DATA_PARTITIONING, v -> this.defaultDataPartitioning = v);
    }

    public void execute(
        String sessionId,
        CancellableTask rootTask,
        PhysicalPlan physicalPlan,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        ActionListener<Result> listener
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
        PhysicalPlan coordinatorPlan = new OutputExec(coordinatorAndDataNodePlan.v1(), collectedPages::add);
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
                "single",
                LOCAL_CLUSTER,
                List.of(),
                configuration,
                foldContext,
                null,
                null
            );
            updateShardCountForCoordinatorOnlyQuery(execInfo);
            try (
                var computeListener = new ComputeListener(transportService.getThreadPool(), cancelQueryOnFailure, listener.map(profiles -> {
                    updateExecutionInfoAfterCoordinatorOnlyQuery(execInfo);
                    return new Result(physicalPlan.output(), collectedPages, profiles, execInfo);
                }))
            ) {
                runCompute(rootTask, computeContext, coordinatorPlan, computeListener.acquireCompute());
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
        try (var computeListener = new ComputeListener(transportService.getThreadPool(), cancelQueryOnFailure, listener.map(profiles -> {
            execInfo.markEndQuery();  // TODO: revisit this time recording model as part of INLINESTATS improvements
            return new Result(outputAttributes, collectedPages, profiles, execInfo);
        }))) {
            try (Releasable ignored = exchangeSource.addEmptySink()) {
                // run compute on the coordinator
                final AtomicBoolean localClusterWasInterrupted = new AtomicBoolean();
                try (
                    var localListener = new ComputeListener(
                        transportService.getThreadPool(),
                        cancelQueryOnFailure,
                        computeListener.acquireCompute().delegateFailure((l, profiles) -> {
                            if (execInfo.clusterInfo.containsKey(LOCAL_CLUSTER)) {
                                execInfo.swapCluster(LOCAL_CLUSTER, (k, v) -> {
                                    var tookTime = TimeValue.timeValueNanos(System.nanoTime() - execInfo.getRelativeStartNanos());
                                    var builder = new EsqlExecutionInfo.Cluster.Builder(v).setTook(tookTime);
                                    if (v.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                                        final Integer failedShards = execInfo.getCluster(LOCAL_CLUSTER).getFailedShards();
                                        var status = localClusterWasInterrupted.get() || (failedShards != null && failedShards > 0)
                                            ? EsqlExecutionInfo.Cluster.Status.PARTIAL
                                            : EsqlExecutionInfo.Cluster.Status.SUCCESSFUL;
                                        builder.setStatus(status);
                                    }
                                    return builder.build();
                                });
                            }
                            l.onResponse(profiles);
                        })
                    )
                ) {
                    runCompute(
                        rootTask,
                        new ComputeContext(
                            sessionId,
                            "final",
                            LOCAL_CLUSTER,
                            List.of(),
                            configuration,
                            foldContext,
                            exchangeSource::createExchangeSource,
                            null
                        ),
                        coordinatorPlan,
                        localListener.acquireCompute()
                    );
                    // starts computes on data nodes on the main cluster
                    if (localConcreteIndices != null && localConcreteIndices.indices().length > 0) {
                        final var dataNodesListener = localListener.acquireCompute();
                        dataNodeComputeHandler.startComputeOnDataNodes(
                            sessionId,
                            LOCAL_CLUSTER,
                            rootTask,
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
                                        .setFailures(r.failures)
                                        .build()
                                );
                                dataNodesListener.onResponse(r.getProfiles());
                            }, e -> {
                                if (configuration.allowPartialResults() && EsqlCCSUtils.canAllowPartial(e)) {
                                    execInfo.swapCluster(
                                        LOCAL_CLUSTER,
                                        (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(
                                            EsqlExecutionInfo.Cluster.Status.PARTIAL
                                        ).setFailures(List.of(new ShardSearchFailure(e))).build()
                                    );
                                    dataNodesListener.onResponse(List.of());
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

    void runCompute(CancellableTask task, ComputeContext context, PhysicalPlan plan, ActionListener<List<DriverProfile>> listener) {
        listener = ActionListener.runBefore(listener, () -> Releasables.close(context.searchContexts()));
        List<EsPhysicalOperationProviders.ShardContext> contexts = new ArrayList<>(context.searchContexts().size());
        for (int i = 0; i < context.searchContexts().size(); i++) {
            SearchContext searchContext = context.searchContexts().get(i);
            var searchExecutionContext = new SearchExecutionContext(searchContext.getSearchExecutionContext()) {

                @Override
                public SourceProvider createSourceProvider() {
                    final Supplier<SourceProvider> supplier = () -> super.createSourceProvider();
                    return new ReinitializingSourceProvider(supplier);

                }
            };
            contexts.add(
                new EsPhysicalOperationProviders.DefaultShardContext(i, searchExecutionContext, searchContext.request().getAliasFilter())
            );
        }
        EsPhysicalOperationProviders physicalOperationProviders = new EsPhysicalOperationProviders(
            context.foldCtx(),
            contexts,
            searchService.getIndicesService().getAnalysis(),
            defaultDataPartitioning
        );
        final List<Driver> drivers;
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
                physicalOperationProviders,
                contexts
            );

            LOGGER.debug("Received physical plan:\n{}", plan);

            plan = PlannerUtils.localPlan(context.searchExecutionContexts(), context.configuration(), context.foldCtx(), plan);
            // the planner will also set the driver parallelism in LocalExecutionPlanner.LocalExecutionPlan (used down below)
            // it's doing this in the planning of EsQueryExec (the source of the data)
            // see also EsPhysicalOperationProviders.sourcePhysicalOperation
            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(context.taskDescription(), context.foldCtx(), plan);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Local execution plan:\n{}", localExecutionPlan.describe());
            }
            drivers = localExecutionPlan.createDrivers(context.sessionId());
            if (drivers.isEmpty()) {
                throw new IllegalStateException("no drivers created");
            }
            LOGGER.debug("using {} drivers", drivers.size());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        ActionListener<Void> listenerCollectingStatus = listener.map(ignored -> {
            if (context.configuration().profile()) {
                return drivers.stream().map(Driver::profile).toList();
            } else {
                return List.of();
            }
        });
        listenerCollectingStatus = ActionListener.releaseAfter(listenerCollectingStatus, () -> Releasables.close(drivers));
        driverRunner.executeDrivers(
            task,
            drivers,
            transportService.getThreadPool().executor(ESQL_WORKER_THREAD_POOL_NAME),
            listenerCollectingStatus
        );
    }

    static PhysicalPlan reductionPlan(ExchangeSinkExec plan, boolean enable) {
        PhysicalPlan reducePlan = new ExchangeSourceExec(plan.source(), plan.output(), plan.isIntermediateAgg());
        if (enable) {
            PhysicalPlan p = PlannerUtils.reductionPlan(plan);
            if (p != null) {
                reducePlan = p.replaceChildren(List.of(reducePlan));
            }
        }
        return new ExchangeSinkExec(plan.source(), plan.output(), plan.isIntermediateAgg(), reducePlan);
    }

    String newChildSession(String session) {
        return session + "/" + childSessionIdGenerator.incrementAndGet();
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

    private static class ComputeGroupTaskRequest extends TransportRequest {
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
