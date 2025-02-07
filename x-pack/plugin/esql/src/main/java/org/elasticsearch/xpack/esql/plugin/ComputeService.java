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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverTaskRunner;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
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
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
        this.dataNodeComputeHandler = new DataNodeComputeHandler(this, searchService, transportService, exchangeService, esqlExecutor);
        this.clusterComputeHandler = new ClusterComputeHandler(
            this,
            exchangeService,
            transportService,
            esqlExecutor,
            dataNodeComputeHandler
        );
        this.exchangeService = exchangeService;
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
        try (var computeListener = new ComputeListener(transportService.getThreadPool(), cancelQueryOnFailure, listener.map(profiles -> {
            execInfo.markEndQuery();  // TODO: revisit this time recording model as part of INLINESTATS improvements
            return new Result(outputAttributes, collectedPages, profiles, execInfo);
        }))) {
            var exchangeSource = new ExchangeSourceHandler(
                queryPragmas.exchangeBufferSize(),
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
                ActionListener.runBefore(computeListener.acquireAvoid(), () -> exchangeService.removeExchangeSourceHandler(sessionId))
            );
            exchangeService.addExchangeSourceHandler(sessionId, exchangeSource);
            try (Releasable ignored = exchangeSource.addEmptySink()) {
                // run compute on the coordinator
                final AtomicBoolean localClusterWasInterrupted = new AtomicBoolean();
                try (
                    var localListener = new ComputeListener(
                        transportService.getThreadPool(),
                        cancelQueryOnFailure,
                        computeListener.acquireCompute().delegateFailure((l, profiles) -> {
                            if (execInfo.isCrossClusterSearch() && execInfo.clusterAliases().contains(LOCAL_CLUSTER)) {
                                var tookTime = TimeValue.timeValueNanos(System.nanoTime() - execInfo.getRelativeStartNanos());
                                var status = localClusterWasInterrupted.get()
                                    ? EsqlExecutionInfo.Cluster.Status.PARTIAL
                                    : EsqlExecutionInfo.Cluster.Status.SUCCESSFUL;
                                execInfo.swapCluster(
                                    LOCAL_CLUSTER,
                                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(status).setTook(tookTime).build()
                                );
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
                            localListener.acquireCompute().map(r -> {
                                localClusterWasInterrupted.set(execInfo.isPartial());
                                if (execInfo.isCrossClusterSearch() && execInfo.clusterAliases().contains(LOCAL_CLUSTER)) {
                                    execInfo.swapCluster(
                                        LOCAL_CLUSTER,
                                        (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(r.getTotalShards())
                                            .setSuccessfulShards(r.getSuccessfulShards())
                                            .setSkippedShards(r.getSkippedShards())
                                            .setFailedShards(r.getFailedShards())
                                            .build()
                                    );
                                }
                                return r.getProfiles();
                            })
                        );
                    }
                }
                // starts computes on remote clusters
                final var remoteClusters = clusterComputeHandler.getRemoteClusters(clusterToConcreteIndices, clusterToOriginalIndices);
                for (ClusterComputeHandler.RemoteCluster cluster : remoteClusters) {
                    clusterComputeHandler.startComputeOnRemoteCluster(
                        sessionId,
                        rootTask,
                        configuration,
                        dataNodePlan,
                        exchangeSource,
                        cluster,
                        cancelQueryOnFailure,
                        execInfo,
                        computeListener.acquireCompute().map(r -> {
                            updateExecutionInfo(execInfo, cluster.clusterAlias(), r);
                            return r.getProfiles();
                        })
                    );
                }
            }
        }
    }

    private void updateExecutionInfo(EsqlExecutionInfo executionInfo, String clusterAlias, ComputeResponse resp) {
        Function<EsqlExecutionInfo.Cluster.Status, EsqlExecutionInfo.Cluster.Status> runningToSuccess = status -> {
            if (status == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                return executionInfo.isPartial() ? EsqlExecutionInfo.Cluster.Status.PARTIAL : EsqlExecutionInfo.Cluster.Status.SUCCESSFUL;
            } else {
                return status;
            }
        };
        if (resp.getTook() != null) {
            var tookTime = TimeValue.timeValueNanos(executionInfo.planningTookTime().nanos() + resp.getTook().nanos());
            executionInfo.swapCluster(
                clusterAlias,
                (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(runningToSuccess.apply(v.getStatus()))
                    .setTook(tookTime)
                    .setTotalShards(resp.getTotalShards())
                    .setSuccessfulShards(resp.getSuccessfulShards())
                    .setSkippedShards(resp.getSkippedShards())
                    .setFailedShards(resp.getFailedShards())
                    .build()
            );
        } else {
            // if the cluster is an older version and does not send back took time, then calculate it here on the coordinator
            // and leave shard info unset, so it is not shown in the CCS metadata section of the JSON response
            executionInfo.swapCluster(
                clusterAlias,
                (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(runningToSuccess.apply(v.getStatus()))
                    .setTook(executionInfo.tookSoFar())
                    .build()
            );
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
                new EsPhysicalOperationProviders(context.foldCtx(), contexts, searchService.getIndicesService().getAnalysis()),
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
}
