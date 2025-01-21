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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

/**
 * Computes the result of a {@link PhysicalPlan}.
 */
public class ComputeService {
    public static final String DATA_ACTION_NAME = EsqlQueryAction.NAME + "/data";
    public static final String CLUSTER_ACTION_NAME = EsqlQueryAction.NAME + "/cluster";

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
        if (dataNodePlan == null) {
            if (clusterToConcreteIndices.values().stream().allMatch(v -> v.indices().length == 0) == false) {
                String error = "expected no concrete indices without data node plan; got " + clusterToConcreteIndices;
                assert false : error;
                listener.onFailure(new IllegalStateException(error));
                return;
            }
            var computeContext = new ComputeContext(
                newChildSession(sessionId),
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                List.of(),
                configuration,
                foldContext,
                null,
                null
            );
            String local = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            updateShardCountForCoordinatorOnlyQuery(execInfo);
            try (var computeListener = ComputeListener.create(local, transportService, rootTask, execInfo, listener.map(r -> {
                updateExecutionInfoAfterCoordinatorOnlyQuery(execInfo);
                return new Result(physicalPlan.output(), collectedPages, r.getProfiles(), execInfo);
            }))) {
                runCompute(rootTask, computeContext, coordinatorPlan, computeListener.acquireCompute(local));
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
        var localOriginalIndices = clusterToOriginalIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        var localConcreteIndices = clusterToConcreteIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        String local = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        /*
         * Grab the output attributes here, so we can pass them to
         * the listener without holding on to a reference to the
         * entire plan.
         */
        List<Attribute> outputAttributes = physicalPlan.output();
        try (
            // this is the top level ComputeListener called once at the end (e.g., once all clusters have finished for a CCS)
            var computeListener = ComputeListener.create(local, transportService, rootTask, execInfo, listener.map(r -> {
                execInfo.markEndQuery();  // TODO: revisit this time recording model as part of INLINESTATS improvements
                return new Result(outputAttributes, collectedPages, r.getProfiles(), execInfo);
            }))
        ) {
            var exchangeSource = new ExchangeSourceHandler(
                queryPragmas.exchangeBufferSize(),
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
                computeListener.acquireAvoid()
            );
            try (Releasable ignored = exchangeSource.addEmptySink()) {
                // run compute on the coordinator
                runCompute(
                    rootTask,
                    new ComputeContext(
                        sessionId,
                        RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                        List.of(),
                        configuration,
                        foldContext,
                        exchangeSource,
                        null
                    ),
                    coordinatorPlan,
                    computeListener.acquireCompute(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)
                );
                // starts computes on data nodes on the main cluster
                if (localConcreteIndices != null && localConcreteIndices.indices().length > 0) {
                    dataNodeComputeHandler.startComputeOnDataNodes(
                        sessionId,
                        RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                        rootTask,
                        configuration,
                        dataNodePlan,
                        Set.of(localConcreteIndices.indices()),
                        localOriginalIndices,
                        exchangeSource,
                        execInfo,
                        computeListener
                    );
                }
                // starts computes on remote clusters
                final var remoteClusters = clusterComputeHandler.getRemoteClusters(clusterToConcreteIndices, clusterToOriginalIndices);
                clusterComputeHandler.startComputeOnRemoteClusters(
                    sessionId,
                    rootTask,
                    configuration,
                    dataNodePlan,
                    exchangeSource,
                    remoteClusters,
                    computeListener
                );
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

    void runCompute(CancellableTask task, ComputeContext context, PhysicalPlan plan, ActionListener<ComputeResponse> listener) {
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
                context.exchangeSource(),
                context.exchangeSink(),
                enrichLookupService,
                lookupFromIndexService,
                new EsPhysicalOperationProviders(context.foldCtx(), contexts, searchService.getIndicesService().getAnalysis())
            );

            LOGGER.debug("Received physical plan:\n{}", plan);

            plan = PlannerUtils.localPlan(context.searchExecutionContexts(), context.configuration(), context.foldCtx(), plan);
            // the planner will also set the driver parallelism in LocalExecutionPlanner.LocalExecutionPlan (used down below)
            // it's doing this in the planning of EsQueryExec (the source of the data)
            // see also EsPhysicalOperationProviders.sourcePhysicalOperation
            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(context.foldCtx(), plan);
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
                return new ComputeResponse(drivers.stream().map(Driver::profile).toList());
            } else {
                final ComputeResponse response = new ComputeResponse(List.of());
                return response;
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
}
