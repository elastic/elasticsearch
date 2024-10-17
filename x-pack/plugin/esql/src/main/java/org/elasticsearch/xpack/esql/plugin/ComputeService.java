/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

/**
 * Computes the result of a {@link PhysicalPlan}.
 */
public class ComputeService {
    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);
    private final SearchService searchService;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;

    private final TransportService transportService;
    private final Executor esqlExecutor;
    private final DriverTaskRunner driverRunner;
    private final ExchangeService exchangeService;
    private final EnrichLookupService enrichLookupService;
    private final ClusterService clusterService;

    public ComputeService(
        SearchService searchService,
        TransportService transportService,
        ExchangeService exchangeService,
        EnrichLookupService enrichLookupService,
        ClusterService clusterService,
        ThreadPool threadPool,
        BigArrays bigArrays,
        BlockFactory blockFactory
    ) {
        this.searchService = searchService;
        this.transportService = transportService;
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.blockFactory = blockFactory;
        this.esqlExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        transportService.registerRequestHandler(DATA_ACTION_NAME, this.esqlExecutor, DataNodeRequest::new, new DataNodeRequestHandler());
        transportService.registerRequestHandler(
            CLUSTER_ACTION_NAME,
            this.esqlExecutor,
            ClusterComputeRequest::new,
            new ClusterRequestHandler()
        );
        this.driverRunner = new DriverTaskRunner(transportService, this.esqlExecutor);
        this.exchangeService = exchangeService;
        this.enrichLookupService = enrichLookupService;
        this.clusterService = clusterService;
    }

    public void execute(
        String sessionId,
        CancellableTask rootTask,
        PhysicalPlan physicalPlan,
        Configuration configuration,
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
                sessionId,
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                List.of(),
                configuration,
                null,
                null
            );
            String local = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
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
        final var exchangeSource = new ExchangeSourceHandler(
            queryPragmas.exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        String local = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        /*
         * Grab the output attributes here, so we can pass them to
         * the listener without holding on to a reference to the
         * entire plan.
         */
        List<Attribute> outputAttributes = physicalPlan.output();
        try (
            Releasable ignored = exchangeSource.addEmptySink();
            // this is the top level ComputeListener called once at the end (e.g., once all clusters have finished for a CCS)
            var computeListener = ComputeListener.create(local, transportService, rootTask, execInfo, listener.map(r -> {
                execInfo.markEndQuery();  // TODO: revisit this time recording model as part of INLINESTATS improvements
                return new Result(outputAttributes, collectedPages, r.getProfiles(), execInfo);
            }))
        ) {
            // run compute on the coordinator
            exchangeSource.addCompletionListener(computeListener.acquireAvoid());
            runCompute(
                rootTask,
                new ComputeContext(sessionId, RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, List.of(), configuration, exchangeSource, null),
                coordinatorPlan,
                computeListener.acquireCompute(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)
            );
            // starts computes on data nodes on the main cluster
            if (localConcreteIndices != null && localConcreteIndices.indices().length > 0) {
                startComputeOnDataNodes(
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
            startComputeOnRemoteClusters(
                sessionId,
                rootTask,
                configuration,
                dataNodePlan,
                exchangeSource,
                getRemoteClusters(clusterToConcreteIndices, clusterToOriginalIndices),
                computeListener
            );
        }
    }

    // For queries like: FROM logs* | LIMIT 0 (including cross-cluster LIMIT 0 queries)
    private static void updateExecutionInfoAfterCoordinatorOnlyQuery(EsqlExecutionInfo execInfo) {
        execInfo.markEndQuery();  // TODO: revisit this time recording model as part of INLINESTATS improvements
        if (execInfo.isCrossClusterSearch()) {
            assert execInfo.planningTookTime() != null : "Planning took time should be set on EsqlExecutionInfo but is null";
            for (String clusterAlias : execInfo.clusterAliases()) {
                // took time and shard counts for SKIPPED clusters were added at end of planning, so only update other cases here
                if (execInfo.getCluster(clusterAlias).getStatus() != EsqlExecutionInfo.Cluster.Status.SKIPPED) {
                    execInfo.swapCluster(
                        clusterAlias,
                        (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTook(execInfo.overallTook())
                            .setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)
                            .setTotalShards(0)
                            .setSuccessfulShards(0)
                            .setSkippedShards(0)
                            .setFailedShards(0)
                            .build()
                    );
                }
            }
        }
    }

    private List<RemoteCluster> getRemoteClusters(
        Map<String, OriginalIndices> clusterToConcreteIndices,
        Map<String, OriginalIndices> clusterToOriginalIndices
    ) {
        List<RemoteCluster> remoteClusters = new ArrayList<>(clusterToConcreteIndices.size());
        RemoteClusterService remoteClusterService = transportService.getRemoteClusterService();
        for (Map.Entry<String, OriginalIndices> e : clusterToConcreteIndices.entrySet()) {
            String clusterAlias = e.getKey();
            OriginalIndices concreteIndices = clusterToConcreteIndices.get(clusterAlias);
            OriginalIndices originalIndices = clusterToOriginalIndices.get(clusterAlias);
            if (originalIndices == null) {
                assert false : "can't find original indices for cluster " + clusterAlias;
                throw new IllegalStateException("can't find original indices for cluster " + clusterAlias);
            }
            if (concreteIndices.indices().length > 0) {
                Transport.Connection connection = remoteClusterService.getConnection(clusterAlias);
                remoteClusters.add(new RemoteCluster(clusterAlias, connection, concreteIndices.indices(), originalIndices));
            }
        }
        return remoteClusters;
    }

    private void startComputeOnDataNodes(
        String sessionId,
        String clusterAlias,
        CancellableTask parentTask,
        Configuration configuration,
        PhysicalPlan dataNodePlan,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        ExchangeSourceHandler exchangeSource,
        EsqlExecutionInfo executionInfo,
        ComputeListener computeListener
    ) {
        var planWithReducer = configuration.pragmas().nodeLevelReduction() == false
            ? dataNodePlan
            : dataNodePlan.transformUp(FragmentExec.class, f -> {
                PhysicalPlan reductionNode = PlannerUtils.dataNodeReductionPlan(f.fragment(), dataNodePlan);
                return reductionNode == null ? f : f.withReducer(reductionNode);
            });

        // The lambda is to say if a TEXT field has an identical exact subfield
        // We cannot use SearchContext because we don't have it yet.
        // Since it's used only for @timestamp, it is relatively safe to assume it's not needed
        // but it would be better to have a proper impl.
        QueryBuilder requestFilter = PlannerUtils.requestFilter(planWithReducer, x -> true);
        var lookupListener = ActionListener.releaseAfter(computeListener.acquireAvoid(), exchangeSource.addEmptySink());
        // SearchShards API can_match is done in lookupDataNodes
        lookupDataNodes(parentTask, clusterAlias, requestFilter, concreteIndices, originalIndices, ActionListener.wrap(dataNodeResult -> {
            try (RefCountingListener refs = new RefCountingListener(lookupListener)) {
                // update ExecutionInfo with shard counts (total and skipped)
                executionInfo.swapCluster(
                    clusterAlias,
                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(dataNodeResult.totalShards())
                        .setSuccessfulShards(dataNodeResult.totalShards())
                        .setSkippedShards(dataNodeResult.skippedShards())
                        .setFailedShards(0)
                        .build()
                );

                // For each target node, first open a remote exchange on the remote node, then link the exchange source to
                // the new remote exchange sink, and initialize the computation on the target node via data-node-request.
                for (DataNode node : dataNodeResult.dataNodes()) {
                    var queryPragmas = configuration.pragmas();
                    ExchangeService.openExchange(
                        transportService,
                        node.connection,
                        sessionId,
                        queryPragmas.exchangeBufferSize(),
                        esqlExecutor,
                        refs.acquire().delegateFailureAndWrap((l, unused) -> {
                            var remoteSink = exchangeService.newRemoteSink(parentTask, sessionId, transportService, node.connection);
                            exchangeSource.addRemoteSink(remoteSink, queryPragmas.concurrentExchangeClients());
                            ActionListener<ComputeResponse> computeResponseListener = computeListener.acquireCompute(clusterAlias);
                            var dataNodeListener = ActionListener.runBefore(computeResponseListener, () -> l.onResponse(null));
                            transportService.sendChildRequest(
                                node.connection,
                                DATA_ACTION_NAME,
                                new DataNodeRequest(
                                    sessionId,
                                    configuration,
                                    clusterAlias,
                                    node.shardIds,
                                    node.aliasFilters,
                                    planWithReducer,
                                    originalIndices.indices(),
                                    originalIndices.indicesOptions()
                                ),
                                parentTask,
                                TransportRequestOptions.EMPTY,
                                new ActionListenerResponseHandler<>(dataNodeListener, ComputeResponse::new, esqlExecutor)
                            );
                        })
                    );
                }
            }
        }, lookupListener::onFailure));
    }

    private void startComputeOnRemoteClusters(
        String sessionId,
        CancellableTask rootTask,
        Configuration configuration,
        PhysicalPlan plan,
        ExchangeSourceHandler exchangeSource,
        List<RemoteCluster> clusters,
        ComputeListener computeListener
    ) {
        var queryPragmas = configuration.pragmas();
        var linkExchangeListeners = ActionListener.releaseAfter(computeListener.acquireAvoid(), exchangeSource.addEmptySink());
        try (RefCountingListener refs = new RefCountingListener(linkExchangeListeners)) {
            for (RemoteCluster cluster : clusters) {
                ExchangeService.openExchange(
                    transportService,
                    cluster.connection,
                    sessionId,
                    queryPragmas.exchangeBufferSize(),
                    esqlExecutor,
                    refs.acquire().delegateFailureAndWrap((l, unused) -> {
                        var remoteSink = exchangeService.newRemoteSink(rootTask, sessionId, transportService, cluster.connection);
                        exchangeSource.addRemoteSink(remoteSink, queryPragmas.concurrentExchangeClients());
                        var remotePlan = new RemoteClusterPlan(plan, cluster.concreteIndices, cluster.originalIndices);
                        var clusterRequest = new ClusterComputeRequest(cluster.clusterAlias, sessionId, configuration, remotePlan);
                        var clusterListener = ActionListener.runBefore(
                            computeListener.acquireCompute(cluster.clusterAlias()),
                            () -> l.onResponse(null)
                        );
                        transportService.sendChildRequest(
                            cluster.connection,
                            CLUSTER_ACTION_NAME,
                            clusterRequest,
                            rootTask,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(clusterListener, ComputeResponse::new, esqlExecutor)
                        );
                    })
                );
            }
        }
    }

    void runCompute(CancellableTask task, ComputeContext context, PhysicalPlan plan, ActionListener<ComputeResponse> listener) {
        listener = ActionListener.runBefore(listener, () -> Releasables.close(context.searchContexts));
        List<EsPhysicalOperationProviders.ShardContext> contexts = new ArrayList<>(context.searchContexts.size());
        for (int i = 0; i < context.searchContexts.size(); i++) {
            SearchContext searchContext = context.searchContexts.get(i);
            contexts.add(
                new EsPhysicalOperationProviders.DefaultShardContext(
                    i,
                    searchContext.getSearchExecutionContext(),
                    searchContext.request().getAliasFilter()
                )
            );
        }
        final List<Driver> drivers;
        try {
            LocalExecutionPlanner planner = new LocalExecutionPlanner(
                context.sessionId,
                context.clusterAlias,
                task,
                bigArrays,
                blockFactory,
                clusterService.getSettings(),
                context.configuration,
                context.exchangeSource(),
                context.exchangeSink(),
                enrichLookupService,
                new EsPhysicalOperationProviders(contexts)
            );

            LOGGER.debug("Received physical plan:\n{}", plan);
            plan = PlannerUtils.localPlan(context.searchExecutionContexts(), context.configuration, plan);
            // the planner will also set the driver parallelism in LocalExecutionPlanner.LocalExecutionPlan (used down below)
            // it's doing this in the planning of EsQueryExec (the source of the data)
            // see also EsPhysicalOperationProviders.sourcePhysicalOperation
            LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(plan);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Local execution plan:\n{}", localExecutionPlan.describe());
            }
            drivers = localExecutionPlan.createDrivers(context.sessionId);
            if (drivers.isEmpty()) {
                throw new IllegalStateException("no drivers created");
            }
            LOGGER.debug("using {} drivers", drivers.size());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        ActionListener<Void> listenerCollectingStatus = listener.map(ignored -> {
            if (context.configuration.profile()) {
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

    private void acquireSearchContexts(
        String clusterAlias,
        List<ShardId> shardIds,
        Configuration configuration,
        Map<Index, AliasFilter> aliasFilters,
        ActionListener<List<SearchContext>> listener
    ) {
        final List<IndexShard> targetShards = new ArrayList<>();
        try {
            for (ShardId shardId : shardIds) {
                var indexShard = searchService.getIndicesService().indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
                targetShards.add(indexShard);
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final var doAcquire = ActionRunnable.supply(listener, () -> {
            final List<SearchContext> searchContexts = new ArrayList<>(targetShards.size());
            boolean success = false;
            try {
                for (IndexShard shard : targetShards) {
                    var aliasFilter = aliasFilters.getOrDefault(shard.shardId().getIndex(), AliasFilter.EMPTY);
                    var shardRequest = new ShardSearchRequest(
                        shard.shardId(),
                        configuration.absoluteStartedTimeInMillis(),
                        aliasFilter,
                        clusterAlias
                    );
                    // TODO: `searchService.createSearchContext` allows opening search contexts without limits,
                    // we need to limit the number of active search contexts here or in SearchService
                    SearchContext context = searchService.createSearchContext(shardRequest, SearchService.NO_TIMEOUT);
                    searchContexts.add(context);
                }
                for (SearchContext searchContext : searchContexts) {
                    searchContext.preProcess();
                }
                success = true;
                return searchContexts;
            } finally {
                if (success == false) {
                    IOUtils.close(searchContexts);
                }
            }
        });
        final AtomicBoolean waitedForRefreshes = new AtomicBoolean();
        try (RefCountingRunnable refs = new RefCountingRunnable(() -> {
            if (waitedForRefreshes.get()) {
                esqlExecutor.execute(doAcquire);
            } else {
                doAcquire.run();
            }
        })) {
            for (IndexShard targetShard : targetShards) {
                final Releasable ref = refs.acquire();
                targetShard.ensureShardSearchActive(await -> {
                    try (ref) {
                        if (await) {
                            waitedForRefreshes.set(true);
                        }
                    }
                });
            }
        }
    }

    record DataNode(Transport.Connection connection, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters) {

    }

    /**
     * Result from lookupDataNodes where can_match is performed to determine what shards can be skipped
     * and which target nodes are needed for running the ES|QL query
     * @param dataNodes list of DataNode to perform the ES|QL query on
     * @param totalShards Total number of shards (from can_match phase), including skipped shards
     * @param skippedShards Number of skipped shards (from can_match phase)
     */
    record DataNodeResult(List<DataNode> dataNodes, int totalShards, int skippedShards) {}

    record RemoteCluster(String clusterAlias, Transport.Connection connection, String[] concreteIndices, OriginalIndices originalIndices) {

    }

    /**
     * Performs can_match and find the target nodes for the given target indices and filter.
     * <p>
     * Ideally, the search_shards API should be called before the field-caps API; however, this can lead
     * to a situation where the column structure (i.e., matched data types) differs depending on the query.
     */
    private void lookupDataNodes(
        Task parentTask,
        String clusterAlias,
        QueryBuilder filter,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        ActionListener<DataNodeResult> listener
    ) {
        ActionListener<SearchShardsResponse> searchShardsListener = listener.map(resp -> {
            Map<String, DiscoveryNode> nodes = new HashMap<>();
            for (DiscoveryNode node : resp.getNodes()) {
                nodes.put(node.getId(), node);
            }
            Map<String, List<ShardId>> nodeToShards = new HashMap<>();
            Map<String, Map<Index, AliasFilter>> nodeToAliasFilters = new HashMap<>();
            int totalShards = 0;
            int skippedShards = 0;
            for (SearchShardsGroup group : resp.getGroups()) {
                var shardId = group.shardId();
                if (group.allocatedNodes().isEmpty()) {
                    throw new ShardNotFoundException(group.shardId(), "no shard copies found {}", group.shardId());
                }
                if (concreteIndices.contains(shardId.getIndexName()) == false) {
                    continue;
                }
                totalShards++;
                if (group.skipped()) {
                    skippedShards++;
                    continue;
                }
                String targetNode = group.allocatedNodes().get(0);
                nodeToShards.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(shardId);
                AliasFilter aliasFilter = resp.getAliasFilters().get(shardId.getIndex().getUUID());
                if (aliasFilter != null) {
                    nodeToAliasFilters.computeIfAbsent(targetNode, k -> new HashMap<>()).put(shardId.getIndex(), aliasFilter);
                }
            }
            List<DataNode> dataNodes = new ArrayList<>(nodeToShards.size());
            for (Map.Entry<String, List<ShardId>> e : nodeToShards.entrySet()) {
                DiscoveryNode node = nodes.get(e.getKey());
                Map<Index, AliasFilter> aliasFilters = nodeToAliasFilters.getOrDefault(e.getKey(), Map.of());
                dataNodes.add(new DataNode(transportService.getConnection(node), e.getValue(), aliasFilters));
            }
            return new DataNodeResult(dataNodes, totalShards, skippedShards);
        });
        SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
            originalIndices.indices(),
            originalIndices.indicesOptions(),
            filter,
            null,
            null,
            false,
            clusterAlias
        );
        transportService.sendChildRequest(
            transportService.getLocalNode(),
            EsqlSearchShardsAction.TYPE.name(),
            searchShardsRequest,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(searchShardsListener, SearchShardsResponse::new, esqlExecutor)
        );
    }

    // TODO: Use an internal action here
    public static final String DATA_ACTION_NAME = EsqlQueryAction.NAME + "/data";

    private class DataNodeRequestExecutor {
        private final DataNodeRequest request;
        private final CancellableTask parentTask;
        private final ExchangeSinkHandler exchangeSink;
        private final ComputeListener computeListener;
        private final int maxConcurrentShards;
        private final ExchangeSink blockingSink; // block until we have completed on all shards or the coordinator has enough data

        DataNodeRequestExecutor(
            DataNodeRequest request,
            CancellableTask parentTask,
            ExchangeSinkHandler exchangeSink,
            int maxConcurrentShards,
            ComputeListener computeListener
        ) {
            this.request = request;
            this.parentTask = parentTask;
            this.exchangeSink = exchangeSink;
            this.computeListener = computeListener;
            this.maxConcurrentShards = maxConcurrentShards;
            this.blockingSink = exchangeSink.createExchangeSink();
        }

        void start() {
            parentTask.addListener(
                () -> exchangeService.finishSinkHandler(request.sessionId(), new TaskCancelledException(parentTask.getReasonCancelled()))
            );
            runBatch(0);
        }

        private void runBatch(int startBatchIndex) {
            final Configuration configuration = request.configuration();
            final String clusterAlias = request.clusterAlias();
            final var sessionId = request.sessionId();
            final int endBatchIndex = Math.min(startBatchIndex + maxConcurrentShards, request.shardIds().size());
            List<ShardId> shardIds = request.shardIds().subList(startBatchIndex, endBatchIndex);
            ActionListener<ComputeResponse> batchListener = new ActionListener<>() {
                final ActionListener<ComputeResponse> ref = computeListener.acquireCompute();

                @Override
                public void onResponse(ComputeResponse result) {
                    try {
                        onBatchCompleted(endBatchIndex);
                    } finally {
                        ref.onResponse(result);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        exchangeService.finishSinkHandler(request.sessionId(), e);
                    } finally {
                        ref.onFailure(e);
                    }
                }
            };
            acquireSearchContexts(clusterAlias, shardIds, configuration, request.aliasFilters(), ActionListener.wrap(searchContexts -> {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH, ESQL_WORKER_THREAD_POOL_NAME);
                var computeContext = new ComputeContext(sessionId, clusterAlias, searchContexts, configuration, null, exchangeSink);
                runCompute(parentTask, computeContext, request.plan(), batchListener);
            }, batchListener::onFailure));
        }

        private void onBatchCompleted(int lastBatchIndex) {
            if (lastBatchIndex < request.shardIds().size() && exchangeSink.isFinished() == false) {
                runBatch(lastBatchIndex);
            } else {
                // don't return until all pages are fetched
                var completionListener = computeListener.acquireAvoid();
                exchangeSink.addCompletionListener(
                    ActionListener.runAfter(completionListener, () -> exchangeService.finishSinkHandler(request.sessionId(), null))
                );
                blockingSink.finish();
            }
        }
    }

    private void runComputeOnDataNode(
        CancellableTask task,
        String externalId,
        PhysicalPlan reducePlan,
        DataNodeRequest request,
        ComputeListener computeListener
    ) {
        var parentListener = computeListener.acquireAvoid();
        try {
            // run compute with target shards
            var internalSink = exchangeService.createSinkHandler(request.sessionId(), request.pragmas().exchangeBufferSize());
            DataNodeRequestExecutor dataNodeRequestExecutor = new DataNodeRequestExecutor(
                request,
                task,
                internalSink,
                request.configuration().pragmas().maxConcurrentShardsPerNode(),
                computeListener
            );
            dataNodeRequestExecutor.start();
            // run the node-level reduction
            var externalSink = exchangeService.getSinkHandler(externalId);
            task.addListener(() -> exchangeService.finishSinkHandler(externalId, new TaskCancelledException(task.getReasonCancelled())));
            var exchangeSource = new ExchangeSourceHandler(1, esqlExecutor);
            exchangeSource.addCompletionListener(computeListener.acquireAvoid());
            exchangeSource.addRemoteSink(internalSink::fetchPageAsync, 1);
            ActionListener<ComputeResponse> reductionListener = computeListener.acquireCompute();
            runCompute(
                task,
                new ComputeContext(
                    request.sessionId(),
                    request.clusterAlias(),
                    List.of(),
                    request.configuration(),
                    exchangeSource,
                    externalSink
                ),
                reducePlan,
                ActionListener.wrap(resp -> {
                    // don't return until all pages are fetched
                    externalSink.addCompletionListener(ActionListener.running(() -> {
                        exchangeService.finishSinkHandler(externalId, null);
                        reductionListener.onResponse(resp);
                    }));
                }, e -> {
                    exchangeService.finishSinkHandler(externalId, e);
                    reductionListener.onFailure(e);
                })
            );
            parentListener.onResponse(null);
        } catch (Exception e) {
            exchangeService.finishSinkHandler(externalId, e);
            exchangeService.finishSinkHandler(request.sessionId(), e);
            parentListener.onFailure(e);
        }
    }

    private class DataNodeRequestHandler implements TransportRequestHandler<DataNodeRequest> {
        @Override
        public void messageReceived(DataNodeRequest request, TransportChannel channel, Task task) {
            final ActionListener<ComputeResponse> listener = new ChannelActionListener<>(channel);
            final ExchangeSinkExec reducePlan;
            if (request.plan() instanceof ExchangeSinkExec plan) {
                var fragments = plan.collectFirstChildren(FragmentExec.class::isInstance);
                if (fragments.isEmpty()) {
                    listener.onFailure(new IllegalStateException("expected a fragment plan for a remote compute; got " + request.plan()));
                    return;
                }

                var localExchangeSource = new ExchangeSourceExec(plan.source(), plan.output(), plan.isIntermediateAgg());
                FragmentExec fragment = (FragmentExec) fragments.get(0);
                reducePlan = new ExchangeSinkExec(
                    plan.source(),
                    plan.output(),
                    plan.isIntermediateAgg(),
                    fragment.reducer() != null ? fragment.reducer().replaceChildren(List.of(localExchangeSource)) : localExchangeSource
                );
            } else {
                listener.onFailure(new IllegalStateException("expected exchange sink for a remote compute; got " + request.plan()));
                return;
            }
            final String sessionId = request.sessionId();
            request = new DataNodeRequest(
                sessionId + "[n]", // internal session
                request.configuration(),
                request.clusterAlias(),
                request.shardIds(),
                request.aliasFilters(),
                request.plan(),
                request.indices(),
                request.indicesOptions()
            );
            try (var computeListener = ComputeListener.create(transportService, (CancellableTask) task, listener)) {
                runComputeOnDataNode((CancellableTask) task, sessionId, reducePlan, request, computeListener);
            }
        }
    }

    public static final String CLUSTER_ACTION_NAME = EsqlQueryAction.NAME + "/cluster";

    private class ClusterRequestHandler implements TransportRequestHandler<ClusterComputeRequest> {
        @Override
        public void messageReceived(ClusterComputeRequest request, TransportChannel channel, Task task) {
            ChannelActionListener<ComputeResponse> listener = new ChannelActionListener<>(channel);
            RemoteClusterPlan remoteClusterPlan = request.remoteClusterPlan();
            var plan = remoteClusterPlan.plan();
            if (plan instanceof ExchangeSinkExec == false) {
                listener.onFailure(new IllegalStateException("expected exchange sink for a remote compute; got " + plan));
                return;
            }
            String clusterAlias = request.clusterAlias();
            /*
             * This handler runs only on remote cluster coordinators, so it creates a new local EsqlExecutionInfo object to record
             * execution metadata for ES|QL processing local to this cluster. The execution info will be copied into the
             * ComputeResponse that is sent back to the primary coordinating cluster.
             */
            EsqlExecutionInfo execInfo = new EsqlExecutionInfo(true);
            execInfo.swapCluster(clusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(clusterAlias, Arrays.toString(request.indices())));
            CancellableTask cancellable = (CancellableTask) task;
            try (var computeListener = ComputeListener.create(clusterAlias, transportService, cancellable, execInfo, listener)) {
                runComputeOnRemoteCluster(
                    clusterAlias,
                    request.sessionId(),
                    (CancellableTask) task,
                    request.configuration(),
                    (ExchangeSinkExec) plan,
                    Set.of(remoteClusterPlan.targetIndices()),
                    remoteClusterPlan.originalIndices(),
                    execInfo,
                    computeListener
                );
            }
        }
    }

    /**
     * Performs a compute on a remote cluster. The output pages are placed in an exchange sink specified by
     * {@code globalSessionId}. The coordinator on the main cluster will poll pages from there.
     * <p>
     * Currently, the coordinator on the remote cluster simply collects pages from data nodes in the remote cluster
     * and places them in the exchange sink. We can achieve this by using a single exchange buffer to minimize overhead.
     * However, here we use two exchange buffers so that we can run an actual plan on this coordinator to perform partial
     * reduce operations, such as limit, topN, and partial-to-partial aggregation in the future.
     */
    void runComputeOnRemoteCluster(
        String clusterAlias,
        String globalSessionId,
        CancellableTask parentTask,
        Configuration configuration,
        ExchangeSinkExec plan,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        EsqlExecutionInfo executionInfo,
        ComputeListener computeListener
    ) {
        final var exchangeSink = exchangeService.getSinkHandler(globalSessionId);
        parentTask.addListener(
            () -> exchangeService.finishSinkHandler(globalSessionId, new TaskCancelledException(parentTask.getReasonCancelled()))
        );
        final String localSessionId = clusterAlias + ":" + globalSessionId;
        var exchangeSource = new ExchangeSourceHandler(
            configuration.pragmas().exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        try (Releasable ignored = exchangeSource.addEmptySink()) {
            exchangeSink.addCompletionListener(computeListener.acquireAvoid());
            exchangeSource.addCompletionListener(computeListener.acquireAvoid());
            PhysicalPlan coordinatorPlan = new ExchangeSinkExec(
                plan.source(),
                plan.output(),
                plan.isIntermediateAgg(),
                new ExchangeSourceExec(plan.source(), plan.output(), plan.isIntermediateAgg())
            );
            runCompute(
                parentTask,
                new ComputeContext(localSessionId, clusterAlias, List.of(), configuration, exchangeSource, exchangeSink),
                coordinatorPlan,
                computeListener.acquireCompute(clusterAlias)
            );
            startComputeOnDataNodes(
                localSessionId,
                clusterAlias,
                parentTask,
                configuration,
                plan,
                concreteIndices,
                originalIndices,
                exchangeSource,
                executionInfo,
                computeListener
            );
        }
    }

    record ComputeContext(
        String sessionId,
        String clusterAlias,
        List<SearchContext> searchContexts,
        Configuration configuration,
        ExchangeSourceHandler exchangeSource,
        ExchangeSinkHandler exchangeSink
    ) {
        public List<SearchExecutionContext> searchExecutionContexts() {
            return searchContexts.stream().map(ctx -> ctx.getSearchExecutionContext()).toList();
        }
    }
}
