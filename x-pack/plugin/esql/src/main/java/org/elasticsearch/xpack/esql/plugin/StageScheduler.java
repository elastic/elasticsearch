/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.StageSinkExec;
import org.elasticsearch.xpack.esql.planner.ExchangeWiring;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Resolves a static stage topology and starts its pipelined shuffle exchanges.
 */
final class StageScheduler {
    private static final Logger LOGGER = LogManager.getLogger(StageScheduler.class);

    private final ComputeService computeService;
    private final DataNodeComputeHandler dataNodeComputeHandler;
    private final ExchangeService exchangeService;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Executor searchExecutor;
    private final ThreadContext threadContext;

    StageScheduler(
        ComputeService computeService,
        DataNodeComputeHandler dataNodeComputeHandler,
        ExchangeService exchangeService,
        TransportService transportService,
        ClusterService clusterService,
        Executor searchExecutor
    ) {
        this.computeService = computeService;
        this.dataNodeComputeHandler = dataNodeComputeHandler;
        this.exchangeService = exchangeService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.searchExecutor = searchExecutor;
        this.threadContext = transportService.getThreadPool().getThreadContext();
    }

    void execute(
        String rootSessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        PhysicalPlan resolvedPlan,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        String profileQualifier,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        ByteSizeValue maxCollectedBytes,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        BoundedPageCollector pageCollector = new BoundedPageCollector(maxCollectedBytes);
        List<Page> collectedPages = pageCollector.pages();
        ActionListener<Result> resultListener = ActionListener.notifyOnce(listener.delegateResponse((l, e) -> {
            collectedPages.forEach(page -> Releasables.closeExpectNoException(page::releaseBlocks));
            l.onFailure(e);
        }));
        ActionListener<DriverCompletionInfo> completionListener = resultListener.map(completionInfo -> {
            ComputeService.updateExecutionInfoAfterCoordinatorOnlyQuery(execInfo, true);
            return new Result(resolvedPlan.output(), collectedPages, null, configuration, completionInfo, execInfo, null);
        });
        ComputeListener computeListener = new ComputeListener(computeService.cancelQueryOnFailure(rootTask), completionListener);
        ActionListener<Void> scheduleListener = ActionListener.notifyOnce(
            ActionListener.runAfter(computeListener.acquireAvoid(), computeListener::close)
        );

        try {
            if (exchangeSinkSupplier != null) {
                throw new IllegalStateException("shuffle stages cannot run with an external exchange sink");
            }
            if (clusterService.state().getMinTransportVersion().supports(DataNodeRequest.ESQL_SHUFFLE_STAGE_WIRING) == false) {
                throw new IllegalStateException(
                    "shuffle stages require transport version [" + DataNodeRequest.ESQL_SHUFFLE_STAGE_WIRING + "]"
                );
            }
            ComputeService.updateShardCountForCoordinatorOnlyQuery(execInfo);
            StagedPlanner.StagedPlan stagedPlan = StagedPlanner.split(resolvedPlan);
            if (stagedPlan.root().placement() != StagedPlanner.StagePlacement.COORDINATOR) {
                throw new IllegalStateException("bug: shuffle root stage must run on the coordinator");
            }
            Map<Integer, ShardStageInfo> shardStageInfos = shardStageInfos(stagedPlan);
            List<String> sharedIndices = sharedOriginalIndices(shardStageInfos);
            resolveShardStages(rootTask, flags, configuration, shardStageInfos, ActionListener.wrap(resolvedShards -> {
                try {
                    Topology topology = buildTopology(stagedPlan, shardStageInfos, resolvedShards, sharedIndices, configuration);
                    logTopology(stagedPlan, topology);
                    openExchangeSinks(stagedPlan, topology, rootSessionId, configuration, ActionListener.wrap(ignored -> {
                        try {
                            startStages(
                                rootSessionId,
                                rootTask,
                                flags,
                                stagedPlan,
                                topology,
                                shardStageInfos,
                                resolvedPlan,
                                configuration,
                                foldContext,
                                profileQualifier,
                                pageCollector,
                                planTimeProfile,
                                computeListener
                            );
                            scheduleListener.onResponse(null);
                        } catch (Exception e) {
                            scheduleListener.onFailure(e);
                        }
                    }, scheduleListener::onFailure));
                } catch (Exception e) {
                    scheduleListener.onFailure(e);
                }
            }, scheduleListener::onFailure));
        } catch (Exception e) {
            scheduleListener.onFailure(e);
        }
    }

    private Map<Integer, ShardStageInfo> shardStageInfos(StagedPlanner.StagedPlan stagedPlan) {
        Map<Integer, ShardStageInfo> result = new LinkedHashMap<>();
        for (StagedPlanner.Stage stage : stagedPlan.stages()) {
            if (stage.placement() != StagedPlanner.StagePlacement.SHARDS) {
                continue;
            }
            if ((stage.plan() instanceof StageSinkExec) == false) {
                throw new IllegalStateException("bug: expected SHARDS stage sink but got [" + stage.plan() + "]");
            }
            StageSinkExec stageSink = (StageSinkExec) stage.plan();
            PhysicalPlan stageFragment = StagedPlanner.splitShardsStage(stageSink).v1().child();
            Map<String, OriginalIndices> concreteByCluster = ComputeService.getIndices(stageFragment, EsRelation::concreteIndices);
            Map<String, OriginalIndices> originalByCluster = ComputeService.getIndices(stageFragment, EsRelation::originalIndices);
            OriginalIndices concreteIndices = concreteByCluster.get(ComputeService.LOCAL_CLUSTER);
            EsRelation lookupRelation = stageFragment instanceof FragmentExec fragment ? StagedPlanner.lookupScanRelation(fragment) : null;
            boolean lookupScan = lookupRelation != null;
            OriginalIndices originalIndices = lookupScan
                ? new OriginalIndices(
                    new String[] { lookupRelation.indexPattern() },
                    IndicesOptions.strictSingleIndexNoExpandForbidClosed()
                )
                : originalByCluster.get(ComputeService.LOCAL_CLUSTER);
            if (concreteIndices == null || originalIndices == null) {
                throw new IllegalStateException("bug: SHARDS stage [" + stage.id() + "] has no local index expressions");
            }
            Set<String> concreteIndexNames = new HashSet<>();
            Collections.addAll(concreteIndexNames, concreteIndices.indices());
            result.put(stage.id(), new ShardStageInfo(stageFragment, Set.copyOf(concreteIndexNames), originalIndices, lookupScan));
        }
        return Map.copyOf(result);
    }

    private static List<String> sharedOriginalIndices(Map<Integer, ShardStageInfo> shardStageInfos) {
        Set<String> indices = new TreeSet<>();
        for (ShardStageInfo shardStageInfo : shardStageInfos.values()) {
            Collections.addAll(indices, shardStageInfo.originalIndices().indices());
        }
        return List.copyOf(indices);
    }

    private void resolveShardStages(
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        Map<Integer, ShardStageInfo> shardStageInfos,
        ActionListener<Map<Integer, Map<DiscoveryNode, DataNodeComputeHandler.NodeShards>>> listener
    ) {
        if (shardStageInfos.isEmpty()) {
            listener.onResponse(Map.of());
            return;
        }
        Map<Integer, Map<DiscoveryNode, DataNodeComputeHandler.NodeShards>> resolvedShards = new java.util.concurrent.ConcurrentHashMap<>();
        AtomicInteger remainingStages = new AtomicInteger(shardStageInfos.size());
        AtomicBoolean completed = new AtomicBoolean();
        for (Map.Entry<Integer, ShardStageInfo> entry : shardStageInfos.entrySet()) {
            if (completed.get()) {
                break;
            }
            try {
                ShardStageInfo info = entry.getValue();
                dataNodeComputeHandler.resolveShardsStatically(
                    rootTask,
                    flags,
                    configuration,
                    info.fragmentPlan(),
                    info.concreteIndices(),
                    info.originalIndices(),
                    info.lookupScan(),
                    ActionListener.wrap(nodes -> {
                        resolvedShards.put(entry.getKey(), nodes);
                        if (remainingStages.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
                            listener.onResponse(Map.copyOf(resolvedShards));
                        }
                    }, e -> {
                        if (completed.compareAndSet(false, true)) {
                            listener.onFailure(e);
                        }
                    })
                );
            } catch (Exception e) {
                if (completed.compareAndSet(false, true)) {
                    listener.onFailure(e);
                }
            }
        }
    }

    private Topology buildTopology(
        StagedPlanner.StagedPlan stagedPlan,
        Map<Integer, ShardStageInfo> shardStageInfos,
        Map<Integer, Map<DiscoveryNode, DataNodeComputeHandler.NodeShards>> resolvedShards,
        List<String> sharedIndices,
        Configuration configuration
    ) {
        List<DiscoveryNode> workers = clusterService.state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .sorted(Comparator.comparing(DiscoveryNode::getId))
            .toList();
        int partitionsPerNode = configuration.pragmas().shufflePartitionsPerNode();
        if (partitionsPerNode < 1) {
            throw new IllegalStateException("shuffle partitions per node must be positive");
        }
        int partitionCount = Math.multiplyExact(workers.size(), partitionsPerNode);
        Map<Integer, StageLocation> stageLocations = new LinkedHashMap<>();
        DiscoveryNode localNode = clusterService.localNode();
        for (StagedPlanner.Stage stage : stagedPlan.stages()) {
            Map<DiscoveryNode, List<Integer>> lanesByNode = new LinkedHashMap<>();
            Map<DiscoveryNode, DataNodeComputeHandler.NodeShards> stageShards = Map.of();
            int laneCount = switch (stage.placement()) {
                case COORDINATOR, SINGLE -> {
                    lanesByNode.put(localNode, List.of(0));
                    yield 1;
                }
                case SHARDS -> {
                    stageShards = resolvedShards.get(stage.id());
                    if (stageShards == null) {
                        throw new IllegalStateException("missing static shard resolution for stage [" + stage.id() + "]");
                    }
                    List<Map.Entry<DiscoveryNode, DataNodeComputeHandler.NodeShards>> entries = stageShards.entrySet()
                        .stream()
                        .sorted(Comparator.comparing(entry -> entry.getKey().getId()))
                        .toList();
                    int lane = 0;
                    for (Map.Entry<DiscoveryNode, DataNodeComputeHandler.NodeShards> entry : entries) {
                        lanesByNode.put(entry.getKey(), List.of(lane++));
                    }
                    yield lane;
                }
                case WORKERS -> {
                    for (int lane = 0; lane < partitionCount; lane++) {
                        DiscoveryNode worker = workers.get(lane % workers.size());
                        lanesByNode.computeIfAbsent(worker, ignored -> new ArrayList<>()).add(lane);
                    }
                    yield partitionCount;
                }
            };
            stageLocations.put(stage.id(), new StageLocation(lanesByNode, stageShards, laneCount));
        }
        Topology topology = new Topology(stageLocations, workers, partitionsPerNode, partitionCount, sharedIndices);
        validateWorkerHashInputs(stagedPlan, topology);
        return topology;
    }

    private static void validateWorkerHashInputs(StagedPlanner.StagedPlan stagedPlan, Topology topology) {
        for (StagedPlanner.Stage stage : stagedPlan.stages()) {
            if (stage.placement() != StagedPlanner.StagePlacement.WORKERS) {
                continue;
            }
            for (String edgeKey : stage.inputEdges()) {
                StagedPlanner.Edge edge = stagedPlan.edge(edgeKey);
                if (edge.distribution() != ExchangeDistribution.HASH) {
                    throw new IllegalStateException("bug: WORKERS stage [" + stage.id() + "] has a non-HASH input edge");
                }
                int consumerLanes = stageLocation(topology, edge.consumerStage()).laneCount();
                if (consumerLanes != topology.partitionCount()) {
                    throw new IllegalStateException(
                        "bug: WORKERS stage ["
                            + stage.id()
                            + "] has ["
                            + consumerLanes
                            + "] consumer lanes, expected ["
                            + topology.partitionCount()
                            + "]"
                    );
                }
            }
        }
    }

    private void openExchangeSinks(
        StagedPlanner.StagedPlan stagedPlan,
        Topology topology,
        String rootSessionId,
        Configuration configuration,
        ActionListener<Void> listener
    ) {
        List<OpenExchange> exchangesToOpen = new ArrayList<>();
        for (StagedPlanner.Edge edge : stagedPlan.edges()) {
            StageLocation producer = stageLocation(topology, edge.producerStage());
            int consumerLanes = stageLocation(topology, edge.consumerStage()).laneCount();
            for (DiscoveryNode producerNode : sortedNodes(producer.lanesByNode().keySet())) {
                for (int consumerLane = 0; consumerLane < consumerLanes; consumerLane++) {
                    exchangesToOpen.add(
                        new OpenExchange(producerNode, StageWiring.exchangeId(rootSessionId, edge.edgeKey(), consumerLane))
                    );
                }
            }
        }
        if (exchangesToOpen.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        AtomicInteger remainingExchanges = new AtomicInteger(exchangesToOpen.size());
        AtomicBoolean completed = new AtomicBoolean();
        for (OpenExchange exchange : exchangesToOpen) {
            if (completed.get()) {
                break;
            }
            try {
                Transport.Connection connection = transportService.getConnection(exchange.producerNode());
                ExchangeService.openExchange(
                    transportService,
                    connection,
                    exchange.exchangeId(),
                    configuration.pragmas().exchangeBufferSize(),
                    searchExecutor,
                    ActionListener.wrap(ignored -> {
                        if (remainingExchanges.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
                            listener.onResponse(null);
                        }
                    }, e -> {
                        if (completed.compareAndSet(false, true)) {
                            listener.onFailure(e);
                        }
                    })
                );
            } catch (Exception e) {
                if (completed.compareAndSet(false, true)) {
                    listener.onFailure(e);
                }
            }
        }
    }

    private void startStages(
        String rootSessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        StagedPlanner.StagedPlan stagedPlan,
        Topology topology,
        Map<Integer, ShardStageInfo> shardStageInfos,
        PhysicalPlan resolvedPlan,
        Configuration configuration,
        FoldContext foldContext,
        String profileQualifier,
        BoundedPageCollector pageCollector,
        PlanTimeProfile planTimeProfile,
        ComputeListener computeListener
    ) {
        StagedPlanner.Stage coordinatorStage = stagedPlan.root();
        ExchangeWiring coordinatorWiring = coordinatorWiring(
            rootTask,
            rootSessionId,
            coordinatorStage,
            stagedPlan,
            topology,
            configuration,
            computeListener
        );
        ComputeContext coordinatorContext = new ComputeContext(
            computeService.newChildSession(rootSessionId),
            computeService.profileDescription(profileQualifier, "shuffle_coordinator"),
            ComputeService.LOCAL_CLUSTER,
            flags,
            EmptyIndexedByShardId.instance(),
            configuration,
            foldContext,
            null,
            null,
            false,
            false,
            coordinatorWiring
        );
        ActionListener<DriverCompletionInfo> coordinatorListener = computeListener.acquireCompute();
        try {
            computeService.runCompute(
                rootTask,
                coordinatorContext,
                new OutputExec(coordinatorStage.plan(), pageCollector),
                computeService.plannerSettings().get(),
                LocalPhysicalOptimization.ENABLED,
                planTimeProfile,
                coordinatorListener
            );
        } catch (Exception e) {
            coordinatorListener.onFailure(e);
        }

        for (StagedPlanner.Stage stage : stagedPlan.stages()) {
            if (stage.placement() == StagedPlanner.StagePlacement.COORDINATOR) {
                continue;
            }
            StageLocation location = stageLocation(topology, stage.id());
            for (Map.Entry<DiscoveryNode, List<Integer>> entry : sortedLaneEntries(location)) {
                DiscoveryNode node = entry.getKey();
                List<Integer> localLanes = entry.getValue();
                StageWiring wiring = stageWiring(stagedPlan, topology, rootSessionId, stage, localLanes);
                List<DataNodeRequest.Shard> shards = List.of();
                Map<Index, AliasFilter> aliasFilters = Map.of();
                String[] indices = topology.sharedIndices().toArray(String[]::new);
                IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
                if (stage.placement() == StagedPlanner.StagePlacement.SHARDS) {
                    ShardStageInfo shardStageInfo = shardStageInfos.get(stage.id());
                    DataNodeComputeHandler.NodeShards nodeShards = location.nodeShards().get(node);
                    if (shardStageInfo == null || nodeShards == null) {
                        throw new IllegalStateException("missing shard request data for stage [" + stage.id() + "]");
                    }
                    shards = nodeShards.shards();
                    aliasFilters = nodeShards.aliasFilters();
                    indices = shardStageInfo.originalIndices().indices();
                    indicesOptions = shardStageInfo.originalIndices().indicesOptions();
                }
                DataNodeRequest request = new DataNodeRequest(
                    computeService.newChildSession(rootSessionId),
                    configuration,
                    ComputeService.LOCAL_CLUSTER,
                    shards,
                    aliasFilters,
                    stage.plan(),
                    indices,
                    indicesOptions,
                    false,
                    false,
                    false,
                    false,
                    List.of(),
                    wiring
                );
                ActionListener<DriverCompletionInfo> stageListener = computeListener.acquireCompute();
                try {
                    Transport.Connection connection = transportService.getConnection(node);
                    transportService.sendChildRequest(
                        connection,
                        ComputeService.DATA_ACTION_NAME,
                        request,
                        rootTask,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            stageListener.map(DataNodeComputeResponse::completionInfo),
                            in -> new DataNodeComputeResponse(in, threadContext),
                            searchExecutor
                        )
                    );
                } catch (Exception e) {
                    stageListener.onFailure(e);
                }
            }
        }
    }

    private ExchangeWiring coordinatorWiring(
        CancellableTask rootTask,
        String rootSessionId,
        StagedPlanner.Stage coordinatorStage,
        StagedPlanner.StagedPlan stagedPlan,
        Topology topology,
        Configuration configuration,
        ComputeListener computeListener
    ) {
        Map<String, Supplier<ExchangeSource>> sources = new HashMap<>();
        for (String edgeKey : coordinatorStage.inputEdges()) {
            StagedPlanner.Edge edge = stagedPlan.edge(edgeKey);
            ExchangeSourceHandler source = new ExchangeSourceHandler(configuration.pragmas().exchangeBufferSize(), searchExecutor);
            try (Releasable hold = source.addEmptySink()) {
                for (String producerNodeId : nodeIds(topology, edge.producerStage())) {
                    DiscoveryNode producerNode = clusterService.state().nodes().get(producerNodeId);
                    if (producerNode == null) {
                        throw new IllegalStateException("producer node [" + producerNodeId + "] for exchange [" + edgeKey + "] is missing");
                    }
                    source.addRemoteSink(
                        exchangeService.newRemoteSink(
                            rootTask,
                            StageWiring.exchangeId(rootSessionId, edgeKey, 0),
                            transportService,
                            transportService.getConnection(producerNode)
                        ),
                        true,
                        () -> {},
                        configuration.pragmas().concurrentExchangeClients(),
                        computeListener.acquireAvoid()
                    );
                }
            }
            if (sources.put(edgeKey, source::createExchangeSource) != null) {
                throw new IllegalStateException("duplicate coordinator input edge [" + edgeKey + "]");
            }
        }
        return new ExchangeWiring(sources, Map.of());
    }

    private static StageWiring stageWiring(
        StagedPlanner.StagedPlan stagedPlan,
        Topology topology,
        String rootSessionId,
        StagedPlanner.Stage stage,
        List<Integer> localLanes
    ) {
        List<StageWiring.Input> inputs = new ArrayList<>(stage.inputEdges().size());
        for (String edgeKey : stage.inputEdges()) {
            StagedPlanner.Edge edge = stagedPlan.edge(edgeKey);
            inputs.add(new StageWiring.Input(edgeKey, nodeIds(topology, edge.producerStage())));
        }
        if (stage.outputEdge() == null) {
            throw new IllegalStateException("bug: non-coordinator stage [" + stage.id() + "] has no output edge");
        }
        StagedPlanner.Edge outputEdge = stagedPlan.edge(stage.outputEdge());
        int consumerLanes = stageLocation(topology, outputEdge.consumerStage()).laneCount();
        return new StageWiring(
            rootSessionId,
            stage.id(),
            stage.placement(),
            localLanes,
            inputs,
            new StageWiring.Output(outputEdge.edgeKey(), outputEdge.distribution(), consumerLanes)
        );
    }

    private static List<String> nodeIds(Topology topology, int stageId) {
        return sortedNodes(stageLocation(topology, stageId).lanesByNode().keySet()).stream().map(DiscoveryNode::getId).toList();
    }

    private static StageLocation stageLocation(Topology topology, int stageId) {
        StageLocation location = topology.stageLocations().get(stageId);
        if (location == null) {
            throw new IllegalStateException("unknown stage [" + stageId + "]");
        }
        return location;
    }

    private static List<DiscoveryNode> sortedNodes(Set<DiscoveryNode> nodes) {
        return nodes.stream().sorted(Comparator.comparing(DiscoveryNode::getId)).toList();
    }

    private static List<Map.Entry<DiscoveryNode, List<Integer>>> sortedLaneEntries(StageLocation location) {
        return location.lanesByNode().entrySet().stream().sorted(Comparator.comparing(entry -> entry.getKey().getId())).toList();
    }

    private static void logTopology(StagedPlanner.StagedPlan stagedPlan, Topology topology) {
        if (LOGGER.isDebugEnabled()) {
            List<String> stages = stagedPlan.stages().stream().map(stage -> {
                StageLocation location = stageLocation(topology, stage.id());
                List<String> nodeLanes = sortedLaneEntries(location).stream()
                    .map(entry -> entry.getKey().getId() + ":" + entry.getValue())
                    .toList();
                return "stage" + stage.id() + "=" + stage.placement() + ", lanes=" + location.laneCount() + ", nodes=" + nodeLanes;
            }).toList();
            List<String> workers = topology.workers().stream().map(DiscoveryNode::getId).toList();
            LOGGER.debug(
                () -> Strings.format(
                    "shuffle topology stages [%s], workers [%s], edges [%s], partitions per node [%d], P [%d]",
                    stages,
                    workers,
                    stagedPlan.edges(),
                    topology.partitionsPerNode(),
                    topology.partitionCount()
                )
            );
        }
    }

    private record ShardStageInfo(
        PhysicalPlan fragmentPlan,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        boolean lookupScan
    ) {}

    private record StageLocation(
        Map<DiscoveryNode, List<Integer>> lanesByNode,
        Map<DiscoveryNode, DataNodeComputeHandler.NodeShards> nodeShards,
        int laneCount
    ) {
        StageLocation {
            Map<DiscoveryNode, List<Integer>> copiedLanes = new LinkedHashMap<>();
            lanesByNode.forEach((node, lanes) -> copiedLanes.put(node, List.copyOf(lanes)));
            lanesByNode = Collections.unmodifiableMap(copiedLanes);
            nodeShards = Map.copyOf(nodeShards);
        }
    }

    private record Topology(
        Map<Integer, StageLocation> stageLocations,
        List<DiscoveryNode> workers,
        int partitionsPerNode,
        int partitionCount,
        List<String> sharedIndices
    ) {
        Topology {
            stageLocations = Map.copyOf(stageLocations);
            workers = List.copyOf(workers);
            sharedIndices = List.copyOf(sharedIndices);
        }
    }

    private record OpenExchange(DiscoveryNode producerNode, String exchangeId) {}
}
