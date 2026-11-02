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
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
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
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.StageSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.StageSourceExec;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
        ComputeService.ExternalDistributionResult externalDistributionResult,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        String profileQualifier,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        ByteSizeValue maxCollectedBytes,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        List<OpenExchange> exchangesToClose = new ArrayList<>();
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
        ComputeListener computeListener = new ComputeListener(
            computeService.cancelQueryOnFailure(rootTask),
            completionListener.delegateResponse(
                (l, failure) -> closeExchangeSinks(
                    exchangesToClose,
                    ActionListener.wrap(ignored -> l.onFailure(failure), cleanupFailure -> {
                        if (cleanupFailure != failure) {
                            failure.addSuppressed(cleanupFailure);
                        }
                        l.onFailure(failure);
                    })
                )
            )
        );
        ActionListener<Void> scheduleListener = ActionListener.notifyOnce(
            ActionListener.runAfter(computeListener.acquireAvoid(), computeListener::close)
        );

        try {
            if (exchangeSinkSupplier != null) {
                throw new IllegalStateException("shuffle stages cannot run with an external exchange sink");
            }
            if (clusterService.state().getMinTransportVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION) == false) {
                throw new IllegalStateException(
                    "shuffle stages require transport version [" + ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION + "]"
                );
            }
            ComputeService.updateShardCountForCoordinatorOnlyQuery(execInfo);
            StagedPlanner.StagedPlan stagedPlan = StagedPlanner.split(resolvedPlan);
            if (stagedPlan.root().placement() != StagedPlanner.StagePlacement.COORDINATOR) {
                throw new IllegalStateException("bug: shuffle root stage must run on the coordinator");
            }
            Map<Integer, ShardStageInfo> shardStageInfos = shardStageInfos(stagedPlan);
            Map<Integer, Map<DiscoveryNode, List<ExternalSplit>>> externalSplitsByStage = externalSplitsByStage(
                stagedPlan,
                externalDistributionResult
            );
            List<String> sharedIndices = sharedOriginalIndices(shardStageInfos);
            resolveShardStages(rootTask, flags, configuration, shardStageInfos, ActionListener.wrap(resolvedShards -> {
                try {
                    Topology topology = buildTopology(
                        stagedPlan,
                        shardStageInfos,
                        resolvedShards,
                        externalSplitsByStage,
                        sharedIndices,
                        configuration
                    );
                    logTopology(stagedPlan, topology);
                    openExchangeSinks(stagedPlan, topology, rootSessionId, configuration, exchangesToClose, ActionListener.wrap(ignored -> {
                        try {
                            startStages(
                                rootSessionId,
                                rootTask,
                                flags,
                                stagedPlan,
                                topology,
                                shardStageInfos,
                                externalSplitsByStage,
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
            if (StagedPlanner.isExternalSourceStage(stage.plan())) {
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

    private Map<Integer, Map<DiscoveryNode, List<ExternalSplit>>> externalSplitsByStage(
        StagedPlanner.StagedPlan stagedPlan,
        ComputeService.ExternalDistributionResult externalDistributionResult
    ) {
        List<StagedPlanner.Stage> externalStages = stagedPlan.stages()
            .stream()
            .filter(stage -> stage.placement() == StagedPlanner.StagePlacement.SHARDS && StagedPlanner.isExternalSourceStage(stage.plan()))
            .toList();
        if (externalStages.isEmpty()) {
            return Map.of();
        }

        Map<String, List<ExternalSplit>> nodeAssignments;
        ExternalDistributionPlan distributionPlan = externalDistributionResult.distributionPlan();
        if (distributionPlan != null && distributionPlan.distributed()) {
            if (distributionPlan.nodeAssignments().isEmpty()) {
                throw new IllegalStateException("bug: distributed external source plan has no node assignments");
            }
            nodeAssignments = distributionPlan.nodeAssignments();
        } else {
            nodeAssignments = Map.of(clusterService.localNode().getId(), externalDistributionResult.coordinatorSplits());
        }

        List<String> sourcePaths = externalStages.stream().map(stage -> StagedPlanner.externalSourcePath(stage.plan())).toList();
        Map<String, Map<String, List<ExternalSplit>>> assignmentsBySource = splitAssignmentsBySource(nodeAssignments, sourcePaths);
        Map<Integer, Map<DiscoveryNode, List<ExternalSplit>>> result = new LinkedHashMap<>();
        for (StagedPlanner.Stage stage : externalStages) {
            String sourcePath = StagedPlanner.externalSourcePath(stage.plan());
            Map<DiscoveryNode, List<ExternalSplit>> assignments = new LinkedHashMap<>();
            for (Map.Entry<String, List<ExternalSplit>> entry : assignmentsBySource.get(sourcePath).entrySet()) {
                DiscoveryNode node = clusterService.state().nodes().get(entry.getKey());
                if (node == null || node.canContainData() == false) {
                    throw new IllegalStateException("external source assignment refers to unavailable data node [" + entry.getKey() + "]");
                }
                assignments.put(node, entry.getValue());
            }
            if (assignments.isEmpty()) {
                assignments.put(clusterService.localNode(), List.of());
            }
            result.put(stage.id(), Map.copyOf(assignments));
        }
        return Map.copyOf(result);
    }

    static Map<String, Map<String, List<ExternalSplit>>> splitAssignmentsBySource(
        Map<String, List<ExternalSplit>> nodeAssignments,
        List<String> sourcePaths
    ) {
        Map<String, Map<String, List<ExternalSplit>>> assignmentsBySource = new LinkedHashMap<>();
        for (String sourcePath : sourcePaths) {
            if (assignmentsBySource.putIfAbsent(sourcePath, new LinkedHashMap<>()) != null) {
                throw new IllegalStateException("bug: multiple external source stages use the same source path");
            }
        }
        for (Map.Entry<String, List<ExternalSplit>> nodeAssignment : nodeAssignments.entrySet()) {
            for (ExternalSplit split : nodeAssignment.getValue()) {
                addSplitAssignment(assignmentsBySource, sourcePaths, nodeAssignment.getKey(), split);
            }
        }

        Map<String, Map<String, List<ExternalSplit>>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, List<ExternalSplit>>> sourceAssignment : assignmentsBySource.entrySet()) {
            Map<String, List<ExternalSplit>> nodeSplits = new LinkedHashMap<>();
            for (Map.Entry<String, List<ExternalSplit>> nodeAssignment : sourceAssignment.getValue().entrySet()) {
                nodeSplits.put(nodeAssignment.getKey(), List.copyOf(nodeAssignment.getValue()));
            }
            result.put(sourceAssignment.getKey(), Map.copyOf(nodeSplits));
        }
        return Map.copyOf(result);
    }

    private static void addSplitAssignment(
        Map<String, Map<String, List<ExternalSplit>>> assignmentsBySource,
        List<String> sourcePaths,
        String nodeId,
        ExternalSplit split
    ) {
        if (split instanceof CoalescedSplit coalescedSplit) {
            List<ExternalSplit> children = CoalescedSplit.flatten(coalescedSplit.children());
            Map<String, List<ExternalSplit>> childrenBySource = new LinkedHashMap<>();
            for (ExternalSplit child : children) {
                String sourcePath = sourcePathForSplit(child, sourcePaths);
                childrenBySource.computeIfAbsent(sourcePath, ignored -> new ArrayList<>()).add(child);
            }
            for (Map.Entry<String, List<ExternalSplit>> sourceChildren : childrenBySource.entrySet()) {
                ExternalSplit assignedSplit = childrenBySource.size() == 1
                    ? coalescedSplit
                    : new CoalescedSplit(coalescedSplit.sourceType(), sourceChildren.getValue());
                assignmentsBySource.get(sourceChildren.getKey()).computeIfAbsent(nodeId, ignored -> new ArrayList<>()).add(assignedSplit);
            }
            return;
        }
        String sourcePath = sourcePathForSplit(split, sourcePaths);
        assignmentsBySource.get(sourcePath).computeIfAbsent(nodeId, ignored -> new ArrayList<>()).add(split);
    }

    private static String sourcePathForSplit(ExternalSplit split, List<String> sourcePaths) {
        if (split instanceof FileSplit fileSplit) {
            String filePath = fileSplit.path().toString();
            String matchedSourcePath = null;
            for (String sourcePath : sourcePaths) {
                if (Regex.simpleMatch(sourcePath, filePath)) {
                    if (matchedSourcePath != null) {
                        throw new IllegalStateException("bug: external split matches multiple external source stages");
                    }
                    matchedSourcePath = sourcePath;
                }
            }
            if (matchedSourcePath == null) {
                throw new IllegalStateException("bug: external split does not match an external source stage");
            }
            return matchedSourcePath;
        }
        if (sourcePaths.size() == 1) {
            return sourcePaths.getFirst();
        }
        throw new IllegalStateException("multiple external source stages require file-based splits");
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
        Map<Integer, Map<DiscoveryNode, List<ExternalSplit>>> externalSplitsByStage,
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
        Map<Integer, Integer> nodeCounts = new HashMap<>();
        for (StagedPlanner.Stage stage : stagedPlan.stages()) {
            int nodes = switch (stage.placement()) {
                case COORDINATOR, SINGLE -> 1;
                case WORKERS -> workers.size();
                case SHARDS -> externalSplitsByStage.containsKey(stage.id())
                    ? externalSplitsByStage.get(stage.id()).size()
                    : resolvedShards.getOrDefault(stage.id(), Map.of()).size();
            };
            if (stage.placement() == StagedPlanner.StagePlacement.WORKERS && nodes == 0) {
                throw new IllegalStateException("shuffle worker stages require at least one data node");
            }
            nodeCounts.put(stage.id(), nodes);
        }
        int partitionsPerNode = budgetedPartitions(
            stagedPlan,
            nodeCounts,
            workers.size(),
            configuration.pragmas().shufflePartitionsPerNode(),
            configuration.pragmas().exchangeBufferSize(),
            configuration.pragmas().shuffleMaxBufferedPages()
        );
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
                    Map<DiscoveryNode, List<ExternalSplit>> externalSplits = externalSplitsByStage.get(stage.id());
                    if (externalSplits != null) {
                        int lane = 0;
                        for (DiscoveryNode node : sortedNodes(externalSplits.keySet())) {
                            lanesByNode.put(node, List.of(lane++));
                        }
                        yield lane;
                    }
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

    /** Fits a common partition count to per-producer-node, per-consumer-lane output queues before allocating the topology. */
    static int budgetedPartitions(
        StagedPlanner.StagedPlan plan,
        Map<Integer, Integer> nodeCounts,
        int workers,
        int requested,
        int bufferSize,
        int budget
    ) {
        if (requested < 1 || bufferSize < 1 || budget < 1) {
            throw new IllegalArgumentException("shuffle partitions, exchange buffer size, and buffer budget must be positive");
        }
        Map<Integer, StagedPlanner.StagePlacement> placements = new HashMap<>();
        plan.stages().forEach(stage -> placements.put(stage.id(), stage.placement()));
        long fixedPages = 0;
        long pagesPerPartition = 0;
        // Saturate before multiplying by requested parallelism, which can exceed the budget by many orders of magnitude.
        long cap = (long) budget + 1;
        for (StagedPlanner.Edge edge : plan.edges()) {
            long producerNodes = 0;
            for (int producer : edge.producerStages()) {
                producerNodes = Math.min(cap, producerNodes + nodeCounts.get(producer));
            }
            long pages = Math.min(cap, producerNodes * nodeCounts.get(edge.consumerStage()));
            pages = Math.min(cap, pages * bufferSize);
            if (placements.get(edge.consumerStage()) == StagedPlanner.StagePlacement.WORKERS
                && edge.distribution() != ExchangeDistribution.BROADCAST) {
                pagesPerPartition = Math.min(cap, pagesPerPartition + pages);
            } else {
                fixedPages = Math.min(cap, fixedPages + pages);
            }
        }
        long minimum = fixedPages + pagesPerPartition;
        if (minimum > budget) {
            throw new IllegalArgumentException(
                "shuffle requires at least ["
                    + minimum
                    + "] buffered exchange pages with one partition per data node, exceeding ["
                    + QueryPragmas.SHUFFLE_MAX_BUFFERED_PAGES.getKey()
                    + "]=["
                    + budget
                    + "]; reduce ["
                    + QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey()
                    + "] or increase the budget"
            );
        }
        int partitions = Math.min(requested, Integer.MAX_VALUE / Math.max(1, workers));
        return pagesPerPartition == 0 ? partitions : (int) Math.min(partitions, (budget - fixedPages) / pagesPerPartition);
    }

    private static void validateWorkerHashInputs(StagedPlanner.StagedPlan stagedPlan, Topology topology) {
        for (StagedPlanner.Stage stage : stagedPlan.stages()) {
            if (stage.placement() != StagedPlanner.StagePlacement.WORKERS) {
                continue;
            }
            for (String edgeKey : stage.inputEdges()) {
                StagedPlanner.Edge edge = stagedPlan.edge(edgeKey);
                if (edge.distribution() != ExchangeDistribution.HASH && edge.distribution() != ExchangeDistribution.BROADCAST) {
                    throw new IllegalStateException("bug: WORKERS stage [" + stage.id() + "] has an unsupported input edge");
                }
                if (edge.distribution() == ExchangeDistribution.BROADCAST) {
                    continue;
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
        List<OpenExchange> exchangesToOpen,
        ActionListener<Void> listener
    ) {
        for (StagedPlanner.Edge edge : stagedPlan.edges()) {
            int consumerLanes = consumerCount(topology, edge);
            for (int producerStageId : edge.producerStages()) {
                StageLocation producer = stageLocation(topology, producerStageId);
                for (DiscoveryNode producerNode : sortedNodes(producer.lanesByNode().keySet())) {
                    for (int consumerLane = 0; consumerLane < consumerLanes; consumerLane++) {
                        exchangesToOpen.add(
                            new OpenExchange(
                                producerNode,
                                StageWiring.exchangeId(rootSessionId, edge.edgeKey(), producerStageId, consumerLane)
                            )
                        );
                    }
                }
            }
        }
        // Wait for every open response so cleanup cannot race a late successful open.
        try (RefCountingListener opens = new RefCountingListener(listener)) {
            for (OpenExchange exchange : exchangesToOpen) {
                ActionListener.run(
                    opens.acquire(),
                    l -> ExchangeService.openExchange(
                        transportService,
                        transportService.getConnection(exchange.producerNode()),
                        exchange.exchangeId(),
                        configuration.pragmas().exchangeBufferSize(),
                        searchExecutor,
                        l
                    )
                );
            }
        }
    }

    private void closeExchangeSinks(List<OpenExchange> exchanges, ActionListener<Void> listener) {
        try (RefCountingListener closes = new RefCountingListener(listener)) {
            for (OpenExchange exchange : exchanges) {
                ActionListener.run(closes.acquire(), l -> {
                    Transport.Connection connection = transportService.getConnection(exchange.producerNode());
                    if (connection.getTransportVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION)) {
                        ExchangeService.closeExchange(transportService, connection, exchange.exchangeId(), searchExecutor, l);
                    } else {
                        l.onResponse(null);
                    }
                });
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
        Map<Integer, Map<DiscoveryNode, List<ExternalSplit>>> externalSplitsByStage,
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

        scheduleStages(stagedPlan, rootTask, searchExecutor, threadContext, (stage, stageListener) -> {
            ActionListener<DriverCompletionInfo> completion = computeListener.acquireCompute();
            try (
                ComputeListener stageComputes = new ComputeListener(
                    computeService.cancelQueryOnFailure(rootTask),
                    ActionListener.wrap(info -> {
                        completion.onResponse(info);
                        stageListener.onResponse(null);
                    }, failure -> {
                        completion.onFailure(failure);
                        stageListener.onFailure(failure);
                    })
                )
            ) {
                startStage(
                    rootSessionId,
                    rootTask,
                    stagedPlan,
                    topology,
                    shardStageInfos,
                    externalSplitsByStage,
                    configuration,
                    stage,
                    stageComputes
                );
            }
        }, computeListener.acquireAvoid());
    }

    private void startStage(
        String rootSessionId,
        CancellableTask rootTask,
        StagedPlanner.StagedPlan stagedPlan,
        Topology topology,
        Map<Integer, ShardStageInfo> shardStageInfos,
        Map<Integer, Map<DiscoveryNode, List<ExternalSplit>>> externalSplitsByStage,
        Configuration configuration,
        StagedPlanner.Stage stage,
        ComputeListener computeListener
    ) {
        try {
            StageLocation location = stageLocation(topology, stage.id());
            for (Map.Entry<DiscoveryNode, List<Integer>> entry : sortedLaneEntries(location)) {
                DiscoveryNode node = entry.getKey();
                List<Integer> localLanes = entry.getValue();
                StageWiring wiring = stageWiring(stagedPlan, topology, rootSessionId, stage, localLanes);
                List<DataNodeRequest.Shard> shards = List.of();
                Map<Index, AliasFilter> aliasFilters = Map.of();
                String[] indices = topology.sharedIndices().toArray(String[]::new);
                IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
                List<ExternalSplit> externalSplits = List.of();
                if (stage.placement() == StagedPlanner.StagePlacement.SHARDS) {
                    Map<DiscoveryNode, List<ExternalSplit>> stageExternalSplits = externalSplitsByStage.get(stage.id());
                    if (stageExternalSplits != null) {
                        externalSplits = stageExternalSplits.getOrDefault(node, List.of());
                        indices = new String[0];
                    } else {
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
                    externalSplits,
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
        } catch (Exception e) {
            computeListener.acquireAvoid().onFailure(e);
        }
    }

    /**
     * Keeps build consumers runnable while delaying the producers feeding their probes. Completion includes draining output exchanges.
     */
    static void scheduleStages(
        StagedPlanner.StagedPlan plan,
        CancellableTask task,
        Executor executor,
        ThreadContext threadContext,
        BiConsumer<StagedPlanner.Stage, ActionListener<Void>> start,
        ActionListener<Void> listener
    ) {
        Map<Integer, Set<Integer>> dependencies = new HashMap<>();
        Map<Integer, SubscribableListener<Void>> ready = new LinkedHashMap<>();
        Map<Integer, SubscribableListener<Void>> completed = new LinkedHashMap<>();
        for (StagedPlanner.Stage stage : plan.stages()) {
            if (stage.placement() != StagedPlanner.StagePlacement.COORDINATOR) {
                ready.put(stage.id(), new SubscribableListener<>());
                completed.put(stage.id(), new SubscribableListener<>());
            }
            for (DistributedHashJoinExec join : stage.plan().collect(DistributedHashJoinExec.class)) {
                Set<Integer> builds = new HashSet<>();
                for (StageSourceExec source : join.right().collect(StageSourceExec.class)) {
                    builds.addAll(plan.edge(source.edgeKey()).producerStages());
                }
                for (StageSourceExec source : join.left().collect(StageSourceExec.class)) {
                    for (int probe : plan.edge(source.edgeKey()).producerStages()) {
                        dependencies.computeIfAbsent(probe, ignored -> new HashSet<>()).addAll(builds);
                    }
                }
            }
        }
        AtomicBoolean aborted = new AtomicBoolean();
        Consumer<Exception> abort = failure -> {
            if (aborted.compareAndSet(false, true)) {
                ready.values().forEach(stage -> stage.onFailure(failure));
            }
        };
        task.addListener(() -> abort.accept(task.getTaskCancelledException()));
        try (RefCountingListener all = new RefCountingListener(listener)) {
            for (StagedPlanner.Stage stage : plan.stages()) {
                if (stage.placement() == StagedPlanner.StagePlacement.COORDINATOR) {
                    continue;
                }
                SubscribableListener<Void> completion = completed.get(stage.id());
                completion.addListener(all.acquire().delegateResponse((l, e) -> {
                    abort.accept(e);
                    l.onFailure(e);
                }));
                ready.get(stage.id()).addListener(ActionListener.wrap(ignored -> ActionListener.run(completion, l -> {
                    task.ensureNotCancelled();
                    start.accept(stage, l);
                }), completion::onFailure), executor, threadContext);
            }
            for (var entry : ready.entrySet()) {
                try (RefCountingListener prerequisites = new RefCountingListener(entry.getValue())) {
                    for (int dependency : dependencies.getOrDefault(entry.getKey(), Set.of())) {
                        completed.get(dependency).addListener(prerequisites.acquire());
                    }
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
                for (int producerStageId : edge.producerStages()) {
                    for (String producerNodeId : nodeIds(topology, producerStageId)) {
                        DiscoveryNode producerNode = clusterService.state().nodes().get(producerNodeId);
                        if (producerNode == null) {
                            throw new IllegalStateException(
                                "producer node [" + producerNodeId + "] for exchange [" + edgeKey + "] is missing"
                            );
                        }
                        source.addRemoteSink(
                            exchangeService.newRemoteSink(
                                rootTask,
                                StageWiring.exchangeId(rootSessionId, edgeKey, producerStageId, 0),
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
            inputs.add(
                new StageWiring.Input(
                    edgeKey,
                    edge.producerStages()
                        .stream()
                        .map(producerStageId -> new StageWiring.Producer(producerStageId, nodeIds(topology, producerStageId)))
                        .toList()
                )
            );
        }
        if (stage.outputEdge() == null) {
            throw new IllegalStateException("bug: non-coordinator stage [" + stage.id() + "] has no output edge");
        }
        StagedPlanner.Edge outputEdge = stagedPlan.edge(stage.outputEdge());
        int consumerLanes = consumerCount(topology, outputEdge);
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

    private static int consumerCount(Topology topology, StagedPlanner.Edge edge) {
        StageLocation consumer = stageLocation(topology, edge.consumerStage());
        return edge.distribution() == ExchangeDistribution.BROADCAST ? consumer.lanesByNode().size() : consumer.laneCount();
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
