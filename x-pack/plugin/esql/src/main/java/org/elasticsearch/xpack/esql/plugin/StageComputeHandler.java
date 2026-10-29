/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.ExchangeWiring;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Runs one stage lane and links its inputs and output to the pre-opened shuffle exchanges.
 */
final class StageComputeHandler {
    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Executor searchExecutor;

    StageComputeHandler(
        ComputeService computeService,
        ExchangeService exchangeService,
        TransportService transportService,
        ClusterService clusterService,
        Executor searchExecutor
    ) {
        this.computeService = computeService;
        this.exchangeService = exchangeService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.searchExecutor = searchExecutor;
    }

    void runStage(
        CancellableTask task,
        DataNodeRequest request,
        PhysicalPlan stagePlan,
        @Nullable Supplier<ExchangeSource> dataSource,
        IndexedByShardId<ComputeSearchContext> searchContexts,
        ActionListener<DriverCompletionInfo> listener
    ) {
        StageWiring wiring = request.stageWiring();
        if (wiring == null) {
            listener.onFailure(new IllegalArgumentException("stage wiring is required to run a stage"));
            return;
        }

        StageWiring.Output output = wiring.output();
        List<String> outputIds = new ArrayList<>(output.consumerLanes());
        List<ExchangeSinkHandler> outputHandlers = new ArrayList<>(output.consumerLanes());
        try {
            for (int consumerLane = 0; consumerLane < output.consumerLanes(); consumerLane++) {
                String exchangeId = StageWiring.exchangeId(
                    wiring.rootSessionId(),
                    output.edgeKey(),
                    wiring.stageId(),
                    consumerLane,
                    wiring.stageQualifiedExchangeIds()
                );
                outputIds.add(exchangeId);
                outputHandlers.add(exchangeService.getSinkHandler(exchangeId));
            }
        } catch (Exception e) {
            finishSinkHandlers(outputIds, e);
            listener.onFailure(e);
            return;
        }

        List<ExchangeSink> keepAlive = new ArrayList<>(outputHandlers.size());
        try {
            for (ExchangeSinkHandler handler : outputHandlers) {
                keepAlive.add(handler.createExchangeSink(() -> {}));
            }
        } catch (Exception e) {
            keepAlive.forEach(ExchangeSink::finish);
            finishSinkHandlers(outputIds, e);
            listener.onFailure(e);
            return;
        }

        task.addListener(() -> finishSinkHandlers(outputIds, new TaskCancelledException(task.getReasonCancelled())));

        ActionListener<DriverCompletionInfo> afterComputes = ActionListener.wrap(
            info -> awaitOutputCompletion(outputIds, outputHandlers, keepAlive, info, listener),
            e -> failOutputs(outputIds, keepAlive, e, listener, new AtomicBoolean())
        );
        try (ComputeListener computeListener = new ComputeListener(computeService.cancelQueryOnFailure(task), afterComputes)) {
            try {
                for (int lane : wiring.localLanes()) {
                    Map<String, Supplier<ExchangeSource>> sourcesByEdge = new HashMap<>();
                    for (StageWiring.Input input : wiring.inputs()) {
                        ExchangeSourceHandler source = new ExchangeSourceHandler(request.pragmas().exchangeBufferSize(), searchExecutor);
                        try (Releasable hold = source.addEmptySink()) {
                            for (StageWiring.Producer producer : input.producers()) {
                                for (String producerNodeId : producer.nodeIds()) {
                                    DiscoveryNode producerNode = clusterService.state().nodes().get(producerNodeId);
                                    if (producerNode == null) {
                                        throw new IllegalStateException(
                                            "producer node [" + producerNodeId + "] for exchange [" + input.edgeKey() + "] is missing"
                                        );
                                    }
                                    source.addRemoteSink(
                                        exchangeService.newRemoteSink(
                                            task,
                                            StageWiring.exchangeId(
                                                wiring.rootSessionId(),
                                                input.edgeKey(),
                                                producer.stageId(),
                                                lane,
                                                wiring.stageQualifiedExchangeIds()
                                            ),
                                            transportService,
                                            transportService.getConnection(producerNode)
                                        ),
                                        true,
                                        () -> {},
                                        request.pragmas().concurrentExchangeClients(),
                                        computeListener.acquireAvoid()
                                    );
                                }
                            }
                        }
                        if (sourcesByEdge.put(input.edgeKey(), source::createExchangeSource) != null) {
                            throw new IllegalStateException("duplicate input edge [" + input.edgeKey() + "]");
                        }
                    }

                    ExchangeWiring exchangeWiring = new ExchangeWiring(sourcesByEdge, Map.of(output.edgeKey(), outputHandlers));
                    ComputeContext context = new ComputeContext(
                        request.sessionId() + "/l" + lane,
                        "stage" + wiring.stageId() + ".lane" + lane,
                        request.clusterAlias(),
                        computeService.createFlags(),
                        searchContexts,
                        request.configuration(),
                        new FoldContext(request.pragmas().foldLimit().getBytes()),
                        dataSource,
                        null,
                        false,
                        false,
                        exchangeWiring
                    );
                    var laneListener = computeListener.acquireCompute();
                    try {
                        computeService.runCompute(
                            task,
                            context,
                            stagePlan,
                            computeService.plannerSettings().get(),
                            LocalPhysicalOptimization.DISABLED,
                            null,
                            laneListener
                        );
                    } catch (Exception e) {
                        laneListener.onFailure(e);
                        break;
                    }
                }
            } catch (Exception e) {
                computeListener.acquireAvoid().onFailure(e);
            }
        }
    }

    private void awaitOutputCompletion(
        List<String> outputIds,
        List<ExchangeSinkHandler> outputHandlers,
        List<ExchangeSink> keepAlive,
        DriverCompletionInfo info,
        ActionListener<DriverCompletionInfo> listener
    ) {
        keepAlive.forEach(ExchangeSink::finish);
        AtomicInteger remainingHandlers = new AtomicInteger(outputHandlers.size());
        AtomicBoolean completed = new AtomicBoolean();
        if (outputHandlers.isEmpty()) {
            if (completed.compareAndSet(false, true)) {
                finishSinkHandlers(outputIds, null);
                listener.onResponse(info);
            }
            return;
        }
        for (ExchangeSinkHandler handler : outputHandlers) {
            handler.addCompletionListener(ActionListener.wrap(ignored -> {
                if (remainingHandlers.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
                    finishSinkHandlers(outputIds, null);
                    listener.onResponse(info);
                }
            }, e -> failOutputs(outputIds, keepAlive, e, listener, completed)));
        }
    }

    private void failOutputs(
        List<String> outputIds,
        List<ExchangeSink> keepAlive,
        Exception failure,
        ActionListener<DriverCompletionInfo> listener,
        AtomicBoolean completed
    ) {
        if (completed.compareAndSet(false, true)) {
            keepAlive.forEach(ExchangeSink::finish);
            finishSinkHandlers(outputIds, failure);
            listener.onFailure(failure);
        }
    }

    private void finishSinkHandlers(List<String> outputIds, @Nullable Exception failure) {
        for (String outputId : outputIds) {
            exchangeService.finishSinkHandler(outputId, failure);
        }
    }
}
