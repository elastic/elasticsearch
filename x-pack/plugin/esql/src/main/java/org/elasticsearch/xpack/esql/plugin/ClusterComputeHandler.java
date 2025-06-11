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
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages computes across multiple clusters by sending {@link ClusterComputeRequest} to remote clusters and executing the computes.
 * This handler delegates the execution of computes on data nodes within each remote cluster to {@link DataNodeComputeHandler}.
 */
final class ClusterComputeHandler implements TransportRequestHandler<ClusterComputeRequest> {
    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final TransportService transportService;
    private final Executor esqlExecutor;
    private final DataNodeComputeHandler dataNodeComputeHandler;

    ClusterComputeHandler(
        ComputeService computeService,
        ExchangeService exchangeService,
        TransportService transportService,
        Executor esqlExecutor,
        DataNodeComputeHandler dataNodeComputeHandler
    ) {
        this.computeService = computeService;
        this.exchangeService = exchangeService;
        this.esqlExecutor = esqlExecutor;
        this.transportService = transportService;
        this.dataNodeComputeHandler = dataNodeComputeHandler;
        transportService.registerRequestHandler(ComputeService.CLUSTER_ACTION_NAME, esqlExecutor, ClusterComputeRequest::new, this);
    }

    void startComputeOnRemoteCluster(
        String sessionId,
        CancellableTask rootTask,
        Configuration configuration,
        PhysicalPlan plan,
        ExchangeSourceHandler exchangeSource,
        RemoteCluster cluster,
        Runnable cancelQueryOnFailure,
        EsqlExecutionInfo executionInfo,
        ActionListener<DriverCompletionInfo> listener
    ) {
        var queryPragmas = configuration.pragmas();
        listener = ActionListener.runBefore(listener, exchangeSource.addEmptySink()::close);
        final var childSessionId = computeService.newChildSession(sessionId);
        final String clusterAlias = cluster.clusterAlias();
        final AtomicBoolean pagesFetched = new AtomicBoolean();
        final AtomicReference<ComputeResponse> finalResponse = new AtomicReference<>();
        listener = listener.delegateResponse((l, e) -> {
            final boolean receivedResults = finalResponse.get() != null || pagesFetched.get();
            if (EsqlCCSUtils.shouldIgnoreRuntimeError(executionInfo, clusterAlias, e)
                || (configuration.allowPartialResults() && EsqlCCSUtils.canAllowPartial(e))) {
                EsqlCCSUtils.markClusterWithFinalStateAndNoShards(
                    executionInfo,
                    clusterAlias,
                    receivedResults ? EsqlExecutionInfo.Cluster.Status.PARTIAL : EsqlExecutionInfo.Cluster.Status.SKIPPED,
                    e
                );
                l.onResponse(DriverCompletionInfo.EMPTY);
            } else {
                l.onFailure(e);
            }
        });
        ExchangeService.openExchange(
            transportService,
            cluster.connection,
            childSessionId,
            queryPragmas.exchangeBufferSize(),
            esqlExecutor,
            listener.delegateFailure((l, unused) -> {
                final CancellableTask groupTask;
                final Runnable onGroupFailure;
                boolean failFast = executionInfo.isSkipUnavailable(clusterAlias) == false && configuration.allowPartialResults() == false;
                if (failFast) {
                    groupTask = rootTask;
                    onGroupFailure = cancelQueryOnFailure;
                } else {
                    try {
                        groupTask = computeService.createGroupTask(rootTask, () -> "compute group: cluster [" + clusterAlias + "]");
                    } catch (TaskCancelledException e) {
                        l.onFailure(e);
                        return;
                    }
                    onGroupFailure = computeService.cancelQueryOnFailure(groupTask);
                    l = ActionListener.runAfter(l, () -> transportService.getTaskManager().unregister(groupTask));
                }
                try (var computeListener = new ComputeListener(transportService.getThreadPool(), onGroupFailure, l.map(completionInfo -> {
                    updateExecutionInfo(executionInfo, clusterAlias, finalResponse.get());
                    return completionInfo;
                }))) {
                    var remotePlan = new RemoteClusterPlan(plan, cluster.concreteIndices, cluster.originalIndices);
                    var clusterRequest = new ClusterComputeRequest(clusterAlias, childSessionId, configuration, remotePlan);
                    final ActionListener<ComputeResponse> clusterListener = computeListener.acquireCompute().map(r -> {
                        finalResponse.set(r);
                        return r.getCompletionInfo();
                    });
                    transportService.sendChildRequest(
                        cluster.connection,
                        ComputeService.CLUSTER_ACTION_NAME,
                        clusterRequest,
                        groupTask,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(clusterListener, ComputeResponse::new, esqlExecutor)
                    );
                    var remoteSink = exchangeService.newRemoteSink(groupTask, childSessionId, transportService, cluster.connection);
                    exchangeSource.addRemoteSink(
                        remoteSink,
                        failFast,
                        () -> pagesFetched.set(true),
                        queryPragmas.concurrentExchangeClients(),
                        computeListener.acquireAvoid()
                    );
                }
            })
        );
    }

    private void updateExecutionInfo(EsqlExecutionInfo executionInfo, String clusterAlias, ComputeResponse resp) {
        executionInfo.swapCluster(clusterAlias, (k, v) -> {
            var builder = new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(resp.getTotalShards())
                .setSuccessfulShards(resp.getSuccessfulShards())
                .setSkippedShards(resp.getSkippedShards())
                .setFailedShards(resp.getFailedShards());
            if (resp.getTook() != null) {
                builder.setTook(TimeValue.timeValueNanos(executionInfo.planningTookTime().nanos() + resp.getTook().nanos()));
            } else {
                // if the cluster is an older version and does not send back took time, then calculate it here on the coordinator
                // and leave shard info unset, so it is not shown in the CCS metadata section of the JSON response
                builder.setTook(executionInfo.tookSoFar());
            }
            if (v.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                builder.setFailures(resp.failures);
                if (executionInfo.isStopped() || resp.failedShards > 0 || resp.failures.isEmpty() == false) {
                    builder.setStatus(EsqlExecutionInfo.Cluster.Status.PARTIAL);
                } else {
                    builder.setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                }
            }
            return builder.build();
        });
    }

    List<RemoteCluster> getRemoteClusters(
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

    record RemoteCluster(String clusterAlias, Transport.Connection connection, String[] concreteIndices, OriginalIndices originalIndices) {

    }

    @Override
    public void messageReceived(ClusterComputeRequest request, TransportChannel channel, Task task) {
        ChannelActionListener<ComputeResponse> listener = new ChannelActionListener<>(channel);
        RemoteClusterPlan remoteClusterPlan = request.remoteClusterPlan();
        var plan = remoteClusterPlan.plan();
        if (plan instanceof ExchangeSinkExec == false) {
            listener.onFailure(new IllegalStateException("expected exchange sink for a remote compute; got " + plan));
            return;
        }
        runComputeOnRemoteCluster(
            request.clusterAlias(),
            request.sessionId(),
            (CancellableTask) task,
            request.configuration(),
            (ExchangeSinkExec) plan,
            Set.of(remoteClusterPlan.targetIndices()),
            remoteClusterPlan.originalIndices(),
            listener
        );
    }

    /**
     * Performs a compute on a remote cluster. The output pages are placed in an exchange sink specified by
     * {@code globalSessionId}. The coordinator on the main cluster will poll pages from there.
     * <p>
     * Currently, the coordinator on the remote cluster polls pages from data nodes within the remote cluster
     * and performs cluster-level reduction before sending pages to the querying cluster. This reduction aims
     * to minimize data transfers across clusters but may require additional CPU resources for operations like
     * aggregations.
     */
    void runComputeOnRemoteCluster(
        String clusterAlias,
        String globalSessionId,
        CancellableTask parentTask,
        Configuration configuration,
        ExchangeSinkExec plan,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        ActionListener<ComputeResponse> listener
    ) {
        final var exchangeSink = exchangeService.getSinkHandler(globalSessionId);
        parentTask.addListener(
            () -> exchangeService.finishSinkHandler(globalSessionId, new TaskCancelledException(parentTask.getReasonCancelled()))
        );
        final String localSessionId = clusterAlias + ":" + globalSessionId;
        final PhysicalPlan coordinatorPlan = ComputeService.reductionPlan(plan, true);
        final AtomicReference<ComputeResponse> finalResponse = new AtomicReference<>();
        final long startTimeInNanos = System.nanoTime();
        final Runnable cancelQueryOnFailure = computeService.cancelQueryOnFailure(parentTask);
        try (var computeListener = new ComputeListener(transportService.getThreadPool(), cancelQueryOnFailure, listener.map(profiles -> {
            final TimeValue took = TimeValue.timeValueNanos(System.nanoTime() - startTimeInNanos);
            final ComputeResponse r = finalResponse.get();
            return new ComputeResponse(profiles, took, r.totalShards, r.successfulShards, r.skippedShards, r.failedShards, r.failures);
        }))) {
            var exchangeSource = new ExchangeSourceHandler(
                configuration.pragmas().exchangeBufferSize(),
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
            );
            try (Releasable ignored = exchangeSource.addEmptySink()) {
                exchangeSink.addCompletionListener(computeListener.acquireAvoid());
                computeService.runCompute(
                    parentTask,
                    new ComputeContext(
                        localSessionId,
                        "remote_reduce",
                        clusterAlias,
                        List.of(),
                        configuration,
                        configuration.newFoldContext(),
                        exchangeSource::createExchangeSource,
                        () -> exchangeSink.createExchangeSink(() -> {})
                    ),
                    coordinatorPlan,
                    computeListener.acquireCompute()
                );
                dataNodeComputeHandler.startComputeOnDataNodes(
                    localSessionId,
                    clusterAlias,
                    parentTask,
                    configuration,
                    plan,
                    concreteIndices,
                    originalIndices,
                    exchangeSource,
                    cancelQueryOnFailure,
                    computeListener.acquireCompute().map(r -> {
                        finalResponse.set(r);
                        return r.getCompletionInfo();
                    })
                );
            }
        }
    }

}
