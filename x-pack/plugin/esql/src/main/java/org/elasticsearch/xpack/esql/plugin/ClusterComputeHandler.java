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
import org.elasticsearch.compute.EsqlRefCountingListener;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasable;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

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

    void startComputeOnRemoteClusters(
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
        try (EsqlRefCountingListener refs = new EsqlRefCountingListener(linkExchangeListeners)) {
            for (RemoteCluster cluster : clusters) {
                final var childSessionId = computeService.newChildSession(sessionId);
                ExchangeService.openExchange(
                    transportService,
                    cluster.connection,
                    childSessionId,
                    queryPragmas.exchangeBufferSize(),
                    esqlExecutor,
                    refs.acquire().delegateFailureAndWrap((l, unused) -> {
                        var remoteSink = exchangeService.newRemoteSink(rootTask, childSessionId, transportService, cluster.connection);
                        exchangeSource.addRemoteSink(remoteSink, true, queryPragmas.concurrentExchangeClients(), ActionListener.noop());
                        var remotePlan = new RemoteClusterPlan(plan, cluster.concreteIndices, cluster.originalIndices);
                        var clusterRequest = new ClusterComputeRequest(cluster.clusterAlias, childSessionId, configuration, remotePlan);
                        var clusterListener = ActionListener.runBefore(
                            computeListener.acquireCompute(cluster.clusterAlias()),
                            () -> l.onResponse(null)
                        );
                        transportService.sendChildRequest(
                            cluster.connection,
                            ComputeService.CLUSTER_ACTION_NAME,
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
        EsqlExecutionInfo executionInfo,
        ComputeListener computeListener
    ) {
        final var exchangeSink = exchangeService.getSinkHandler(globalSessionId);
        parentTask.addListener(
            () -> exchangeService.finishSinkHandler(globalSessionId, new TaskCancelledException(parentTask.getReasonCancelled()))
        );
        final String localSessionId = clusterAlias + ":" + globalSessionId;
        final PhysicalPlan coordinatorPlan = ComputeService.reductionPlan(plan, true);
        var exchangeSource = new ExchangeSourceHandler(
            configuration.pragmas().exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
            computeListener.acquireAvoid()
        );
        try (Releasable ignored = exchangeSource.addEmptySink()) {
            exchangeSink.addCompletionListener(computeListener.acquireAvoid());
            computeService.runCompute(
                parentTask,
                new ComputeContext(
                    localSessionId,
                    clusterAlias,
                    List.of(),
                    configuration,
                    configuration.newFoldContext(),
                    exchangeSource,
                    exchangeSink
                ),
                coordinatorPlan,
                computeListener.acquireCompute(clusterAlias)
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
                executionInfo,
                computeListener
            );
        }
    }

}
