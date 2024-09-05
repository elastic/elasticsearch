/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterSnapshotStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CancellableSingleObjectCache;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterConnection;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.usage.UsageService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

/**
 * Transport action implementing _cluster/stats API.
 */
public class TransportClusterStatsAction extends TransportClusterStatsBaseAction<ClusterStatsResponse> {

    public static final ActionType<ClusterStatsResponse> TYPE = new ActionType<>("cluster:monitor/stats");

    private final MetadataStatsCache<MappingStats> mappingStatsCache;
    private final MetadataStatsCache<AnalysisStats> analysisStatsCache;
    private final RemoteClusterService remoteClusterService;
    private static final Logger logger = LogManager.getLogger(TransportClusterStatsAction.class);
    private final Settings settings;

    @Inject
    public TransportClusterStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        IndicesService indicesService,
        RepositoriesService repositoriesService,
        UsageService usageService,
        ActionFilters actionFilters,
        Settings settings
    ) {
        super(
            TYPE.name(),
            threadPool,
            clusterService,
            transportService,
            nodeService,
            indicesService,
            repositoriesService,
            usageService,
            actionFilters
        );
        this.mappingStatsCache = new MetadataStatsCache<>(threadPool.getThreadContext(), MappingStats::of);
        this.analysisStatsCache = new MetadataStatsCache<>(threadPool.getThreadContext(), AnalysisStats::of);
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.settings = settings;
    }

    private ActionFuture<Map<String, RemoteClusterStatsResponse>> remoteFuture;

    @Override
    protected void doExecute(Task task, ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener) {
        remoteFuture = getStatsFromRemotes(request);
        super.doExecute(task, request, listener);
    }

    @Override
    protected void newResponseAsync(
        final Task task,
        final ClusterStatsRequest request,
        final List<ClusterStatsNodeResponse> responses,
        final List<FailedNodeException> failures,
        final ActionListener<ClusterStatsResponse> listener
    ) {
        assert Transports.assertNotTransportThread(
            "Computation of mapping/analysis stats runs expensive computations on mappings found in "
                + "the cluster state that are too slow for a transport thread"
        );
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
        assert task instanceof CancellableTask;
        final CancellableTask cancellableTask = (CancellableTask) task;
        final ClusterState state = clusterService.state();
        final Metadata metadata = state.metadata();
        final ClusterSnapshotStats clusterSnapshotStats = ClusterSnapshotStats.of(
            state,
            clusterService.threadPool().absoluteTimeInMillis()
        );

        // This will wait until remotes are done if it didn't happen yet
        var remoteClusterStats = getRemoteClusterStats(request);

        final ListenableFuture<MappingStats> mappingStatsStep = new ListenableFuture<>();
        final ListenableFuture<AnalysisStats> analysisStatsStep = new ListenableFuture<>();
        mappingStatsCache.get(metadata, cancellableTask::isCancelled, mappingStatsStep);
        analysisStatsCache.get(metadata, cancellableTask::isCancelled, analysisStatsStep);
        mappingStatsStep.addListener(
            listener.delegateFailureAndWrap(
                (l, mappingStats) -> analysisStatsStep.addListener(
                    l.delegateFailureAndWrap(
                        (ll, analysisStats) -> ActionListener.completeWith(
                            ll,
                            () -> new ClusterStatsResponse(
                                System.currentTimeMillis(),
                                metadata.clusterUUID(),
                                clusterService.getClusterName(),
                                responses,
                                failures,
                                mappingStats,
                                analysisStats,
                                VersionStats.of(metadata, responses),
                                clusterSnapshotStats,
                                remoteClusterStats
                            )
                        )
                    )
                )
            )
        );
    }

    private static class MetadataStatsCache<T> extends CancellableSingleObjectCache<Metadata, Long, T> {
        private final BiFunction<Metadata, Runnable, T> function;

        MetadataStatsCache(ThreadContext threadContext, BiFunction<Metadata, Runnable, T> function) {
            super(threadContext);
            this.function = function;
        }

        @Override
        protected void refresh(
            Metadata metadata,
            Runnable ensureNotCancelled,
            BooleanSupplier supersedeIfStale,
            ActionListener<T> listener
        ) {
            ActionListener.completeWith(listener, () -> function.apply(metadata, ensureNotCancelled));
        }

        @Override
        protected Long getKey(Metadata indexMetadata) {
            return indexMetadata.version();
        }

        @Override
        protected boolean isFresh(Long currentKey, Long newKey) {
            return newKey <= currentKey;
        }
    }

    private Map<String, ClusterStatsResponse.RemoteClusterStats> getRemoteClusterStats(ClusterStatsRequest request) {
        if (request.doRemotes() == false) {
            return null;
        }
        Map<String, ClusterStatsResponse.RemoteClusterStats> remoteClustersStats = new HashMap<>();
        Map<String, RemoteClusterStatsResponse> remoteData = resolveRemoteClusterStats();

        for (String clusterAlias : remoteClusterService.getRegisteredRemoteClusterNames()) {
            RemoteClusterConnection remoteConnection = remoteClusterService.getRemoteClusterConnection(clusterAlias);
            RemoteConnectionInfo remoteConnectionInfo = remoteConnection.getConnectionInfo();
            RemoteClusterStatsResponse response = remoteData.get(clusterAlias);
            var compression = RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterAlias).get(settings);
            var remoteClusterStats = new ClusterStatsResponse.RemoteClusterStats(
                response,
                remoteConnectionInfo.getModeInfo().modeName(),
                remoteConnection.isSkipUnavailable(),
                compression.toString()
            );
            remoteClustersStats.put(clusterAlias, remoteClusterStats);
        }
        return remoteClustersStats;
    }

    private Map<String, RemoteClusterStatsResponse> resolveRemoteClusterStats() {
        try {
            return remoteFuture.actionGet();
        } catch (ElasticsearchException e) {
            logger.warn("Failed to get remote cluster stats", e);
            return Map.of();
        }
    }

    private ActionFuture<Map<String, RemoteClusterStatsResponse>> getStatsFromRemotes(ClusterStatsRequest request) {
        if (request.doRemotes() == false) {
            // this will never be used since getRemoteClusterStats has the same check
            return null;
        }

        // TODO: make correct pool
        final var remoteClientResponseExecutor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);
        var remotes = remoteClusterService.getRegisteredRemoteClusterNames();

        var remotesFuture = new PlainActionFuture<Map<String, RemoteClusterStatsResponse>>();
        var groupListener = new RemoteClusterActionListener<>(remotes.size(), remotesFuture);

        for (String clusterAlias : remotes) {
            ClusterStatsRequest remoteRequest = request.subRequest();
            var remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                clusterAlias,
                remoteClientResponseExecutor,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
            );
            remoteClusterClient.execute(
                TransportRemoteClusterStatsAction.REMOTE_TYPE,
                remoteRequest,
                groupListener.remoteListener(clusterAlias)
            );

        }

        return remotesFuture;
    }
}
