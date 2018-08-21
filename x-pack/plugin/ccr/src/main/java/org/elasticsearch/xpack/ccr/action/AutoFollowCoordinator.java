/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A component that runs only on the elected master node and follows leader indices automatically
 * if they match with a auto follow pattern that is defined in {@link AutoFollowMetadata}.
 */
public class AutoFollowCoordinator implements ClusterStateApplier {

    private static final Logger LOGGER = LogManager.getLogger(AutoFollowCoordinator.class);

    private final Client client;
    private final TimeValue pollInterval;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    private volatile boolean localNodeMaster = false;

    public AutoFollowCoordinator(Settings settings,
                                 Client client,
                                 ThreadPool threadPool,
                                 ClusterService clusterService) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;

        this.pollInterval = CcrSettings.CCR_AUTO_FOLLOW_POLL_INTERVAL.get(settings);
        clusterService.addStateApplier(this);
    }

    void doAutoFollow() {
        if (localNodeMaster == false) {
            return;
        }
        ClusterState localClusterState = clusterService.state();
        AutoFollowMetadata autoFollowMetadata = localClusterState.getMetaData().custom(AutoFollowMetadata.TYPE);
        if (autoFollowMetadata == null) {
            threadPool.schedule(pollInterval, ThreadPool.Names.SAME, this::doAutoFollow);
            return;
        }

        if (autoFollowMetadata.getPatterns().isEmpty()) {
            threadPool.schedule(pollInterval, ThreadPool.Names.SAME, this::doAutoFollow);
            return;
        }

        Consumer<Exception> handler = e -> {
            if (e != null) {
                LOGGER.error("Failure occurred during auto following indices", e);
            }
            threadPool.schedule(pollInterval, ThreadPool.Names.SAME, this::doAutoFollow);
        };
        AutoFollower operation = new AutoFollower(client, handler, autoFollowMetadata) {

            void clusterStateApiCall(Client remoteClient, BiConsumer<ClusterState, Exception> handler) {
                ClusterStateRequest request = new ClusterStateRequest();
                request.clear();
                request.metaData(true);
                remoteClient.admin().cluster().state(request,
                    ActionListener.wrap(
                        r -> handler.accept(r.getState(), null),
                        e -> handler.accept(null, e)
                    )
                );
            }

            void createAndFollowApiCall(FollowIndexAction.Request followRequest, Consumer<Exception> handler) {
                client.execute(CreateAndFollowIndexAction.INSTANCE, new CreateAndFollowIndexAction.Request(followRequest),
                    ActionListener.wrap(r -> handler.accept(null), handler::accept));
            }

            void updateAutoMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                clusterService.submitStateUpdateTask("update_auto_follow_metadata", new ClusterStateUpdateTask() {

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return updateFunction.apply(currentState);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        handler.accept(e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        handler.accept(null);
                    }
                });
            }

        };
        operation.autoFollowIndices();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        final boolean beforeLocalMasterNode = localNodeMaster;
        localNodeMaster = event.localNodeMaster();
        if (beforeLocalMasterNode == false && localNodeMaster) {
            threadPool.schedule(pollInterval, ThreadPool.Names.SAME, this::doAutoFollow);
        }
    }

    abstract static class AutoFollower {

        private final Client client;
        private final Consumer<Exception> handler;
        private final AutoFollowMetadata autoFollowMetadata;

        private final AtomicInteger executedRequests = new AtomicInteger(0);
        private final AtomicReference<Exception> errorHolder = new AtomicReference<>();

        AutoFollower(Client client, Consumer<Exception> handler, AutoFollowMetadata autoFollowMetadata) {
            this.client = client;
            this.handler = handler;
            this.autoFollowMetadata = autoFollowMetadata;
        }

        void autoFollowIndices() {
            for (Map.Entry<String, AutoFollowMetadata.AutoFollowPattern> entry : autoFollowMetadata.getPatterns().entrySet()) {
                String clusterAlias = entry.getKey();
                AutoFollowMetadata.AutoFollowPattern autoFollowPattern = entry.getValue();
                Client remoteClient = clusterAlias.equals("_local_") ? client : client.getRemoteClusterClient(clusterAlias);
                List<String> followedIndices = autoFollowMetadata.getFollowedLeaderIndexUUIDS().get(clusterAlias);

                clusterStateApiCall(remoteClient, (remoteClusterState, e) -> {
                    if (remoteClusterState != null) {
                        handleClusterAlias(clusterAlias, autoFollowPattern, followedIndices, remoteClusterState);
                    } else {
                        finalise(e);
                    }
                });
            }
        }

        private void handleClusterAlias(String clusterAlias, AutoFollowMetadata.AutoFollowPattern autoFollowPattern,
                                List<String> followedIndexUUIDS, ClusterState remoteClusterState) {
            List<IndexMetaData> leaderIndicesToFollow = new ArrayList<>();
            String[] patterns = autoFollowPattern.getLeaderIndexPatterns().toArray(new String[0]);
            for (IndexMetaData indexMetaData : remoteClusterState.getMetaData()) {
                if (Regex.simpleMatch(patterns, indexMetaData.getIndex().getName())) {
                    if (followedIndexUUIDS.contains(indexMetaData.getIndex().getUUID()) == false) {
                        leaderIndicesToFollow.add(indexMetaData);
                    }
                }
            }
            if (leaderIndicesToFollow.isEmpty()) {
                finalise(null);
            } else {
                AtomicInteger numRequests = new AtomicInteger();
                for (IndexMetaData indexToFollow : leaderIndicesToFollow) {
                    String leaderIndexName = indexToFollow.getIndex().getName();
                    String followIndexName = leaderIndexName;
                    if (autoFollowPattern.getFollowIndexPattern() != null) {
                        followIndexName = autoFollowPattern.getFollowIndexPattern().replace("{{leader_index}}", leaderIndexName);
                    }

                    String leaderIndexNameWithClusterAliasPrefix = clusterAlias.equals("_local_") ? leaderIndexName :
                        clusterAlias + ":" + leaderIndexName;
                    FollowIndexAction.Request followRequest =
                        new FollowIndexAction.Request(leaderIndexNameWithClusterAliasPrefix, followIndexName,
                            autoFollowPattern.getMaxBatchOperationCount(), autoFollowPattern.getMaxConcurrentReadBatches(),
                            autoFollowPattern.getMaxOperationSizeInBytes(), autoFollowPattern.getMaxConcurrentWriteBatches(),
                            autoFollowPattern.getMaxWriteBufferSize(), autoFollowPattern.getRetryTimeout(),
                            autoFollowPattern.getIdleShardRetryDelay());

                    // This runs on the elected master node, so we can update cluster state here:
                    Consumer<Exception> handler = followError -> {
                        if (followError != null) {
                            LOGGER.error("Failed to auto follow leader index [" + leaderIndexName + "]", followError);
                            if (numRequests.incrementAndGet() == leaderIndicesToFollow.size()) {
                                finalise(followError);
                            }
                            return;
                        }
                        Function<ClusterState, ClusterState> clusterStateUpdateFunction = currentState -> {
                            AutoFollowMetadata currentAutoFollowMetadata = currentState.metaData().custom(AutoFollowMetadata.TYPE);

                            Map<String, List<String>> newFollowedIndexUUIDS =
                                new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDS());
                            newFollowedIndexUUIDS.get(clusterAlias).add(indexToFollow.getIndexUUID());

                            ClusterState.Builder newState = ClusterState.builder(currentState);
                            AutoFollowMetadata newAutoFollowMetadata =
                                new AutoFollowMetadata(currentAutoFollowMetadata.getPatterns(), newFollowedIndexUUIDS);
                            newState.metaData(MetaData.builder(currentState.getMetaData())
                                .putCustom(AutoFollowMetadata.TYPE, newAutoFollowMetadata)
                                .build());
                            return newState.build();
                        };
                        updateAutoMetadata(clusterStateUpdateFunction, updateError -> {
                            if (updateError != null) {
                                LOGGER.error("Failed to mark leader index [" + leaderIndexName + "] as auto followed", updateError);
                            } else {
                                LOGGER.debug("Successfully marked leader index [{}] as auto followed", leaderIndexName);
                            }
                            if (numRequests.incrementAndGet() == leaderIndicesToFollow.size()) {
                                finalise(updateError);
                            }
                        });
                    };
                    LOGGER.info("Auto following leader index [{}] as follow index [{}]", leaderIndexName, followIndexName);
                    createAndFollowApiCall(followRequest, handler);
                }
            }
        }

        private void finalise(Exception failure) {
            if (errorHolder.compareAndSet(null, failure) == false) {
                errorHolder.get().addSuppressed(failure);
            }

            if (executedRequests.incrementAndGet() == autoFollowMetadata.getPatterns().size()) {
                handler.accept(errorHolder.get());
            }
        }

        // abstract methods to make unit testing possible:

        abstract void clusterStateApiCall(Client remoteClient, BiConsumer<ClusterState, Exception> handler);

        abstract void createAndFollowApiCall(FollowIndexAction.Request followRequest, Consumer<Exception> handler);

        abstract void updateAutoMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler);

    }
}
