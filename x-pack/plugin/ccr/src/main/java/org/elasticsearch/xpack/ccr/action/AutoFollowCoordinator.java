/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A component that runs only on the elected master node and follows leader indices automatically
 * if they match with a auto follow pattern that is defined in {@link AutoFollowMetadata}.
 */
public class AutoFollowCoordinator implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(AutoFollowCoordinator.class);
    private static final int MAX_AUTO_FOLLOW_ERRORS = 256;

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final CcrLicenseChecker ccrLicenseChecker;

    private volatile Map<String, AutoFollower> autoFollowers = Collections.emptyMap();

    // The following fields are read and updated under a lock:
    private long numberOfSuccessfulIndicesAutoFollowed = 0;
    private long numberOfFailedIndicesAutoFollowed = 0;
    private long numberOfFailedRemoteClusterStateRequests = 0;
    private final LinkedHashMap<String, ElasticsearchException> recentAutoFollowErrors;

    public AutoFollowCoordinator(
            Client client,
            ThreadPool threadPool,
            ClusterService clusterService,
            CcrLicenseChecker ccrLicenseChecker) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker, "ccrLicenseChecker");
        clusterService.addListener(this);
        this.recentAutoFollowErrors = new LinkedHashMap<String, ElasticsearchException>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<String, ElasticsearchException> eldest) {
                return size() > MAX_AUTO_FOLLOW_ERRORS;
            }
        };
    }

    public synchronized AutoFollowStats getStats() {
        return new AutoFollowStats(
            numberOfFailedIndicesAutoFollowed,
            numberOfFailedRemoteClusterStateRequests,
            numberOfSuccessfulIndicesAutoFollowed,
            new TreeMap<>(recentAutoFollowErrors)
        );
    }

    synchronized void updateStats(List<AutoFollowResult> results) {
        for (AutoFollowResult result : results) {
            if (result.clusterStateFetchException != null) {
                recentAutoFollowErrors.put(result.autoFollowPatternName,
                    new ElasticsearchException(result.clusterStateFetchException));
                numberOfFailedRemoteClusterStateRequests++;
                LOGGER.warn(new ParameterizedMessage("failure occurred while fetching cluster state for auto follow pattern [{}]",
                    result.autoFollowPatternName), result.clusterStateFetchException);
            } else {
                for (Map.Entry<Index, Exception> entry : result.autoFollowExecutionResults.entrySet()) {
                    if (entry.getValue() != null) {
                        numberOfFailedIndicesAutoFollowed++;
                        recentAutoFollowErrors.put(result.autoFollowPatternName + ":" + entry.getKey().getName(),
                            ExceptionsHelper.convertToElastic(entry.getValue()));
                        LOGGER.warn(new ParameterizedMessage("failure occurred while auto following index [{}] for auto follow " +
                            "pattern [{}]", entry.getKey(), result.autoFollowPatternName), entry.getValue());
                    } else {
                        numberOfSuccessfulIndicesAutoFollowed++;
                    }
                }
            }

        }
    }

    void updateAutoFollowers(ClusterState followerClusterState) {
        AutoFollowMetadata autoFollowMetadata = followerClusterState.getMetaData().custom(AutoFollowMetadata.TYPE);
        if (autoFollowMetadata == null) {
            return;
        }

        if (ccrLicenseChecker.isCcrAllowed() == false) {
            // TODO: set non-compliant status on auto-follow coordination that can be viewed via a stats API
            LOGGER.warn("skipping auto-follower coordination", LicenseUtils.newComplianceException("ccr"));
            return;
        }

        final CopyOnWriteHashMap<String, AutoFollower> autoFollowers = CopyOnWriteHashMap.copyOf(this.autoFollowers);
        Set<String> newRemoteClusters = autoFollowMetadata.getPatterns().entrySet().stream()
            .map(entry -> entry.getValue().getRemoteCluster())
            .filter(remoteCluster -> autoFollowers.containsKey(remoteCluster) == false)
            .collect(Collectors.toSet());

        Map<String, AutoFollower> newAutoFollowers = new HashMap<>(newRemoteClusters.size());
        for (String remoteCluster : newRemoteClusters) {
            AutoFollower autoFollower = new AutoFollower(remoteCluster, threadPool, this::updateStats, clusterService::state) {

                @Override
                void getLeaderClusterState(final String remoteCluster,
                                           final BiConsumer<ClusterState, Exception> handler) {
                    final ClusterStateRequest request = new ClusterStateRequest();
                    request.clear();
                    request.metaData(true);
                    request.routingTable(true);
                    // TODO: set non-compliant status on auto-follow coordination that can be viewed via a stats API
                    ccrLicenseChecker.checkRemoteClusterLicenseAndFetchClusterState(
                        client,
                        remoteCluster,
                        request,
                        e -> handler.accept(null, e),
                        leaderClusterState -> handler.accept(leaderClusterState, null));
                }

                @Override
                void createAndFollow(Map<String, String> headers,
                                     PutFollowAction.Request request,
                                     Runnable successHandler,
                                     Consumer<Exception> failureHandler) {
                    Client followerClient = CcrLicenseChecker.wrapClient(client, headers);
                    followerClient.execute(
                        PutFollowAction.INSTANCE,
                        request,
                        ActionListener.wrap(r -> successHandler.run(), failureHandler)
                    );
                }

                @Override
                void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction,
                                              Consumer<Exception> handler) {
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
            newAutoFollowers.put(remoteCluster, autoFollower);
            autoFollower.autoFollowIndices();
        }

        List<String> removedRemoteClusters = new ArrayList<>();
        for (String remoteCluster : autoFollowers.keySet()) {
            boolean exist = autoFollowMetadata.getPatterns().values().stream()
                .anyMatch(pattern -> pattern.getRemoteCluster().equals(remoteCluster));
            if (exist == false) {
                removedRemoteClusters.add(remoteCluster);
            }
        }
        this.autoFollowers = autoFollowers
            .copyAndPutAll(newAutoFollowers)
            .copyAndRemoveAll(removedRemoteClusters);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            updateAutoFollowers(event.state());
        }
    }

    /**
     * Each auto follower independently monitors a remote cluster for new leader indices that should be auto followed.
     * The reason that this should happen independently, is that when auto followers start to make use of cluster state
     * API's 'wait_for_metadata_version' feature, it may take sometime before a remote cluster responds with a new
     * cluster state or times out. Other auto follow patterns for different remote clusters are then forced to wait,
     * which can cause new follower indices to unnecessarily start with a large backlog of operations that need to be
     * replicated.
     */
    abstract static class AutoFollower {

        private final String remoteCluster;
        private final ThreadPool threadPool;
        private final Consumer<List<AutoFollowResult>> statsUpdater;
        private final Supplier<ClusterState> followerClusterStateSupplier;

        private volatile CountDown autoFollowPatternsCountDown;
        private volatile AtomicArray<AutoFollowResult> autoFollowResults;

        AutoFollower(final String remoteCluster,
                     final ThreadPool threadPool,
                     final Consumer<List<AutoFollowResult>> statsUpdater,
                     final Supplier<ClusterState> followerClusterStateSupplier) {
            this.remoteCluster = remoteCluster;
            this.threadPool = threadPool;
            this.statsUpdater = statsUpdater;
            this.followerClusterStateSupplier = followerClusterStateSupplier;
        }

        void autoFollowIndices() {
            final ClusterState followerClusterState = followerClusterStateSupplier.get();
            final AutoFollowMetadata autoFollowMetadata = followerClusterState.metaData().custom(AutoFollowMetadata.TYPE);
            if (autoFollowMetadata == null) {
                LOGGER.info("AutoFollower for cluster [{}] has stopped, because there is no autofollow metadata", remoteCluster);
                return;
            }

            final List<String> patterns = autoFollowMetadata.getPatterns().entrySet().stream()
                .filter(entry -> entry.getValue().getRemoteCluster().equals(remoteCluster))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            if (patterns.isEmpty()) {
                LOGGER.info("AutoFollower for cluster [{}] has stopped, because there are no more patterns", remoteCluster);
                return;
            }

            this.autoFollowPatternsCountDown = new CountDown(patterns.size());
            this.autoFollowResults = new AtomicArray<>(patterns.size());

            getLeaderClusterState(remoteCluster, (leaderClusterState, e) -> {
                if (leaderClusterState != null) {
                    assert e == null;

                    int i = 0;
                    for (String autoFollowPatternName : patterns) {
                        final int slot = i;
                        AutoFollowPattern autoFollowPattern = autoFollowMetadata.getPatterns().get(autoFollowPatternName);
                        Map<String, String> headers = autoFollowMetadata.getHeaders().get(autoFollowPatternName);
                        List<String> followedIndices = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get(autoFollowPatternName);

                        final List<Index> leaderIndicesToFollow = getLeaderIndicesToFollow(autoFollowPattern, leaderClusterState,
                            followerClusterState, followedIndices);
                        if (leaderIndicesToFollow.isEmpty()) {
                            finalise(slot, new AutoFollowResult(autoFollowPatternName));
                        } else {
                            List<Tuple<String, AutoFollowPattern>> patternsForTheSameLeaderCluster = autoFollowMetadata.getPatterns()
                                .entrySet().stream()
                                .filter(item -> autoFollowPatternName.equals(item.getKey()) == false)
                                .filter(item -> remoteCluster.equals(item.getValue().getRemoteCluster()))
                                .map(item -> new Tuple<>(item.getKey(), item.getValue()))
                                .collect(Collectors.toList());

                            Consumer<AutoFollowResult> resultHandler = result -> finalise(slot, result);
                            checkAutoFollowPattern(autoFollowPatternName, remoteCluster, autoFollowPattern, leaderIndicesToFollow, headers,
                                patternsForTheSameLeaderCluster, resultHandler);
                        }
                        i++;
                    }
                } else {
                    List<AutoFollowResult> results = new ArrayList<>(patterns.size());
                    for (String autoFollowPatternName : patterns) {
                        results.add(new AutoFollowResult(autoFollowPatternName, e));
                    }
                    statsUpdater.accept(results);
                }
            });
        }

        private void checkAutoFollowPattern(String autoFollowPattenName,
                                            String leaderCluster,
                                            AutoFollowPattern autoFollowPattern,
                                            List<Index> leaderIndicesToFollow,
                                            Map<String, String> headers,
                                            List<Tuple<String, AutoFollowPattern>> patternsForTheSameLeaderCluster,
                                            Consumer<AutoFollowResult> resultHandler) {

            final CountDown leaderIndicesCountDown = new CountDown(leaderIndicesToFollow.size());
            final AtomicArray<Tuple<Index, Exception>> results = new AtomicArray<>(leaderIndicesToFollow.size());
            for (int i = 0; i < leaderIndicesToFollow.size(); i++) {
                final Index indexToFollow = leaderIndicesToFollow.get(i);
                final int slot = i;

                List<String> otherMatchingPatterns = patternsForTheSameLeaderCluster.stream()
                    .filter(otherPattern -> otherPattern.v2().match(indexToFollow.getName()))
                    .map(Tuple::v1)
                    .collect(Collectors.toList());
                if (otherMatchingPatterns.size() != 0) {
                    results.set(slot, new Tuple<>(indexToFollow, new ElasticsearchException("index to follow [" + indexToFollow.getName() +
                        "] for pattern [" + autoFollowPattenName + "] matches with other patterns " + otherMatchingPatterns + "")));
                    if (leaderIndicesCountDown.countDown()) {
                        resultHandler.accept(new AutoFollowResult(autoFollowPattenName, results.asList()));
                    }
                } else {
                    followLeaderIndex(autoFollowPattenName, leaderCluster, indexToFollow, autoFollowPattern, headers, error -> {
                        results.set(slot, new Tuple<>(indexToFollow, error));
                        if (leaderIndicesCountDown.countDown()) {
                            resultHandler.accept(new AutoFollowResult(autoFollowPattenName, results.asList()));
                        }
                    });
                }

            }
        }

        private void followLeaderIndex(String autoFollowPattenName,
                                       String remoteCluster,
                                       Index indexToFollow,
                                       AutoFollowPattern pattern,
                                       Map<String,String> headers,
                                       Consumer<Exception> onResult) {
            final String leaderIndexName = indexToFollow.getName();
            final String followIndexName = getFollowerIndexName(pattern, leaderIndexName);

            ResumeFollowAction.Request followRequest = new ResumeFollowAction.Request();
            followRequest.setFollowerIndex(followIndexName);
            followRequest.setMaxReadRequestOperationCount(pattern.getMaxReadRequestOperationCount());
            followRequest.setMaxReadRequestSize(pattern.getMaxReadRequestSize());
            followRequest.setMaxOutstandingReadRequests(pattern.getMaxOutstandingReadRequests());
            followRequest.setMaxWriteRequestOperationCount(pattern.getMaxWriteRequestOperationCount());
            followRequest.setMaxWriteRequestSize(pattern.getMaxWriteRequestSize());
            followRequest.setMaxOutstandingWriteRequests(pattern.getMaxOutstandingWriteRequests());
            followRequest.setMaxWriteBufferCount(pattern.getMaxWriteBufferCount());
            followRequest.setMaxWriteBufferSize(pattern.getMaxWriteBufferSize());
            followRequest.setMaxRetryDelay(pattern.getMaxRetryDelay());
            followRequest.setReadPollTimeout(pattern.getPollTimeout());

            PutFollowAction.Request request = new PutFollowAction.Request();
            request.setRemoteCluster(remoteCluster);
            request.setLeaderIndex(indexToFollow.getName());
            request.setFollowRequest(followRequest);

            // Execute if the create and follow api call succeeds:
            Runnable successHandler = () -> {
                LOGGER.info("Auto followed leader index [{}] as follow index [{}]", leaderIndexName, followIndexName);

                // This function updates the auto follow metadata in the cluster to record that the leader index has been followed:
                // (so that we do not try to follow it in subsequent auto follow runs)
                Function<ClusterState, ClusterState> function = recordLeaderIndexAsFollowFunction(autoFollowPattenName, indexToFollow);
                // The coordinator always runs on the elected master node, so we can update cluster state here:
                updateAutoFollowMetadata(function, onResult);
            };
            createAndFollow(headers, request, successHandler, onResult);
        }

        private void finalise(int slot, AutoFollowResult result) {
            assert autoFollowResults.get(slot) == null;
            autoFollowResults.set(slot, result);
            if (autoFollowPatternsCountDown.countDown()) {
                statsUpdater.accept(autoFollowResults.asList());
                // TODO: Remove scheduling here with using cluster state API's waitForMetadataVersion:
                threadPool.schedule(TimeValue.timeValueMillis(2500), ThreadPool.Names.GENERIC, this::autoFollowIndices);
            }
        }

        static List<Index> getLeaderIndicesToFollow(AutoFollowPattern autoFollowPattern,
                                                    ClusterState leaderClusterState,
                                                    ClusterState followerClusterState,
                                                    List<String> followedIndexUUIDs) {
            List<Index> leaderIndicesToFollow = new ArrayList<>();
            for (IndexMetaData leaderIndexMetaData : leaderClusterState.getMetaData()) {
                if (autoFollowPattern.match(leaderIndexMetaData.getIndex().getName())) {
                    IndexRoutingTable indexRoutingTable = leaderClusterState.routingTable().index(leaderIndexMetaData.getIndex());
                    if (indexRoutingTable != null &&
                        // Leader indices can be in the cluster state, but not all primary shards may be ready yet.
                        // This checks ensures all primary shards have started, so that index following does not fail.
                        // If not all primary shards are ready, then the next time the auto follow coordinator runs
                        // this index will be auto followed.
                        indexRoutingTable.allPrimaryShardsActive() &&
                        followedIndexUUIDs.contains(leaderIndexMetaData.getIndex().getUUID()) == false) {
                        // TODO: iterate over the indices in the followerClusterState and check whether a IndexMetaData
                        // has a leader index uuid custom metadata entry that matches with uuid of leaderIndexMetaData variable
                        // If so then handle it differently: not follow it, but just add an entry to
                        // AutoFollowMetadata#followedLeaderIndexUUIDs
                        if (leaderIndexMetaData.getSettings().getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false)) {
                            leaderIndicesToFollow.add(leaderIndexMetaData.getIndex());
                        }
                    }
                }
            }
            return leaderIndicesToFollow;
        }

        static String getFollowerIndexName(AutoFollowPattern autoFollowPattern, String leaderIndexName) {
            if (autoFollowPattern.getFollowIndexPattern() != null) {
                return autoFollowPattern.getFollowIndexPattern().replace("{{leader_index}}", leaderIndexName);
            } else {
                return leaderIndexName;
            }
        }

        static Function<ClusterState, ClusterState> recordLeaderIndexAsFollowFunction(String name,
                                                                                      Index indexToFollow) {
            return currentState -> {
                AutoFollowMetadata currentAutoFollowMetadata = currentState.metaData().custom(AutoFollowMetadata.TYPE);
                Map<String, List<String>> newFollowedIndexUUIDS = new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs());
                if (newFollowedIndexUUIDS.containsKey(name) == false) {
                    // A delete auto follow pattern request can have removed the auto follow pattern while we want to update
                    // the auto follow metadata with the fact that an index was successfully auto followed. If this
                    // happens, we can just skip this step.
                    return currentState;
                }

                newFollowedIndexUUIDS.compute(name, (key, existingUUIDs) -> {
                    assert existingUUIDs != null;
                    List<String> newUUIDs = new ArrayList<>(existingUUIDs);
                    newUUIDs.add(indexToFollow.getUUID());
                    return Collections.unmodifiableList(newUUIDs);
                });
                final AutoFollowMetadata newAutoFollowMetadata = new AutoFollowMetadata(currentAutoFollowMetadata.getPatterns(),
                    newFollowedIndexUUIDS, currentAutoFollowMetadata.getHeaders());
                return ClusterState.builder(currentState)
                    .metaData(MetaData.builder(currentState.getMetaData())
                        .putCustom(AutoFollowMetadata.TYPE, newAutoFollowMetadata).build())
                    .build();
            };
        }

        /**
         * Fetch the cluster state from the leader with the specified cluster alias
         * @param remoteCluster      the name of the leader cluster
         * @param handler            the callback to invoke
         */
        abstract void getLeaderClusterState(
            String remoteCluster,
            BiConsumer<ClusterState, Exception> handler
        );

        abstract void createAndFollow(
            Map<String, String> headers,
            PutFollowAction.Request followRequest,
            Runnable successHandler,
            Consumer<Exception> failureHandler
        );

        abstract void updateAutoFollowMetadata(
            Function<ClusterState, ClusterState> updateFunction,
            Consumer<Exception> handler
        );

    }

    static class AutoFollowResult {

        final String autoFollowPatternName;
        final Exception clusterStateFetchException;
        final Map<Index, Exception> autoFollowExecutionResults;

        AutoFollowResult(String autoFollowPatternName, List<Tuple<Index, Exception>> results) {
            this.autoFollowPatternName = autoFollowPatternName;

            Map<Index, Exception> autoFollowExecutionResults = new HashMap<>();
            for (Tuple<Index, Exception> result : results) {
                autoFollowExecutionResults.put(result.v1(), result.v2());
            }

            this.clusterStateFetchException = null;
            this.autoFollowExecutionResults = Collections.unmodifiableMap(autoFollowExecutionResults);
        }

        AutoFollowResult(String autoFollowPatternName, Exception e) {
            this.autoFollowPatternName = autoFollowPatternName;
            this.clusterStateFetchException = e;
            this.autoFollowExecutionResults = Collections.emptyMap();
        }

        AutoFollowResult(String autoFollowPatternName) {
            this(autoFollowPatternName, (Exception) null);
        }
    }
}
