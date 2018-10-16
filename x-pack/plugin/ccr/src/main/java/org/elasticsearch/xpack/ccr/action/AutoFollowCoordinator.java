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
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
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
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A component that runs only on the elected master node and follows leader indices automatically
 * if they match with a auto follow pattern that is defined in {@link AutoFollowMetadata}.
 */
public class AutoFollowCoordinator implements ClusterStateApplier {

    private static final Logger LOGGER = LogManager.getLogger(AutoFollowCoordinator.class);
    private static final int MAX_AUTO_FOLLOW_ERRORS = 256;

    private final Client client;
    private final TimeValue pollInterval;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final CcrLicenseChecker ccrLicenseChecker;

    private volatile boolean localNodeMaster = false;

    // The following fields are read and updated under a lock:
    private long numberOfSuccessfulIndicesAutoFollowed = 0;
    private long numberOfFailedIndicesAutoFollowed = 0;
    private long numberOfFailedRemoteClusterStateRequests = 0;
    private final LinkedHashMap<String, ElasticsearchException> recentAutoFollowErrors;

    public AutoFollowCoordinator(
            Settings settings,
            Client client,
            ThreadPool threadPool,
            ClusterService clusterService,
            CcrLicenseChecker ccrLicenseChecker) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker, "ccrLicenseChecker");

        this.pollInterval = CcrSettings.CCR_AUTO_FOLLOW_POLL_INTERVAL.get(settings);
        clusterService.addStateApplier(this);

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
                recentAutoFollowErrors.put(result.clusterAlias,
                    new ElasticsearchException(result.clusterStateFetchException));
                numberOfFailedRemoteClusterStateRequests++;
                LOGGER.warn(new ParameterizedMessage("failure occurred while fetching cluster state in leader cluster [{}]",
                    result.clusterAlias), result.clusterStateFetchException);
            } else {
                for (Map.Entry<Index, Exception> entry : result.autoFollowExecutionResults.entrySet()) {
                    if (entry.getValue() != null) {
                        numberOfFailedIndicesAutoFollowed++;
                        recentAutoFollowErrors.put(result.clusterAlias + ":" + entry.getKey().getName(),
                            ExceptionsHelper.convertToElastic(entry.getValue()));
                        LOGGER.warn(new ParameterizedMessage("failure occurred while auto following index [{}] in leader cluster [{}]",
                            entry.getKey(), result.clusterAlias), entry.getValue());
                    } else {
                        numberOfSuccessfulIndicesAutoFollowed++;
                    }
                }
            }

        }
    }

    private void doAutoFollow() {
        if (localNodeMaster == false) {
            return;
        }
        ClusterState followerClusterState = clusterService.state();
        AutoFollowMetadata autoFollowMetadata = followerClusterState.getMetaData().custom(AutoFollowMetadata.TYPE);
        if (autoFollowMetadata == null) {
            threadPool.schedule(pollInterval, ThreadPool.Names.SAME, this::doAutoFollow);
            return;
        }

        if (autoFollowMetadata.getPatterns().isEmpty()) {
            threadPool.schedule(pollInterval, ThreadPool.Names.SAME, this::doAutoFollow);
            return;
        }

        if (ccrLicenseChecker.isCcrAllowed() == false) {
            // TODO: set non-compliant status on auto-follow coordination that can be viewed via a stats API
            LOGGER.warn("skipping auto-follower coordination", LicenseUtils.newComplianceException("ccr"));
            threadPool.schedule(pollInterval, ThreadPool.Names.SAME, this::doAutoFollow);
            return;
        }

        Consumer<List<AutoFollowResult>> handler = results -> {
            updateStats(results);
            threadPool.schedule(pollInterval, ThreadPool.Names.SAME, this::doAutoFollow);
        };
        AutoFollower operation = new AutoFollower(handler, followerClusterState) {

            @Override
            void getLeaderClusterState(final Map<String, String> headers,
                                       final String leaderClusterAlias,
                                       final BiConsumer<ClusterState, Exception> handler) {
                final ClusterStateRequest request = new ClusterStateRequest();
                request.clear();
                request.metaData(true);

                if ("_local_".equals(leaderClusterAlias)) {
                    Client client = CcrLicenseChecker.wrapClient(AutoFollowCoordinator.this.client, headers);
                    client.admin().cluster().state(
                            request, ActionListener.wrap(r -> handler.accept(r.getState(), null), e -> handler.accept(null, e)));
                } else {
                    // TODO: set non-compliant status on auto-follow coordination that can be viewed via a stats API
                    ccrLicenseChecker.checkRemoteClusterLicenseAndFetchClusterState(
                            client,
                            headers,
                            leaderClusterAlias,
                            request,
                            e -> handler.accept(null, e),
                            leaderClusterState -> handler.accept(leaderClusterState, null));
                }

            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 ResumeFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                Client followerClient = CcrLicenseChecker.wrapClient(client, headers);
                PutFollowAction.Request request = new PutFollowAction.Request(followRequest);
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

        private final Consumer<List<AutoFollowResult>> handler;
        private final ClusterState followerClusterState;
        private final AutoFollowMetadata autoFollowMetadata;

        private final CountDown autoFollowPatternsCountDown;
        private final AtomicArray<AutoFollowResult> autoFollowResults;

        AutoFollower(final Consumer<List<AutoFollowResult>> handler, final ClusterState followerClusterState) {
            this.handler = handler;
            this.followerClusterState = followerClusterState;
            this.autoFollowMetadata = followerClusterState.getMetaData().custom(AutoFollowMetadata.TYPE);
            this.autoFollowPatternsCountDown = new CountDown(autoFollowMetadata.getPatterns().size());
            this.autoFollowResults = new AtomicArray<>(autoFollowMetadata.getPatterns().size());
        }

        void autoFollowIndices() {
            int i = 0;
            for (Map.Entry<String, AutoFollowPattern> entry : autoFollowMetadata.getPatterns().entrySet()) {
                final int slot = i;
                final String clusterAlias = entry.getKey();
                final AutoFollowPattern autoFollowPattern = entry.getValue();

                Map<String, String> headers = autoFollowMetadata.getHeaders().get(clusterAlias);
                getLeaderClusterState(headers, clusterAlias, (leaderClusterState, e) -> {
                    if (leaderClusterState != null) {
                        assert e == null;
                        final List<String> followedIndices = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get(clusterAlias);
                        final List<Index> leaderIndicesToFollow = getLeaderIndicesToFollow(clusterAlias, autoFollowPattern,
                            leaderClusterState, followerClusterState, followedIndices);
                        if (leaderIndicesToFollow.isEmpty()) {
                            finalise(slot, new AutoFollowResult(clusterAlias));
                        } else {
                            Consumer<AutoFollowResult> resultHandler = result -> finalise(slot, result);
                            checkAutoFollowPattern(clusterAlias, autoFollowPattern, leaderIndicesToFollow, headers, resultHandler);
                        }
                    } else {
                        finalise(slot, new AutoFollowResult(clusterAlias, e));
                    }
                });
                i++;
            }
        }

        private void checkAutoFollowPattern(String clusterAlias,
                                            AutoFollowPattern autoFollowPattern,
                                            List<Index> leaderIndicesToFollow,
                                            Map<String, String> headers,
                                            Consumer<AutoFollowResult> resultHandler) {

            final CountDown leaderIndicesCountDown = new CountDown(leaderIndicesToFollow.size());
            final AtomicArray<Tuple<Index, Exception>> results = new AtomicArray<>(leaderIndicesToFollow.size());
            for (int i = 0; i < leaderIndicesToFollow.size(); i++) {
                final Index indexToFollow = leaderIndicesToFollow.get(i);
                final int slot = i;
                followLeaderIndex(clusterAlias, indexToFollow, autoFollowPattern, headers, error -> {
                    results.set(slot, new Tuple<>(indexToFollow, error));
                    if (leaderIndicesCountDown.countDown()) {
                        resultHandler.accept(new AutoFollowResult(clusterAlias, results.asList()));
                    }
                });
            }
        }

        private void followLeaderIndex(String clusterAlias,
                                       Index indexToFollow,
                                       AutoFollowPattern pattern,
                                       Map<String,String> headers,
                                       Consumer<Exception> onResult) {
            final String leaderIndexName = indexToFollow.getName();
            final String followIndexName = getFollowerIndexName(pattern, leaderIndexName);

            String leaderIndexNameWithClusterAliasPrefix = clusterAlias.equals("_local_") ? leaderIndexName :
                clusterAlias + ":" + leaderIndexName;
            ResumeFollowAction.Request request = new ResumeFollowAction.Request();
            request.setLeaderIndex(leaderIndexNameWithClusterAliasPrefix);
            request.setFollowerIndex(followIndexName);
            request.setMaxBatchOperationCount(pattern.getMaxBatchOperationCount());
            request.setMaxConcurrentReadBatches(pattern.getMaxConcurrentReadBatches());
            request.setMaxBatchSize(pattern.getMaxBatchSize());
            request.setMaxConcurrentWriteBatches(pattern.getMaxConcurrentWriteBatches());
            request.setMaxWriteBufferSize(pattern.getMaxWriteBufferSize());
            request.setMaxRetryDelay(pattern.getMaxRetryDelay());
            request.setPollTimeout(pattern.getPollTimeout());

            // Execute if the create and follow api call succeeds:
            Runnable successHandler = () -> {
                LOGGER.info("Auto followed leader index [{}] as follow index [{}]", leaderIndexName, followIndexName);

                // This function updates the auto follow metadata in the cluster to record that the leader index has been followed:
                // (so that we do not try to follow it in subsequent auto follow runs)
                Function<ClusterState, ClusterState> function = recordLeaderIndexAsFollowFunction(clusterAlias, indexToFollow);
                // The coordinator always runs on the elected master node, so we can update cluster state here:
                updateAutoFollowMetadata(function, onResult);
            };
            createAndFollow(headers, request, successHandler, onResult);
        }

        private void finalise(int slot, AutoFollowResult result) {
            assert autoFollowResults.get(slot) == null;
            autoFollowResults.set(slot, result);
            if (autoFollowPatternsCountDown.countDown()) {
                handler.accept(autoFollowResults.asList());
            }
        }

        static List<Index> getLeaderIndicesToFollow(String clusterAlias,
                                                    AutoFollowPattern autoFollowPattern,
                                                    ClusterState leaderClusterState,
                                                    ClusterState followerClusterState,
                                                    List<String> followedIndexUUIDs) {
            List<Index> leaderIndicesToFollow = new ArrayList<>();
            for (IndexMetaData leaderIndexMetaData : leaderClusterState.getMetaData()) {
                // If an auto follow pattern has been set up for the local cluster then
                // we should not automatically follow a leader index that is also a follow index because
                // this can result into an index creation explosion.
                if (leaderIndexMetaData.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY) != null &&
                    clusterAlias.equals("_local_")) {
                    continue;
                }

                if (autoFollowPattern.match(leaderIndexMetaData.getIndex().getName())) {
                    if (followedIndexUUIDs.contains(leaderIndexMetaData.getIndex().getUUID()) == false) {
                        // TODO: iterate over the indices in the followerClusterState and check whether a IndexMetaData
                        // has a leader index uuid custom metadata entry that matches with uuid of leaderIndexMetaData variable
                        // If so then handle it differently: not follow it, but just add an entry to
                        // AutoFollowMetadata#followedLeaderIndexUUIDs
                        leaderIndicesToFollow.add(leaderIndexMetaData.getIndex());
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

        static Function<ClusterState, ClusterState> recordLeaderIndexAsFollowFunction(String clusterAlias,
                                                                                      Index indexToFollow) {
            return currentState -> {
                AutoFollowMetadata currentAutoFollowMetadata = currentState.metaData().custom(AutoFollowMetadata.TYPE);
                Map<String, List<String>> newFollowedIndexUUIDS = new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs());
                newFollowedIndexUUIDS.compute(clusterAlias, (key, existingUUIDs) -> {
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
         *
         * @param headers            the client headers
         * @param leaderClusterAlias the cluster alias of the leader
         * @param handler            the callback to invoke
         */
        abstract void getLeaderClusterState(
            Map<String, String> headers,
            String leaderClusterAlias,
            BiConsumer<ClusterState, Exception> handler
        );

        abstract void createAndFollow(
            Map<String, String> headers,
            ResumeFollowAction.Request followRequest,
            Runnable successHandler,
            Consumer<Exception> failureHandler
        );

        abstract void updateAutoFollowMetadata(
            Function<ClusterState, ClusterState> updateFunction,
            Consumer<Exception> handler
        );

    }

    static class AutoFollowResult {

        final String clusterAlias;
        final Exception clusterStateFetchException;
        final Map<Index, Exception> autoFollowExecutionResults;

        AutoFollowResult(String clusterAlias, List<Tuple<Index, Exception>> results) {
            this.clusterAlias = clusterAlias;

            Map<Index, Exception> autoFollowExecutionResults = new HashMap<>();
            for (Tuple<Index, Exception> result : results) {
                autoFollowExecutionResults.put(result.v1(), result.v2());
            }

            this.clusterStateFetchException = null;
            this.autoFollowExecutionResults = Collections.unmodifiableMap(autoFollowExecutionResults);
        }

        AutoFollowResult(String clusterAlias, Exception e) {
            this.clusterAlias = clusterAlias;
            this.clusterStateFetchException = e;
            this.autoFollowExecutionResults = Collections.emptyMap();
        }

        AutoFollowResult(String clusterAlias) {
            this(clusterAlias, (Exception) null);
        }
    }
}
