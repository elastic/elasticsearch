/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ccr.AutoFollowStats.AutoFollowedCluster;

/**
 * A component that runs only on the elected master node and follows leader indices automatically
 * if they match with a auto follow pattern that is defined in {@link AutoFollowMetadata}.
 */
public class AutoFollowCoordinator extends AbstractLifecycleComponent implements ClusterStateListener {

    /**
     * This is the string that will be replaced by the leader index name for a backing index or data
     * stream. It allows auto-following to automatically rename an index or data stream when
     * automatically followed. For example, using "{{leader_index}}_copy" for the follow pattern
     * means that a data stream called "logs-foo-bar" would be renamed "logs-foo-bar_copy" when
     * replicated, and a backing index called ".ds-logs-foo-bar-2022-02-02-000001" would be renamed
     * to ".ds-logs-foo-bar_copy-2022-02-02-000001".
     * See {@link AutoFollower#getFollowerIndexName} for the entire usage.
     */
    public static final String AUTO_FOLLOW_PATTERN_REPLACEMENT = "{{leader_index}}";

    private static final Logger LOGGER = LogManager.getLogger(AutoFollowCoordinator.class);
    private static final int MAX_AUTO_FOLLOW_ERRORS = 256;

    private static final Pattern DS_BACKING_PATTERN = Pattern.compile(
        "^(.*?" + DataStream.BACKING_INDEX_PREFIX + ")(.+)-(\\d{4}.\\d{2}.\\d{2})(-[\\d]+)?$"
    );

    private final Client client;
    private final ClusterService clusterService;
    private final CcrLicenseChecker ccrLicenseChecker;
    private final LongSupplier relativeMillisTimeProvider;
    private final LongSupplier absoluteMillisTimeProvider;
    private final Executor executor;

    private volatile TimeValue waitForMetadataTimeOut;
    private volatile Map<String, AutoFollower> autoFollowers = Collections.emptyMap();
    private volatile Set<String> patterns = Set.of();

    // The following fields are read and updated under a lock:
    private long numberOfSuccessfulIndicesAutoFollowed = 0;
    private long numberOfFailedIndicesAutoFollowed = 0;
    private long numberOfFailedRemoteClusterStateRequests = 0;
    private final LinkedHashMap<AutoFollowErrorKey, Tuple<Long, ElasticsearchException>> recentAutoFollowErrors;

    private record AutoFollowErrorKey(String pattern, String index) {
        private AutoFollowErrorKey(String pattern, String index) {
            this.pattern = Objects.requireNonNull(pattern);
            this.index = index;
        }

        @Override
        public String toString() {
            return index != null ? pattern + ':' + index : pattern;
        }
    }

    public AutoFollowCoordinator(
        final Settings settings,
        final Client client,
        final ClusterService clusterService,
        final CcrLicenseChecker ccrLicenseChecker,
        final LongSupplier relativeMillisTimeProvider,
        final LongSupplier absoluteMillisTimeProvider,
        final Executor executor
    ) {

        this.client = client;
        this.clusterService = clusterService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker, "ccrLicenseChecker");
        this.relativeMillisTimeProvider = relativeMillisTimeProvider;
        this.absoluteMillisTimeProvider = absoluteMillisTimeProvider;
        this.executor = Objects.requireNonNull(executor);
        this.recentAutoFollowErrors = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<AutoFollowErrorKey, Tuple<Long, ElasticsearchException>> eldest) {
                return size() > MAX_AUTO_FOLLOW_ERRORS;
            }
        };

        Consumer<TimeValue> updater = newWaitForTimeOut -> {
            if (newWaitForTimeOut.equals(waitForMetadataTimeOut) == false) {
                LOGGER.info("changing wait_for_metadata_timeout from [{}] to [{}]", waitForMetadataTimeOut, newWaitForTimeOut);
                waitForMetadataTimeOut = newWaitForTimeOut;
            }
        };
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT, updater);
        waitForMetadataTimeOut = CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT.get(settings);
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
        LOGGER.trace("stopping all auto-followers");
        /*
         * Synchronization is not necessary here; the field is volatile and the map is a copy-on-write map, any new auto-followers will not
         * start since we check started status of the coordinator before starting them.
         */
        autoFollowers.values().forEach(AutoFollower::stop);
    }

    @Override
    protected void doClose() {

    }

    public synchronized AutoFollowStats getStats() {
        final Map<String, AutoFollower> autoFollowersCopy = this.autoFollowers;
        final TreeMap<String, AutoFollowedCluster> timesSinceLastAutoFollowPerRemoteCluster = new TreeMap<>();
        for (Map.Entry<String, AutoFollower> entry : autoFollowersCopy.entrySet()) {
            long lastAutoFollowTimeInMillis = entry.getValue().lastAutoFollowTimeInMillis;
            long lastSeenMetadataVersion = entry.getValue().metadataVersion;
            if (lastAutoFollowTimeInMillis != -1) {
                long timeSinceLastCheckInMillis = relativeMillisTimeProvider.getAsLong() - lastAutoFollowTimeInMillis;
                timesSinceLastAutoFollowPerRemoteCluster.put(
                    entry.getKey(),
                    new AutoFollowedCluster(timeSinceLastCheckInMillis, lastSeenMetadataVersion)
                );
            } else {
                timesSinceLastAutoFollowPerRemoteCluster.put(entry.getKey(), new AutoFollowedCluster(-1L, lastSeenMetadataVersion));
            }
        }

        var recentAutoFollowErrorsCopy = new TreeMap<String, Tuple<Long, ElasticsearchException>>();
        for (var entry : recentAutoFollowErrors.entrySet()) {
            recentAutoFollowErrorsCopy.put(entry.getKey().toString(), entry.getValue());
        }

        return new AutoFollowStats(
            numberOfFailedIndicesAutoFollowed,
            numberOfFailedRemoteClusterStateRequests,
            numberOfSuccessfulIndicesAutoFollowed,
            recentAutoFollowErrorsCopy,
            timesSinceLastAutoFollowPerRemoteCluster
        );
    }

    synchronized void updateStats(List<AutoFollowResult> results) {
        // purge stats for removed patterns
        var currentPatterns = this.patterns;
        recentAutoFollowErrors.keySet().removeIf(key -> currentPatterns.contains(key.pattern) == false);
        // add new stats
        long newStatsReceivedTimeStamp = absoluteMillisTimeProvider.getAsLong();
        for (AutoFollowResult result : results) {
            var onlyPatternKey = new AutoFollowErrorKey(result.autoFollowPatternName, null);
            if (result.clusterStateFetchException != null) {
                recentAutoFollowErrors.put(
                    onlyPatternKey,
                    Tuple.tuple(newStatsReceivedTimeStamp, new ElasticsearchException(result.clusterStateFetchException))
                );
                numberOfFailedRemoteClusterStateRequests++;
                LOGGER.warn(
                    () -> format(
                        "failure occurred while fetching cluster state for auto follow pattern [%s]",
                        result.autoFollowPatternName
                    ),
                    result.clusterStateFetchException
                );
            } else {
                recentAutoFollowErrors.remove(onlyPatternKey);
                for (Map.Entry<Index, Exception> entry : result.autoFollowExecutionResults.entrySet()) {
                    var patternAndIndexKey = new AutoFollowErrorKey(result.autoFollowPatternName, entry.getKey().getName());
                    if (entry.getValue() != null) {
                        numberOfFailedIndicesAutoFollowed++;
                        recentAutoFollowErrors.put(
                            patternAndIndexKey,
                            Tuple.tuple(newStatsReceivedTimeStamp, ExceptionsHelper.convertToElastic(entry.getValue()))
                        );
                        LOGGER.warn(
                            () -> format(
                                "failure occurred while auto following index [%s] for auto follow pattern [%s]",
                                entry.getKey(),
                                result.autoFollowPatternName
                            ),
                            entry.getValue()
                        );
                    } else {
                        numberOfSuccessfulIndicesAutoFollowed++;
                        recentAutoFollowErrors.remove(patternAndIndexKey);
                    }
                }
            }
        }
    }

    void updateAutoFollowers(ClusterState followerClusterState) {
        final AutoFollowMetadata autoFollowMetadata = followerClusterState.getMetadata()
            .getProject()
            .custom(AutoFollowMetadata.TYPE, AutoFollowMetadata.EMPTY);

        if (autoFollowMetadata.getPatterns().isEmpty() && this.autoFollowers.isEmpty()) {
            return;
        }

        if (ccrLicenseChecker.isCcrAllowed() == false) {
            // TODO: set non-compliant status on auto-follow coordination that can be viewed via a stats API
            LOGGER.warn("skipping auto-follower coordination", LicenseUtils.newComplianceException("ccr"));
            return;
        }

        this.patterns = Set.copyOf(autoFollowMetadata.getPatterns().keySet());

        final Map<String, AutoFollower> currentAutoFollowers = Map.copyOf(this.autoFollowers);
        Set<String> newRemoteClusters = autoFollowMetadata.getPatterns()
            .values()
            .stream()
            .filter(AutoFollowPattern::isActive)
            .map(AutoFollowPattern::getRemoteCluster)
            .filter(remoteCluster -> currentAutoFollowers.containsKey(remoteCluster) == false)
            .collect(Collectors.toSet());

        Map<String, AutoFollower> newAutoFollowers = Maps.newMapWithExpectedSize(newRemoteClusters.size());
        for (String remoteCluster : newRemoteClusters) {
            AutoFollower autoFollower = new AutoFollower(
                remoteCluster,
                this::updateStats,
                clusterService::state,
                relativeMillisTimeProvider,
                executor
            ) {

                @Override
                void getRemoteClusterState(
                    final String remoteCluster,
                    final long metadataVersion,
                    final BiConsumer<ClusterStateResponse, Exception> handler
                ) {
                    // TODO: set non-compliant status on auto-follow coordination that can be viewed via a stats API
                    CcrLicenseChecker.checkRemoteClusterLicenseAndFetchClusterState(
                        client,
                        remoteCluster,
                        new ClusterStateRequest(waitForMetadataTimeOut).clear()
                            .metadata(true)
                            .routingTable(true)
                            .local(true)
                            .waitForMetadataVersion(metadataVersion)
                            .waitForTimeout(waitForMetadataTimeOut),
                        e -> handler.accept(null, e),
                        remoteClusterStateResponse -> handler.accept(remoteClusterStateResponse, null)
                    );
                }

                @Override
                void createAndFollow(
                    Map<String, String> headers,
                    PutFollowAction.Request request,
                    Runnable successHandler,
                    Consumer<Exception> failureHandler
                ) {
                    Client followerClient = CcrLicenseChecker.wrapClient(client, headers, clusterService.state());
                    followerClient.execute(
                        PutFollowAction.INSTANCE,
                        request,
                        ActionListener.wrap(r -> successHandler.run(), failureHandler)
                    );
                }

                @Override
                void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                    submitUnbatchedTask("update_auto_follow_metadata", new ClusterStateUpdateTask() {

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            return updateFunction.apply(currentState);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            handler.accept(e);
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                            handler.accept(null);
                        }
                    });
                }

            };
            newAutoFollowers.put(remoteCluster, autoFollower);
            LOGGER.info("starting auto-follower for remote cluster [{}]", remoteCluster);
            if (lifecycleState() == Lifecycle.State.STARTED) {
                autoFollower.start();
            }
        }

        List<String> removedRemoteClusters = new ArrayList<>();
        for (Map.Entry<String, AutoFollower> entry : currentAutoFollowers.entrySet()) {
            String remoteCluster = entry.getKey();
            AutoFollower autoFollower = entry.getValue();
            boolean exist = autoFollowMetadata.getPatterns()
                .values()
                .stream()
                .filter(AutoFollowPattern::isActive)
                .anyMatch(pattern -> pattern.getRemoteCluster().equals(remoteCluster));
            if (exist == false) {
                LOGGER.info("removing auto-follower for remote cluster [{}]", remoteCluster);
                autoFollower.removed = true;
                removedRemoteClusters.add(remoteCluster);
            } else if (autoFollower.remoteClusterConnectionMissing) {
                LOGGER.info("retrying auto-follower for remote cluster [{}] after remote cluster connection was missing", remoteCluster);
                autoFollower.remoteClusterConnectionMissing = false;
                if (lifecycleState() == Lifecycle.State.STARTED) {
                    autoFollower.start();
                }
            }
        }
        assert assertNoOtherActiveAutoFollower(newAutoFollowers);

        Map<String, AutoFollower> updatedFollowers = new HashMap<>(currentAutoFollowers);
        updatedFollowers.putAll(newAutoFollowers);
        removedRemoteClusters.forEach(updatedFollowers.keySet()::remove);
        this.autoFollowers = Collections.unmodifiableMap(updatedFollowers);
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private boolean assertNoOtherActiveAutoFollower(Map<String, AutoFollower> newAutoFollowers) {
        for (AutoFollower newAutoFollower : newAutoFollowers.values()) {
            AutoFollower previousInstance = autoFollowers.get(newAutoFollower.remoteCluster);
            assert previousInstance == null || previousInstance.removed;
        }
        return true;
    }

    Map<String, AutoFollower> getAutoFollowers() {
        return autoFollowers;
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
        private final Consumer<List<AutoFollowResult>> statsUpdater;
        private final Supplier<ClusterState> followerClusterStateSupplier;
        private final LongSupplier relativeTimeProvider;
        private final Executor executor;

        private volatile long lastAutoFollowTimeInMillis = -1;
        private volatile long metadataVersion = 0;
        private volatile boolean remoteClusterConnectionMissing = false;
        volatile boolean removed = false;
        private volatile CountDown autoFollowPatternsCountDown;
        private volatile AtomicArray<AutoFollowResult> autoFollowResults;
        private volatile boolean stop;
        private volatile List<String> lastActivePatterns = List.of();

        AutoFollower(
            final String remoteCluster,
            final Consumer<List<AutoFollowResult>> statsUpdater,
            final Supplier<ClusterState> followerClusterStateSupplier,
            final LongSupplier relativeTimeProvider,
            final Executor executor
        ) {
            this.remoteCluster = remoteCluster;
            this.statsUpdater = statsUpdater;
            this.followerClusterStateSupplier = followerClusterStateSupplier;
            this.relativeTimeProvider = relativeTimeProvider;
            this.executor = Objects.requireNonNull(executor);
        }

        void start() {
            if (stop) {
                LOGGER.trace("auto-follower is stopped for remote cluster [{}]", remoteCluster);
                return;
            }
            if (removed) {
                // This check exists to avoid two AutoFollower instances a single remote cluster.
                // (If an auto follow pattern is deleted and then added back quickly enough then
                // the old AutoFollower instance still sees that there is an auto follow pattern
                // for the remote cluster it is tracking and will continue to operate, while in
                // the meantime in updateAutoFollowers() method another AutoFollower instance has been
                // started for the same remote cluster.)
                LOGGER.trace("auto-follower instance for cluster [{}] has been removed", remoteCluster);
                return;
            }

            lastAutoFollowTimeInMillis = relativeTimeProvider.getAsLong();
            final ClusterState clusterState = followerClusterStateSupplier.get();
            final AutoFollowMetadata autoFollowMetadata = clusterState.metadata().getProject().custom(AutoFollowMetadata.TYPE);
            if (autoFollowMetadata == null) {
                LOGGER.info("auto-follower for cluster [{}] has stopped, because there is no autofollow metadata", remoteCluster);
                return;
            }

            final List<String> patterns = autoFollowMetadata.getPatterns()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getRemoteCluster().equals(remoteCluster))
                .filter(entry -> entry.getValue().isActive())
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList());
            if (patterns.isEmpty()) {
                LOGGER.info("auto-follower for cluster [{}] has stopped, because there are no more patterns", remoteCluster);
                return;
            }

            this.autoFollowPatternsCountDown = new CountDown(patterns.size());
            this.autoFollowResults = new AtomicArray<>(patterns.size());

            // keep the list of the last known active patterns for this auto-follower
            // if the list changed, we explicitly retrieve the last cluster state in
            // order to avoid timeouts when waiting for the next remote cluster state
            // version that might never arrive
            final long nextMetadataVersion = Objects.equals(patterns, lastActivePatterns) ? metadataVersion + 1 : metadataVersion;
            this.lastActivePatterns = List.copyOf(patterns);

            final Thread thread = Thread.currentThread();
            getRemoteClusterState(remoteCluster, Math.max(1L, nextMetadataVersion), (remoteClusterStateResponse, remoteError) -> {
                // Also check removed flag here, as it may take a while for this remote cluster state api call to return:
                if (removed) {
                    LOGGER.trace("auto-follower instance for cluster [{}] has been removed", remoteCluster);
                    return;
                }

                if (remoteClusterStateResponse != null) {
                    assert remoteError == null;
                    if (remoteClusterStateResponse.isWaitForTimedOut()) {
                        LOGGER.trace("auto-follow coordinator timed out getting remote cluster state from [{}]", remoteCluster);
                        start();
                        return;
                    }
                    ClusterState remoteClusterState = remoteClusterStateResponse.getState();
                    metadataVersion = remoteClusterState.metadata().version();
                    autoFollowIndices(autoFollowMetadata, clusterState, remoteClusterState, patterns, thread);
                } else {
                    assert remoteError != null;
                    if (remoteError instanceof NoSuchRemoteClusterException) {
                        LOGGER.info("auto-follower for cluster [{}] has stopped, because remote connection is gone", remoteCluster);
                        remoteClusterConnectionMissing = true;
                        return;
                    }

                    for (int i = 0; i < patterns.size(); i++) {
                        String autoFollowPatternName = patterns.get(i);
                        finalise(i, new AutoFollowResult(autoFollowPatternName, remoteError), thread);
                    }
                }
            });
        }

        void stop() {
            LOGGER.trace("stopping auto-follower for remote cluster [{}]", remoteCluster);
            stop = true;
        }

        private void autoFollowIndices(
            final AutoFollowMetadata autoFollowMetadata,
            final ClusterState clusterState,
            final ClusterState remoteClusterState,
            final List<String> patterns,
            final Thread thread
        ) {
            int i = 0;
            for (String autoFollowPatternName : patterns) {
                final int slot = i;
                AutoFollowPattern autoFollowPattern = autoFollowMetadata.getPatterns().get(autoFollowPatternName);
                Map<String, String> headers = autoFollowMetadata.getHeaders().get(autoFollowPatternName);
                List<String> followedIndices = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get(autoFollowPatternName);

                final List<Index> leaderIndicesToFollow = getLeaderIndicesToFollow(autoFollowPattern, remoteClusterState, followedIndices);
                if (leaderIndicesToFollow.isEmpty()) {
                    finalise(slot, new AutoFollowResult(autoFollowPatternName), thread);
                } else {
                    List<Tuple<String, AutoFollowPattern>> patternsForTheSameRemoteCluster = autoFollowMetadata.getPatterns()
                        .entrySet()
                        .stream()
                        .filter(item -> autoFollowPatternName.equals(item.getKey()) == false)
                        .filter(item -> remoteCluster.equals(item.getValue().getRemoteCluster()))
                        .map(item -> new Tuple<>(item.getKey(), item.getValue()))
                        .collect(Collectors.toList());

                    Consumer<AutoFollowResult> resultHandler = result -> finalise(slot, result, thread);
                    checkAutoFollowPattern(
                        autoFollowPatternName,
                        remoteCluster,
                        autoFollowPattern,
                        leaderIndicesToFollow,
                        headers,
                        patternsForTheSameRemoteCluster,
                        remoteClusterState.metadata(),
                        clusterState.metadata(),
                        resultHandler
                    );
                }
                i++;
            }
            cleanFollowedRemoteIndices(remoteClusterState, patterns);
        }

        /**
         * Go through all the leader indices that need to be followed, ensuring that they are
         * auto-followed by only a single pattern, have soft-deletes enabled, are not
         * searchable snapshots, and are not already followed. If all of those conditions are met,
         * then follow the indices.
         */
        private void checkAutoFollowPattern(
            String autoFollowPattenName,
            String remoteClusterString,
            AutoFollowPattern autoFollowPattern,
            List<Index> leaderIndicesToFollow,
            Map<String, String> headers,
            List<Tuple<String, AutoFollowPattern>> patternsForTheSameRemoteCluster,
            Metadata remoteMetadata,
            Metadata localMetadata,
            Consumer<AutoFollowResult> resultHandler
        ) {
            final GroupedActionListener<Tuple<Index, Exception>> groupedListener = new GroupedActionListener<>(
                leaderIndicesToFollow.size(),
                ActionListener.wrap(rs -> resultHandler.accept(new AutoFollowResult(autoFollowPattenName, new ArrayList<>(rs))), e -> {
                    final var illegalStateException = new IllegalStateException("must never happen", e);
                    LOGGER.error("must never happen", illegalStateException);
                    assert false : illegalStateException;
                    throw illegalStateException;
                })
            );

            // Loop through all the as-of-yet-unfollowed indices from the leader
            for (final Index indexToFollow : leaderIndicesToFollow) {
                // Look up the abstraction for the given index, e.g., an index ".ds-foo" could look
                // up the Data Stream "foo"
                IndexAbstraction indexAbstraction = remoteMetadata.getProject().getIndicesLookup().get(indexToFollow.getName());
                // Ensure that the remote cluster doesn't have other patterns
                // that would follow the index, there can be only one.
                List<String> otherMatchingPatterns = patternsForTheSameRemoteCluster.stream()
                    .filter(otherPattern -> otherPattern.v2().match(indexAbstraction))
                    .map(Tuple::v1)
                    .toList();
                if (otherMatchingPatterns.size() != 0) {
                    groupedListener.onResponse(
                        new Tuple<>(
                            indexToFollow,
                            new ElasticsearchException(
                                "index to follow ["
                                    + indexToFollow.getName()
                                    + "] for pattern ["
                                    + autoFollowPattenName
                                    + "] matches with other patterns "
                                    + otherMatchingPatterns
                                    + ""
                            )
                        )
                    );
                } else {
                    final IndexMetadata leaderIndexMetadata = remoteMetadata.getProject().getIndexSafe(indexToFollow);
                    // First ensure that the index on the leader that we want to follow has soft-deletes enabled
                    if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(leaderIndexMetadata.getSettings()) == false) {
                        String message = String.format(
                            Locale.ROOT,
                            "index [%s] cannot be followed, because soft deletes are not enabled",
                            indexToFollow.getName()
                        );
                        LOGGER.warn(message);
                        updateAutoFollowMetadata(recordLeaderIndexAsFollowFunction(autoFollowPattenName, indexToFollow), error -> {
                            ElasticsearchException failure = new ElasticsearchException(message);
                            if (error != null) {
                                failure.addSuppressed(error);
                            }
                            groupedListener.onResponse(new Tuple<>(indexToFollow, failure));
                        });
                    } else if (leaderIndexMetadata.isSearchableSnapshot()) {
                        String message = String.format(
                            Locale.ROOT,
                            "index to follow [%s] is a searchable snapshot index and cannot be used for cross-cluster replication purpose",
                            indexToFollow.getName()
                        );
                        LOGGER.debug(message);
                        updateAutoFollowMetadata(recordLeaderIndexAsFollowFunction(autoFollowPattenName, indexToFollow), error -> {
                            ElasticsearchException failure = new ElasticsearchException(message);
                            if (error != null) {
                                failure.addSuppressed(error);
                            }
                            groupedListener.onResponse(new Tuple<>(indexToFollow, failure));
                        });
                    } else if (leaderIndexAlreadyFollowed(autoFollowPattern, indexToFollow, localMetadata)) {
                        updateAutoFollowMetadata(
                            recordLeaderIndexAsFollowFunction(autoFollowPattenName, indexToFollow),
                            error -> groupedListener.onResponse(new Tuple<>(indexToFollow, error))
                        );
                    } else {
                        // Finally, if there are no reasons why we cannot follow the leader index, perform the follow.
                        followLeaderIndex(
                            autoFollowPattenName,
                            remoteClusterString,
                            indexToFollow,
                            indexAbstraction,
                            autoFollowPattern,
                            headers,
                            error -> groupedListener.onResponse(new Tuple<>(indexToFollow, error))
                        );
                    }
                }
            }
        }

        private static boolean leaderIndexAlreadyFollowed(AutoFollowPattern autoFollowPattern, Index leaderIndex, Metadata localMetadata) {
            String followIndexName = getFollowerIndexName(autoFollowPattern, leaderIndex.getName());
            IndexMetadata indexMetadata = localMetadata.getProject().index(followIndexName);
            if (indexMetadata != null) {
                // If an index with the same name exists, but it is not a follow index for this leader index then
                // we should let the auto follower attempt to auto follow it, so it can fail later and
                // it is then visible in the auto follow stats. For example a cluster can just happen to have
                // an index with the same name as the new follower index.
                Map<String, String> customData = indexMetadata.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
                if (customData != null) {
                    String recordedLeaderIndexUUID = customData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
                    return leaderIndex.getUUID().equals(recordedLeaderIndexUUID);
                }
            }
            return false;
        }

        /**
         * Given a remote cluster, index that will be followed (and its abstraction), as well as an
         * {@link AutoFollowPattern}, generate the internal follow request for following the index.
         */
        static PutFollowAction.Request generateRequest(
            String remoteCluster,
            Index indexToFollow,
            IndexAbstraction indexAbstraction,
            AutoFollowPattern pattern
        ) {
            final String leaderIndexName = indexToFollow.getName();
            final String followIndexName = getFollowerIndexName(pattern, leaderIndexName);

            // TODO use longer timeouts here? see https://github.com/elastic/elasticsearch/issues/109150
            PutFollowAction.Request request = new PutFollowAction.Request(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS);
            request.setRemoteCluster(remoteCluster);
            request.setLeaderIndex(indexToFollow.getName());
            request.setFollowerIndex(followIndexName);
            request.setSettings(pattern.getSettings());
            // If there was a pattern specified for renaming the backing index, and this index is
            // part of a data stream, then send the new data stream name as part of the request.
            if (pattern.getFollowIndexPattern() != null && indexAbstraction.getParentDataStream() != null) {
                String dataStreamName = indexAbstraction.getParentDataStream().getName();
                // Send the follow index pattern as the data stream pattern, so that data streams can be
                // renamed accordingly (not only the backing indices)
                request.setDataStreamName(pattern.getFollowIndexPattern().replace(AUTO_FOLLOW_PATTERN_REPLACEMENT, dataStreamName));
            }
            request.getParameters().setMaxReadRequestOperationCount(pattern.getMaxReadRequestOperationCount());
            request.getParameters().setMaxReadRequestSize(pattern.getMaxReadRequestSize());
            request.getParameters().setMaxOutstandingReadRequests(pattern.getMaxOutstandingReadRequests());
            request.getParameters().setMaxWriteRequestOperationCount(pattern.getMaxWriteRequestOperationCount());
            request.getParameters().setMaxWriteRequestSize(pattern.getMaxWriteRequestSize());
            request.getParameters().setMaxOutstandingWriteRequests(pattern.getMaxOutstandingWriteRequests());
            request.getParameters().setMaxWriteBufferCount(pattern.getMaxWriteBufferCount());
            request.getParameters().setMaxWriteBufferSize(pattern.getMaxWriteBufferSize());
            request.getParameters().setMaxRetryDelay(pattern.getMaxRetryDelay());
            request.getParameters().setReadPollTimeout(pattern.getReadPollTimeout());
            request.masterNodeTimeout(TimeValue.MAX_VALUE);

            return request;
        }

        private void followLeaderIndex(
            String autoFollowPattenName,
            String remoteClusterString,
            Index indexToFollow,
            IndexAbstraction indexAbstraction,
            AutoFollowPattern pattern,
            Map<String, String> headers,
            Consumer<Exception> onResult
        ) {
            PutFollowAction.Request request = generateRequest(remoteClusterString, indexToFollow, indexAbstraction, pattern);

            // Execute if the create and follow api call succeeds:
            Runnable successHandler = () -> {
                LOGGER.info("auto followed leader index [{}] as follow index [{}]", indexToFollow, request.getFollowerIndex());

                // This function updates the auto follow metadata in the cluster to record that the leader index has been followed:
                // (so that we do not try to follow it in subsequent auto follow runs)
                Function<ClusterState, ClusterState> function = recordLeaderIndexAsFollowFunction(autoFollowPattenName, indexToFollow);
                // The coordinator always runs on the elected master node, so we can update cluster state here:
                updateAutoFollowMetadata(function, onResult);
            };
            createAndFollow(headers, request, successHandler, onResult);
        }

        private void finalise(int slot, AutoFollowResult result, final Thread thread) {
            assert autoFollowResults.get(slot) == null;
            autoFollowResults.set(slot, result);
            if (autoFollowPatternsCountDown.countDown()) {
                statsUpdater.accept(autoFollowResults.asList());
                /*
                 * In the face of a failure, we could be called back on the same thread. That is, it could be that we
                 * never fired off the asynchronous remote cluster state call, instead failing beforehand. In this case,
                 * we will recurse on the same thread. If there are repeated failures, we could blow the stack and
                 * overflow. A real-world scenario in which this can occur is if the local connect queue is full. To
                 * avoid this, if we are called back on the same thread, then we truncate the stack by forking to
                 * another thread.
                 */
                if (thread == Thread.currentThread()) {
                    executor.execute(this::start);
                    return;
                }
                start();
            }
        }

        /**
         * Given an auto following pattern for a set of indices and the cluster state from a remote
         * cluster, return the list of indices that need to be followed. The list of followed index
         * UUIDs contains indices that have already been followed, so the returned list will only
         * contain "new" indices from the leader that need to be followed.
         *
         * When looking up the name of the index to see if it matches one of the patterns, the index
         * abstraction ({@link IndexAbstraction}) of the index is used for comparison, this means
         * that if an index named ".ds-foo" was part of a data stream "foo", then an auto-follow
         * pattern of "f*" would allow the ".ds-foo" index to be returned.
         *
         * @param autoFollowPattern pattern to check indices that may need to be followed
         * @param remoteClusterState state from the remote ES cluster
         * @param followedIndexUUIDs a collection of UUIDs of indices already being followed
         * @return any new indices on the leader that need to be followed
         */
        static List<Index> getLeaderIndicesToFollow(
            AutoFollowPattern autoFollowPattern,
            ClusterState remoteClusterState,
            List<String> followedIndexUUIDs
        ) {
            List<Index> leaderIndicesToFollow = new ArrayList<>();
            for (IndexMetadata leaderIndexMetadata : remoteClusterState.getMetadata().getProject()) {
                if (leaderIndexMetadata.getState() != IndexMetadata.State.OPEN) {
                    continue;
                }
                IndexAbstraction indexAbstraction = remoteClusterState.getMetadata()
                    .getProject()
                    .getIndicesLookup()
                    .get(leaderIndexMetadata.getIndex().getName());
                if (autoFollowPattern.isActive() && autoFollowPattern.match(indexAbstraction)) {
                    IndexRoutingTable indexRoutingTable = remoteClusterState.routingTable().index(leaderIndexMetadata.getIndex());
                    if (indexRoutingTable != null &&
                    // Leader indices can be in the cluster state, but not all primary shards may be ready yet.
                    // This checks ensures all primary shards have started, so that index following does not fail.
                    // If not all primary shards are ready, then the next time the auto follow coordinator runs
                    // this index will be auto followed.
                        indexRoutingTable.allPrimaryShardsActive()
                        && followedIndexUUIDs.contains(leaderIndexMetadata.getIndex().getUUID()) == false) {
                        leaderIndicesToFollow.add(leaderIndexMetadata.getIndex());
                    }
                }
            }
            return leaderIndicesToFollow;
        }

        /**
         * Returns the new name for the follower index. If the auto-follow configuration includes a
         * follow index pattern, the text "{@code {{leader_index}}}" is replaced with the original
         * index name, so a leader index called "foo" and a pattern of "{{leader_index}}_copy"
         * becomes a new follower index called "foo_copy".
         */
        static String getFollowerIndexName(AutoFollowPattern autoFollowPattern, String leaderIndexName) {
            final String followPattern = autoFollowPattern.getFollowIndexPattern();
            if (followPattern != null) {
                if (leaderIndexName.contains(DataStream.BACKING_INDEX_PREFIX)) {
                    // The index being replicated is a data stream backing index, so it's something
                    // like: <optional-prefix>.ds-<data-stream-name>-20XX-mm-dd-NNNNNN
                    //
                    // However, we cannot just replace the name with the proposed follow index
                    // pattern, or else we'll end up with something like ".ds-logs-foo-bar-2022-02-02-000001_copy"
                    // for "{{leader_index}}_copy", which will cause problems because it doesn't
                    // follow a parseable pattern. Instead it would be better to rename it as though
                    // the data stream name was the leader index name, ending up with
                    // ".ds-logs-foo-bar_copy-2022-02-02-000001" as the final index name.
                    Matcher m = DS_BACKING_PATTERN.matcher(leaderIndexName);
                    if (m.find()) {
                        return m.group(1) + // Prefix including ".ds-"
                            followPattern.replace(AUTO_FOLLOW_PATTERN_REPLACEMENT, m.group(2)) + // Data stream name changed
                            "-" + // Hyphen separator
                            m.group(3) + // Date math
                            m.group(4);
                    } else {
                        throw new IllegalArgumentException(
                            "unable to determine follower index name from leader index name ["
                                + leaderIndexName
                                + "] and follow index pattern: ["
                                + followPattern
                                + "], index appears to follow a regular data stream backing pattern, but could not be parsed"
                        );
                    }
                } else {
                    // If the index does nat contain a `.ds-<thing>`, then rename it as usual.
                    return followPattern.replace("{{leader_index}}", leaderIndexName);
                }
            } else {
                return leaderIndexName;
            }
        }

        static Function<ClusterState, ClusterState> recordLeaderIndexAsFollowFunction(String name, Index indexToFollow) {
            return currentState -> {
                final var project = currentState.metadata().getProject();
                AutoFollowMetadata currentAutoFollowMetadata = project.custom(AutoFollowMetadata.TYPE);
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
                final AutoFollowMetadata newAutoFollowMetadata = new AutoFollowMetadata(
                    currentAutoFollowMetadata.getPatterns(),
                    newFollowedIndexUUIDS,
                    currentAutoFollowMetadata.getHeaders()
                );
                return ClusterState.builder(currentState)
                    .putProjectMetadata(ProjectMetadata.builder(project).putCustom(AutoFollowMetadata.TYPE, newAutoFollowMetadata).build())
                    .build();
            };
        }

        void cleanFollowedRemoteIndices(final ClusterState remoteClusterState, final List<String> patterns) {
            updateAutoFollowMetadata(cleanFollowedRemoteIndices(remoteClusterState.metadata(), patterns), e -> {
                if (e != null) {
                    LOGGER.warn("Error occured while cleaning followed leader indices", e);
                }
            });
        }

        static Function<ClusterState, ClusterState> cleanFollowedRemoteIndices(
            final Metadata remoteMetadata,
            final List<String> autoFollowPatternNames
        ) {
            return currentState -> {
                final var currentProject = currentState.metadata().getProject();
                AutoFollowMetadata currentAutoFollowMetadata = currentProject.custom(AutoFollowMetadata.TYPE);
                Map<String, List<String>> autoFollowPatternNameToFollowedIndexUUIDs = new HashMap<>(
                    currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs()
                );
                Set<String> remoteIndexUUIDS = remoteMetadata.getProject()
                    .indices()
                    .values()
                    .stream()
                    .map(IndexMetadata::getIndexUUID)
                    .collect(Collectors.toSet());

                boolean requiresCSUpdate = false;
                for (String autoFollowPatternName : autoFollowPatternNames) {
                    if (autoFollowPatternNameToFollowedIndexUUIDs.containsKey(autoFollowPatternName) == false) {
                        // A delete auto follow pattern request can have removed the auto follow pattern while we want to update
                        // the auto follow metadata with the fact that an index was successfully auto followed. If this
                        // happens, we can just skip this step.
                        continue;
                    }

                    List<String> followedIndexUUIDs = new ArrayList<>(autoFollowPatternNameToFollowedIndexUUIDs.get(autoFollowPatternName));
                    // Remove leader indices that no longer exist in the remote cluster:
                    boolean entriesRemoved = followedIndexUUIDs.removeIf(
                        followedLeaderIndexUUID -> remoteIndexUUIDS.contains(followedLeaderIndexUUID) == false
                    );
                    if (entriesRemoved) {
                        requiresCSUpdate = true;
                    }
                    autoFollowPatternNameToFollowedIndexUUIDs.put(autoFollowPatternName, followedIndexUUIDs);
                }

                if (requiresCSUpdate) {
                    final AutoFollowMetadata newAutoFollowMetadata = new AutoFollowMetadata(
                        currentAutoFollowMetadata.getPatterns(),
                        autoFollowPatternNameToFollowedIndexUUIDs,
                        currentAutoFollowMetadata.getHeaders()
                    );
                    return ClusterState.builder(currentState)
                        .putProjectMetadata(
                            ProjectMetadata.builder(currentProject).putCustom(AutoFollowMetadata.TYPE, newAutoFollowMetadata).build()
                        )
                        .build();
                } else {
                    return currentState;
                }
            };
        }

        /**
         * Fetch a remote cluster state from with the specified cluster alias
         * @param remoteCluster      the name of the leader cluster
         * @param metadataVersion   the last seen metadata version
         * @param handler            the callback to invoke
         */
        abstract void getRemoteClusterState(
            String remoteCluster,
            long metadataVersion,
            BiConsumer<ClusterStateResponse, Exception> handler
        );

        abstract void createAndFollow(
            Map<String, String> headers,
            PutFollowAction.Request followRequest,
            Runnable successHandler,
            Consumer<Exception> failureHandler
        );

        abstract void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler);

    }

    static class AutoFollowResult {

        final String autoFollowPatternName;
        final Exception clusterStateFetchException;
        final Map<Index, Exception> autoFollowExecutionResults;

        AutoFollowResult(String autoFollowPatternName, List<Tuple<Index, Exception>> results) {
            this.autoFollowPatternName = autoFollowPatternName;

            Map<Index, Exception> mutableAutoFollowExecutionResults = new HashMap<>();
            for (Tuple<Index, Exception> result : results) {
                mutableAutoFollowExecutionResults.put(result.v1(), result.v2());
            }

            this.clusterStateFetchException = null;
            this.autoFollowExecutionResults = Collections.unmodifiableMap(mutableAutoFollowExecutionResults);
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
