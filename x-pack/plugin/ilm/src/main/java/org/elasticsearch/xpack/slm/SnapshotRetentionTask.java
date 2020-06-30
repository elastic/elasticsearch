/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD;

/**
 * The {@code SnapshotRetentionTask} is invoked by the scheduled job from the
 * {@link SnapshotRetentionService}. It is responsible for retrieving the snapshots for repositories
 * that have an SLM policy configured, and then deleting the snapshots that fall outside the
 * retention policy.
 */
public class SnapshotRetentionTask implements SchedulerEngine.Listener {

    private static final Logger logger = LogManager.getLogger(SnapshotRetentionTask.class);
    private static final AtomicBoolean running = new AtomicBoolean(false);

    private final Client client;
    private final ClusterService clusterService;
    private final LongSupplier nowNanoSupplier;
    private final ThreadPool threadPool;
    private final SnapshotHistoryStore historyStore;

    public SnapshotRetentionTask(Client client, ClusterService clusterService, LongSupplier nowNanoSupplier,
                                 SnapshotHistoryStore historyStore, ThreadPool threadPool) {
        this.client = new OriginSettingClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN);
        this.clusterService = clusterService;
        this.nowNanoSupplier = nowNanoSupplier;
        this.historyStore = historyStore;
        this.threadPool = threadPool;
    }

    private static String formatSnapshots(Map<String, List<SnapshotInfo>> snapshotMap) {
        return snapshotMap.entrySet().stream()
            .map(e -> e.getKey() + ": [" + e.getValue().stream()
                .map(si -> si.snapshotId().getName())
                .collect(Collectors.joining(","))
                + "]")
            .collect(Collectors.joining(","));
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        assert event.getJobName().equals(SnapshotRetentionService.SLM_RETENTION_JOB_ID) ||
            event.getJobName().equals(SnapshotRetentionService.SLM_RETENTION_MANUAL_JOB_ID):
            "expected id to be " + SnapshotRetentionService.SLM_RETENTION_JOB_ID + " or " +
                SnapshotRetentionService.SLM_RETENTION_MANUAL_JOB_ID + " but it was " + event.getJobName();

        final ClusterState state = clusterService.state();

        // Skip running retention if SLM is disabled, however, even if it's
        // disabled we allow manual running.
        if (SnapshotLifecycleService.slmStoppedOrStopping(state) &&
            event.getJobName().equals(SnapshotRetentionService.SLM_RETENTION_MANUAL_JOB_ID) == false) {
            logger.debug("skipping SLM retention as SLM is currently stopped or stopping");
            return;
        }

        if (running.compareAndSet(false, true)) {
            final SnapshotLifecycleStats slmStats = new SnapshotLifecycleStats();

            // Defined here so it can be re-used without having to repeat it
            final Consumer<Exception> failureHandler = e -> {
                try {
                    logger.error("error during snapshot retention task", e);
                    slmStats.retentionFailed();
                    updateStateWithStats(slmStats);
                } finally {
                    logger.info("SLM retention snapshot cleanup task completed with error");
                    running.set(false);
                }
            };

            try {
                final TimeValue maxDeletionTime = LifecycleSettings.SLM_RETENTION_DURATION_SETTING.get(state.metadata().settings());

                logger.info("starting SLM retention snapshot cleanup task");
                slmStats.retentionRun();
                // Find all SLM policies that have retention enabled
                final Map<String, SnapshotLifecyclePolicy> policiesWithRetention = getAllPoliciesWithRetentionEnabled(state);
                logger.trace("policies with retention enabled: {}", policiesWithRetention.keySet());

                // For those policies (there may be more than one for the same repo),
                // return the repos that we need to get the snapshots for
                final Set<String> repositioriesToFetch = policiesWithRetention.values().stream()
                    .map(SnapshotLifecyclePolicy::getRepository)
                    .collect(Collectors.toSet());
                logger.trace("fetching snapshots from repositories: {}", repositioriesToFetch);

                if (repositioriesToFetch.isEmpty()) {
                    running.set(false);
                    logger.info("there are no repositories to fetch, SLM retention snapshot cleanup task complete");
                    return;
                }
                // Finally, asynchronously retrieve all the snapshots, deleting them serially,
                // before updating the cluster state with the new metrics and setting 'running'
                // back to false
                getAllRetainableSnapshots(repositioriesToFetch, new ActionListener<>() {
                    @Override
                    public void onResponse(Map<String, List<SnapshotInfo>> allSnapshots) {
                        try {
                            if (logger.isTraceEnabled()) {
                                logger.trace("retrieved snapshots: [{}]", formatSnapshots(allSnapshots));
                            }
                            // Find all the snapshots that are past their retention date
                            final Map<String, List<SnapshotInfo>> snapshotsToBeDeleted = allSnapshots.entrySet().stream()
                                .collect(Collectors.toMap(Map.Entry::getKey,
                                    e -> e.getValue().stream()
                                        .filter(snapshot -> snapshotEligibleForDeletion(snapshot, allSnapshots, policiesWithRetention))
                                        .collect(Collectors.toList())));

                            if (logger.isTraceEnabled()) {
                                logger.trace("snapshots eligible for deletion: [{}]", formatSnapshots(snapshotsToBeDeleted));
                            }

                            // Finally, delete the snapshots that need to be deleted
                            maybeDeleteSnapshots(snapshotsToBeDeleted, maxDeletionTime, slmStats);

                            updateStateWithStats(slmStats);
                        } finally {
                            logger.info("SLM retention snapshot cleanup task complete");
                            running.set(false);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        failureHandler.accept(e);
                    }
                }, failureHandler);
            } catch (Exception e) {
                failureHandler.accept(e);
            }
        } else {
            logger.trace("snapshot lifecycle retention task started, but a task is already running, skipping");
        }
    }

    static Map<String, SnapshotLifecyclePolicy> getAllPoliciesWithRetentionEnabled(final ClusterState state) {
        final SnapshotLifecycleMetadata snapMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta == null) {
            return Collections.emptyMap();
        }
        return snapMeta.getSnapshotConfigurations().entrySet().stream()
            .filter(e -> e.getValue().getPolicy().getRetentionPolicy() != null)
            .filter(e -> e.getValue().getPolicy().getRetentionPolicy().equals(SnapshotRetentionConfiguration.EMPTY) == false)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPolicy()));
    }

    static boolean snapshotEligibleForDeletion(SnapshotInfo snapshot, Map<String, List<SnapshotInfo>> allSnapshots,
                                               Map<String, SnapshotLifecyclePolicy> policies) {
        if (snapshot.userMetadata() == null) {
            // This snapshot has no metadata, it is not eligible for deletion
            return false;
        }

        final String policyId;
        try {
            policyId = (String) snapshot.userMetadata().get(POLICY_ID_METADATA_FIELD);
        } catch (Exception e) {
            logger.debug("unable to retrieve policy id from snapshot metadata [" + snapshot.userMetadata() + "]", e);
            return false;
        }

        if (policyId == null) {
            // policyId was null in the metadata, so it's not eligible
            return false;
        }

        SnapshotLifecyclePolicy policy = policies.get(policyId);
        if (policy == null) {
            // This snapshot was taking by a policy that doesn't exist, so it's not eligible
            return false;
        }

        SnapshotRetentionConfiguration retention = policy.getRetentionPolicy();
        if (retention == null || retention.equals(SnapshotRetentionConfiguration.EMPTY)) {
            // Retention is not configured
            return false;
        }

        final String repository = policy.getRepository();
        // Retrieve the predicate based on the retention policy, passing in snapshots pertaining only to *this* policy and repository
        boolean eligible = retention.getSnapshotDeletionPredicate(
            allSnapshots.get(repository).stream()
                .filter(info -> Optional.ofNullable(info.userMetadata())
                    .map(meta -> meta.get(POLICY_ID_METADATA_FIELD))
                    .map(pId -> pId.equals(policyId))
                    .orElse(false))
                .collect(Collectors.toList()))
            .test(snapshot);
        logger.debug("[{}] testing snapshot [{}] deletion eligibility: {}",
            repository, snapshot.snapshotId(), eligible ? "ELIGIBLE" : "INELIGIBLE");
        return eligible;
    }

    void getAllRetainableSnapshots(Collection<String> repositories, ActionListener<Map<String, List<SnapshotInfo>>> listener,
                                   Consumer<Exception> errorHandler) {
        if (repositories.isEmpty()) {
            // Skip retrieving anything if there are no repositories to fetch
            listener.onResponse(Collections.emptyMap());
            return;
        }

        client.admin().cluster()
            .prepareGetSnapshots(repositories.toArray(Strings.EMPTY_ARRAY))
            .setIgnoreUnavailable(true)
            .execute(ActionListener.wrap(resp -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("retrieved snapshots: {}",
                            repositories.stream()
                                .flatMap(repo -> resp.getSnapshots(repo).stream().map(si -> si.snapshotId().getName()))
                                .collect(Collectors.toList()));
                    }
                    Map<String, List<SnapshotInfo>> snapshots = new HashMap<>();
                    final Set<SnapshotState> retainableStates = Set.of(SnapshotState.SUCCESS, SnapshotState.FAILED, SnapshotState.PARTIAL);
                    repositories.forEach(repo -> {
                        snapshots.put(repo,
                            // Only return snapshots in the SUCCESS state
                            resp.getSnapshots(repo).stream()
                                .filter(info -> retainableStates.contains(info.state()))
                                .collect(Collectors.toList()));
                    });
                    listener.onResponse(snapshots);
                },
                e -> {
                    logger.debug(new ParameterizedMessage("unable to retrieve snapshots for [{}] repositories", repositories), e);
                    errorHandler.accept(e);
                }));
    }

    static String getPolicyId(SnapshotInfo snapshotInfo) {
        return Optional.ofNullable(snapshotInfo.userMetadata())
            .filter(meta -> meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD) != null)
            .filter(meta -> meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD) instanceof String)
            .map(meta -> (String) meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD))
            .orElseThrow(() -> new IllegalStateException("expected snapshot " + snapshotInfo +
                " to have a policy in its metadata, but it did not"));
    }

    /**
     * Maybe delete the given snapshots. If a snapshot is currently running according to the cluster
     * state, this waits (using a {@link ClusterStateObserver} until a cluster state with no running
     * snapshots before executing the blocking
     * {@link #deleteSnapshots(Map, TimeValue, SnapshotLifecycleStats)} request. At most, we wait
     * for the maximum allowed deletion time before timing out waiting for a state with no
     * running snapshots.
     *
     * It's possible the task may still run into a SnapshotInProgressException, if a snapshot is
     * started between the state retrieved here and the actual deletion. Since is is expected to be
     * a rare case, no special handling is present.
     */
    private void maybeDeleteSnapshots(Map<String, List<SnapshotInfo>> snapshotsToDelete,
                                      TimeValue maximumTime,
                                      SnapshotLifecycleStats slmStats) {
        int count = snapshotsToDelete.values().stream().mapToInt(List::size).sum();
        if (count == 0) {
            logger.debug("no snapshots are eligible for deletion");
            return;
        }

        ClusterState state = clusterService.state();
        if (okayToDeleteSnapshots(state)) {
            logger.trace("there are no snapshots currently running, proceeding with snapshot deletion of [{}]",
                formatSnapshots(snapshotsToDelete));
            deleteSnapshots(snapshotsToDelete, maximumTime, slmStats);
        } else {
            logger.debug("a snapshot is currently running, rescheduling SLM retention for after snapshot has completed");
            ClusterStateObserver observer = new ClusterStateObserver(clusterService, maximumTime, logger, threadPool.getThreadContext());
            CountDownLatch latch = new CountDownLatch(1);
            observer.waitForNextChange(
                new NoSnapshotRunningListener(observer,
                    newState -> threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(() -> {
                        try {
                            logger.trace("received cluster state without running snapshots, proceeding with snapshot deletion of [{}]",
                                formatSnapshots(snapshotsToDelete));
                            deleteSnapshots(snapshotsToDelete, maximumTime, slmStats);
                        } finally {
                            latch.countDown();
                        }
                    }),
                    e -> {
                        latch.countDown();
                        throw new ElasticsearchException(e);
                    }));
            try {
                logger.trace("waiting for snapshot deletion to complete");
                // Wait until we find a cluster state not running a snapshot operation.
                // If we can't find one within a day, give up and throw an error.
                latch.await(1, TimeUnit.DAYS);
                logger.trace("deletion complete");
            } catch (InterruptedException e) {
                throw new ElasticsearchException(e);
            }
        }
    }

    void deleteSnapshots(Map<String, List<SnapshotInfo>> snapshotsToDelete,
                         TimeValue maximumTime,
                         SnapshotLifecycleStats slmStats) {
        int count = snapshotsToDelete.values().stream().mapToInt(List::size).sum();

        logger.info("starting snapshot retention deletion for [{}] snapshots", count);
        long startTime = nowNanoSupplier.getAsLong();
        final AtomicInteger deleted =  new AtomicInteger(0);
        final AtomicInteger failed = new AtomicInteger(0);
        for (Map.Entry<String, List<SnapshotInfo>> entry : snapshotsToDelete.entrySet()) {
            String repo = entry.getKey();
            List<SnapshotInfo> snapshots = entry.getValue();
            for (SnapshotInfo info : snapshots) {
                final String policyId = getPolicyId(info);
                final long deleteStartTime = nowNanoSupplier.getAsLong();
                // TODO: Use snapshot multi-delete instead of this loop if all nodes in the cluster support it
                //       i.e are newer or equal to SnapshotsService#MULTI_DELETE_VERSION
                deleteSnapshot(policyId, repo, info.snapshotId(), slmStats, ActionListener.wrap(acknowledgedResponse -> {
                    deleted.incrementAndGet();
                    assert acknowledgedResponse.isAcknowledged();
                    historyStore.putAsync(SnapshotHistoryItem.deletionSuccessRecord(Instant.now().toEpochMilli(),
                            info.snapshotId().getName(), policyId, repo));
                }, e -> {
                    failed.incrementAndGet();
                    try {
                        final SnapshotHistoryItem result = SnapshotHistoryItem.deletionFailureRecord(Instant.now().toEpochMilli(),
                            info.snapshotId().getName(), policyId, repo, e);
                        historyStore.putAsync(result);
                    } catch (IOException ex) {
                        // This shouldn't happen unless there's an issue with serializing the original exception
                        logger.error(new ParameterizedMessage(
                            "failed to record snapshot deletion failure for snapshot lifecycle policy [{}]",
                            policyId), ex);
                    }
                }));
                // Check whether we have exceeded the maximum time allowed to spend deleting
                // snapshots, if we have, short-circuit the rest of the deletions
                long finishTime = nowNanoSupplier.getAsLong();
                TimeValue deletionTime = TimeValue.timeValueNanos(finishTime - deleteStartTime);
                logger.debug("elapsed time for deletion of [{}] snapshot: {}", info.snapshotId(), deletionTime);
                TimeValue totalDeletionTime = TimeValue.timeValueNanos(finishTime - startTime);
                if (totalDeletionTime.compareTo(maximumTime) > 0) {
                    logger.info("maximum snapshot retention deletion time reached, time spent: [{}]," +
                            " maximum allowed time: [{}], deleted [{}] out of [{}] snapshots scheduled for deletion, failed to delete [{}]",
                        totalDeletionTime, maximumTime, deleted, count, failed);
                    slmStats.deletionTime(totalDeletionTime);
                    slmStats.retentionTimedOut();
                    return;
                }
            }
        }
        TimeValue totalElapsedTime = TimeValue.timeValueNanos(nowNanoSupplier.getAsLong() - startTime);
        logger.debug("total elapsed time for deletion of [{}] snapshots: {}", deleted, totalElapsedTime);
        slmStats.deletionTime(totalElapsedTime);
    }

    /**
     * Delete the given snapshot from the repository in blocking manner
     *
     * @param repo     The repository the snapshot is in
     * @param snapshot The snapshot metadata
     * @param listener {@link ActionListener#onResponse(Object)} is called if a {@link SnapshotHistoryItem} can be created representing a
     *                  successful or failed deletion call. {@link ActionListener#onFailure(Exception)} is called only if interrupted.
     */
    void deleteSnapshot(String slmPolicy, String repo, SnapshotId snapshot, SnapshotLifecycleStats slmStats,
                        ActionListener<AcknowledgedResponse> listener) {
        logger.info("[{}] snapshot retention deleting snapshot [{}]", repo, snapshot);
        CountDownLatch latch = new CountDownLatch(1);
        client.admin().cluster().prepareDeleteSnapshot(repo, snapshot.getName())
            .execute(new LatchedActionListener<>(ActionListener.wrap(acknowledgedResponse -> {
                    if (acknowledgedResponse.isAcknowledged()) {
                        logger.debug("[{}] snapshot [{}] deleted successfully", repo, snapshot);
                    } else {
                        logger.warn("[{}] snapshot [{}] delete issued but the request was not acknowledged", repo, snapshot);
                    }
                    slmStats.snapshotDeleted(slmPolicy);
                    listener.onResponse(acknowledgedResponse);
                },
                e -> {
                    logger.warn(new ParameterizedMessage("[{}] failed to delete snapshot [{}] for retention",
                        repo, snapshot), e);
                    slmStats.snapshotDeleteFailure(slmPolicy);
                    listener.onFailure(e);
                }), latch));
        try {
            // Deletes cannot occur simultaneously, so wait for this
            // deletion to complete before attempting the next one
            latch.await();
        } catch (InterruptedException e) {
            logger.error(new ParameterizedMessage("[{}] deletion of snapshot [{}] interrupted",
                repo, snapshot), e);
            listener.onFailure(e);
            slmStats.snapshotDeleteFailure(slmPolicy);
        }
    }

    void updateStateWithStats(SnapshotLifecycleStats newStats) {
        clusterService.submitStateUpdateTask("update_slm_stats", new UpdateSnapshotLifecycleStatsTask(newStats));
    }

    public static boolean okayToDeleteSnapshots(ClusterState state) {
        // Cannot delete during a snapshot
        if (state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().size() > 0) {
            logger.trace("deletion cannot proceed as there are snapshots in progress");
            return false;
        }

        // Cannot delete during an existing delete
        if (state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).hasDeletionsInProgress()) {
            logger.trace("deletion cannot proceed as there are snapshot deletions in progress");
            return false;
        }

        // Cannot delete while a repository is being cleaned
        if (state.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY).hasCleanupInProgress()) {
            logger.trace("deletion cannot proceed as there are repository cleanups in progress");
            return false;
        }

        // Cannot delete during a restore
        if (state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).isEmpty() == false) {
            logger.trace("deletion cannot proceed as there are snapshot restores in progress");
            return false;
        }

        // It's okay to delete snapshots
        return true;
    }

    /**
     * A {@link ClusterStateObserver.Listener} that invokes the given function with the new state,
     * once no snapshots are running. If a snapshot is still running it registers a new listener
     * and tries again. Passes any exceptions to the original exception listener if they occur.
     */
    class NoSnapshotRunningListener implements ClusterStateObserver.Listener {

        private final Consumer<ClusterState> reRun;
        private final Consumer<Exception> exceptionConsumer;
        private final ClusterStateObserver observer;

        NoSnapshotRunningListener(ClusterStateObserver observer,
                                  Consumer<ClusterState> reRun,
                                  Consumer<Exception> exceptionConsumer) {
            this.observer = observer;
            this.reRun = reRun;
            this.exceptionConsumer = exceptionConsumer;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            try {
                if (okayToDeleteSnapshots(state)) {
                    logger.debug("retrying SLM snapshot retention deletion after snapshot operation has completed");
                    reRun.accept(state);
                } else {
                    logger.trace("received new cluster state but a snapshot operation is still running");
                    observer.waitForNextChange(this);
                }
            } catch (Exception e) {
                exceptionConsumer.accept(e);
            }
        }

        @Override
        public void onClusterServiceClose() {
            // This means the cluster is being shut down, so nothing to do here
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            exceptionConsumer.accept(
                new IllegalStateException("slm retention snapshot deletion out while waiting for ongoing snapshot operations to complete"));
        }
    }
}
