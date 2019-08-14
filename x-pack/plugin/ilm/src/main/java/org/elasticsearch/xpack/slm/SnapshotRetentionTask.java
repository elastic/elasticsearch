/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
    private final SnapshotHistoryStore historyStore;

    public SnapshotRetentionTask(Client client, ClusterService clusterService, LongSupplier nowNanoSupplier,
                                 SnapshotHistoryStore historyStore) {
        this.client = new OriginSettingClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN);
        this.clusterService = clusterService;
        this.nowNanoSupplier = nowNanoSupplier;
        this.historyStore = historyStore;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        assert event.getJobName().equals(SnapshotRetentionService.SLM_RETENTION_JOB_ID) :
            "expected id to be " + SnapshotRetentionService.SLM_RETENTION_JOB_ID + " but it was " + event.getJobName();
        if (running.compareAndSet(false, true)) {
            final SnapshotLifecycleStats slmStats = new SnapshotLifecycleStats();

            // Defined here so it can be re-used without having to repeat it
            final Consumer<Exception> failureHandler = e -> {
                try {
                    logger.error("error during snapshot retention task", e);
                    slmStats.retentionFailed();
                    updateStateWithStats(slmStats);
                } finally {
                    running.set(false);
                }
            };

            try {
                final ClusterState state = clusterService.state();
                final TimeValue maxDeletionTime = LifecycleSettings.SLM_RETENTION_DURATION_SETTING.get(state.metaData().settings());

                logger.info("starting SLM retention snapshot cleanup task");
                slmStats.retentionRun();
                // Find all SLM policies that have retention enabled
                final Map<String, SnapshotLifecyclePolicy> policiesWithRetention = getAllPoliciesWithRetentionEnabled(state);

                // For those policies (there may be more than one for the same repo),
                // return the repos that we need to get the snapshots for
                final Set<String> repositioriesToFetch = policiesWithRetention.values().stream()
                    .map(SnapshotLifecyclePolicy::getRepository)
                    .collect(Collectors.toSet());

                if (repositioriesToFetch.isEmpty()) {
                    running.set(false);
                    return;
                }

                // Finally, asynchronously retrieve all the snapshots, deleting them serially,
                // before updating the cluster state with the new metrics and setting 'running'
                // back to false
                getAllSuccessfulSnapshots(repositioriesToFetch, new ActionListener<>() {
                    @Override
                    public void onResponse(Map<String, List<SnapshotInfo>> allSnapshots) {
                        try {
                            // Find all the snapshots that are past their retention date
                            final Map<String, List<SnapshotInfo>> snapshotsToBeDeleted = allSnapshots.entrySet().stream()
                                .collect(Collectors.toMap(Map.Entry::getKey,
                                    e -> e.getValue().stream()
                                        .filter(snapshot -> snapshotEligibleForDeletion(snapshot, allSnapshots, policiesWithRetention))
                                        .collect(Collectors.toList())));

                            // Finally, delete the snapshots that need to be deleted
                            deleteSnapshots(snapshotsToBeDeleted, maxDeletionTime, slmStats);

                            updateStateWithStats(slmStats);
                        } finally {
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
        final SnapshotLifecycleMetadata snapMeta = state.metaData().custom(SnapshotLifecycleMetadata.TYPE);
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

    void getAllSuccessfulSnapshots(Collection<String> repositories, ActionListener<Map<String, List<SnapshotInfo>>> listener,
                                   Consumer<Exception> errorHandler) {
        if (repositories.isEmpty()) {
            // Skip retrieving anything if there are no repositories to fetch
            listener.onResponse(Collections.emptyMap());
        }

        client.admin().cluster()
            .prepareGetSnapshots(repositories.toArray(Strings.EMPTY_ARRAY))
            .setIgnoreUnavailable(true)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(final GetSnapshotsResponse resp) {
                    Map<String, List<SnapshotInfo>> snapshots = new HashMap<>();
                    repositories.forEach(repo -> {
                        snapshots.put(repo,
                            // Only return snapshots in the SUCCESS state
                            resp.getSnapshots(repo).stream()
                                .filter(info -> info.state() == SnapshotState.SUCCESS)
                                .collect(Collectors.toList()));
                    });
                    listener.onResponse(snapshots);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(new ParameterizedMessage("unable to retrieve snapshots for [{}] repositories", repositories), e);
                    errorHandler.accept(e);
                }
            });
    }

    static String getPolicyId(SnapshotInfo snapshotInfo) {
        return Optional.ofNullable(snapshotInfo.userMetadata())
            .filter(meta -> meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD) != null)
            .filter(meta -> meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD) instanceof String)
            .map(meta -> (String) meta.get(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD))
            .orElseThrow(() -> new IllegalStateException("expected snapshot " + snapshotInfo +
                " to have a policy in its metadata, but it did not"));
    }

    void deleteSnapshots(Map<String, List<SnapshotInfo>> snapshotsToDelete,
                         TimeValue maximumTime,
                         SnapshotLifecycleStats slmStats) {
        int count = snapshotsToDelete.values().stream().mapToInt(List::size).sum();
        if (count == 0) {
            logger.debug("no snapshots are eligible for deletion");
            return;
        }

        logger.info("starting snapshot retention deletion for [{}] snapshots", count);
        long startTime = nowNanoSupplier.getAsLong();
        int deleted = 0;
        int failed = 0;
        for (Map.Entry<String, List<SnapshotInfo>> entry : snapshotsToDelete.entrySet()) {
            String repo = entry.getKey();
            List<SnapshotInfo> snapshots = entry.getValue();
            for (SnapshotInfo info : snapshots) {
                Optional<SnapshotHistoryItem> result = deleteSnapshot(getPolicyId(info), repo, info, slmStats);
                if (result.isPresent()) {
                    if (result.get().isSuccess()) {
                        deleted++;
                    }
                    historyStore.putAsync(result.get());
                } else {
                    failed++;
                }
                // Check whether we have exceeded the maximum time allowed to spend deleting
                // snapshots, if we have, short-circuit the rest of the deletions
                TimeValue elapsedDeletionTime = TimeValue.timeValueNanos(nowNanoSupplier.getAsLong() - startTime);
                logger.trace("elapsed time for deletion of [{}] snapshot: {}", info.snapshotId(), elapsedDeletionTime);
                if (elapsedDeletionTime.compareTo(maximumTime) > 0) {
                    logger.info("maximum snapshot retention deletion time reached, time spent: [{}]," +
                            " maximum allowed time: [{}], deleted [{}] out of [{}] snapshots scheduled for deletion, failed to delete [{}]",
                        elapsedDeletionTime, maximumTime, deleted, count, failed);
                    slmStats.deletionTime(elapsedDeletionTime);
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
     * @param repo The repository the snapshot is in
     * @param snapshot The snapshot metadata
     * @return If present, a SnapshotHistoryItem containing the results of the deletion. Empty if no response or interrupted.
     */
    Optional<SnapshotHistoryItem> deleteSnapshot(String slmPolicy, String repo, SnapshotInfo snapshot, SnapshotLifecycleStats slmStats) {
        logger.info("[{}] snapshot retention deleting snapshot [{}]", repo, snapshot.snapshotId());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SnapshotHistoryItem> result = new AtomicReference<>();
        client.admin().cluster().prepareDeleteSnapshot(repo, snapshot.snapshotId().getName())
            .execute(new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged()) {
                        logger.debug("[{}] snapshot [{}] deleted successfully", repo, snapshot.snapshotId());
                        result.set(SnapshotHistoryItem.deletionSuccessRecord(Instant.now().toEpochMilli(),
                            snapshot.snapshotId().getName(), slmPolicy, repo));
                    } else {
                        logger.warn("[{}] snapshot [{}] delete issued but the request was not acknowledged", repo, snapshot.snapshotId());
                    }
                    slmStats.snapshotDeleted(slmPolicy);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(new ParameterizedMessage("[{}] failed to delete snapshot [{}] for retention",
                        repo, snapshot.snapshotId()), e);
                    try {
                        result.set(SnapshotHistoryItem.deletionFailureRecord(Instant.now().toEpochMilli(),
                            snapshot.snapshotId().getName(), slmPolicy, repo, e));
                    } catch (IOException ex) {
                        // This shouldn't happen unless there's an issue with serializing the original exception
                        logger.error(new ParameterizedMessage(
                            "failed to record snapshot creation failure for snapshot lifecycle policy [{}]",
                            slmPolicy), e);
                    }
                    slmStats.snapshotDeleteFailure(slmPolicy);
                }
            }, latch));
        try {
            // Deletes cannot occur simultaneously, so wait for this
            // deletion to complete before attempting the next one
            latch.await();
        } catch (InterruptedException e) {
            logger.error(new ParameterizedMessage("[{}] deletion of snapshot [{}] interrupted",
                repo, snapshot.snapshotId()), e);
            slmStats.snapshotDeleteFailure(slmPolicy);
        }
        return Optional.ofNullable(result.get());
    }

    void updateStateWithStats(SnapshotLifecycleStats newStats) {
        clusterService.submitStateUpdateTask("update_slm_stats", new UpdateSnapshotLifecycleStatsTask(newStats));
    }
}
