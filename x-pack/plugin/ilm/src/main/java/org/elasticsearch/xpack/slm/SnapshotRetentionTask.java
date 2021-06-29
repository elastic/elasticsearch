/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    private final Client client;
    private final ClusterService clusterService;
    private final LongSupplier nowNanoSupplier;
    private final SnapshotHistoryStore historyStore;

    /**
     * Set of all currently deleting {@link SnapshotId} used to prevent starting multiple deletes for the same snapshot.
     */
    private final Set<SnapshotId> runningDeletions = Collections.synchronizedSet(new HashSet<>());

    public SnapshotRetentionTask(Client client, ClusterService clusterService, LongSupplier nowNanoSupplier,
                                 SnapshotHistoryStore historyStore) {
        this.client = new OriginSettingClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN);
        this.clusterService = clusterService;
        this.nowNanoSupplier = nowNanoSupplier;
        this.historyStore = historyStore;
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

        final SnapshotLifecycleStats slmStats = new SnapshotLifecycleStats();

        // Defined here so it can be re-used without having to repeat it
        final Consumer<Exception> failureHandler = e -> {
            try {
                logger.error("error during snapshot retention task", e);
                slmStats.retentionFailed();
                updateStateWithStats(slmStats);
            } finally {
                logger.info("SLM retention snapshot cleanup task completed with error");
            }
        };

        try {
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
                logger.info("there are no repositories to fetch, SLM retention snapshot cleanup task complete");
                return;
            }
            // Finally, asynchronously retrieve all the snapshots, deleting them serially,
            // before updating the cluster state with the new metrics and setting 'running'
            // back to false
            getAllRetainableSnapshots(repositioriesToFetch, new ActionListener<>() {
                @Override
                public void onResponse(Map<String, List<SnapshotInfo>> allSnapshots) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("retrieved snapshots: [{}]", formatSnapshots(allSnapshots));
                    }
                    // Find all the snapshots that are past their retention date
                    final Map<String, List<Tuple<SnapshotId, String>>> snapshotsToBeDeleted = allSnapshots.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey,
                                    e -> e.getValue().stream()
                                            .filter(snapshot -> snapshotEligibleForDeletion(snapshot, allSnapshots, policiesWithRetention))
                                            // SnapshotInfo instances can be quite large in case they contain e.g. a large collection of
                                            // exceptions so we extract the only two things (id + policy id) here so they can be GCed
                                            .map(snapshotInfo -> Tuple.tuple(snapshotInfo.snapshotId(), getPolicyId(snapshotInfo)))
                                            .collect(Collectors.toList())));

                    if (logger.isTraceEnabled()) {
                        logger.trace("snapshots eligible for deletion: [{}]", snapshotsToBeDeleted);
                    }

                    // Finally, delete the snapshots that need to be deleted
                    deleteSnapshots(snapshotsToBeDeleted, slmStats, ActionListener.wrap(() -> {
                        updateStateWithStats(slmStats);
                        logger.info("SLM retention snapshot cleanup task complete");
                    }));
                }

                @Override
                public void onFailure(Exception e) {
                    failureHandler.accept(e);
                }
            }, failureHandler);
        } catch (Exception e) {
            failureHandler.accept(e);
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
            // don't time out on this request to not produce failed SLM runs in case of a temporarily slow master node
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .setIgnoreUnavailable(true)
            .execute(ActionListener.wrap(resp -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("retrieved snapshots: {}",
                            repositories.stream()
                                .flatMap(repo ->
                                    resp.getSnapshots()
                                        .stream()
                                        .filter(info -> repo.equals(info.repository()))
                                        .map(si -> si.snapshotId().getName())
                                ).collect(Collectors.toList()));
                    }
                    Map<String, List<SnapshotInfo>> snapshots = new HashMap<>();
                    final Set<SnapshotState> retainableStates = Set.of(SnapshotState.SUCCESS, SnapshotState.FAILED, SnapshotState.PARTIAL);
                    repositories.forEach(repo -> {
                        snapshots.put(repo,
                            // Only return snapshots in the SUCCESS state
                            resp.getSnapshots().stream()
                                .filter(info -> repo.equals(info.repository()) && retainableStates.contains(info.state()))
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

    void deleteSnapshots(Map<String, List<Tuple<SnapshotId, String>>> snapshotsToDelete,
                         SnapshotLifecycleStats slmStats,
                         ActionListener<Void> listener) {
        int count = snapshotsToDelete.values().stream().mapToInt(List::size).sum();
        if (count == 0) {
            listener.onResponse(null);
            logger.debug("no snapshots are eligible for deletion");
            return;
        }

        logger.info("starting snapshot retention deletion for [{}] snapshots", count);
        long startTime = nowNanoSupplier.getAsLong();
        final AtomicInteger deleted = new AtomicInteger(0);
        final AtomicInteger failed = new AtomicInteger(0);
        final GroupedActionListener<Void> allDeletesListener =
                new GroupedActionListener<>(ActionListener.runAfter(listener.map(v -> null),
                        () -> {
                            TimeValue totalElapsedTime = TimeValue.timeValueNanos(nowNanoSupplier.getAsLong() - startTime);
                            logger.debug("total elapsed time for deletion of [{}] snapshots: {}", deleted, totalElapsedTime);
                            slmStats.deletionTime(totalElapsedTime);
                        }), snapshotsToDelete.size());
        for (Map.Entry<String, List<Tuple<SnapshotId, String>>> entry : snapshotsToDelete.entrySet()) {
            String repo = entry.getKey();
            List<Tuple<SnapshotId, String>> snapshots = entry.getValue();
            if (snapshots.isEmpty() == false) {
                deleteSnapshots(slmStats, deleted, failed, repo, snapshots, allDeletesListener);
            }
        }
    }

    private void deleteSnapshots(SnapshotLifecycleStats slmStats, AtomicInteger deleted, AtomicInteger failed, String repo,
                                 List<Tuple<SnapshotId, String>> snapshots, ActionListener<Void> listener) {

        final ActionListener<Void> allDeletesListener =
                new GroupedActionListener<>(listener.map(v -> null), snapshots.size());
        for (Tuple<SnapshotId, String> info : snapshots) {
            final SnapshotId snapshotId = info.v1();
            if (runningDeletions.add(snapshotId) == false) {
                // snapshot is already being deleted, no need to start another delete job for it
                allDeletesListener.onResponse(null);
                continue;
            }
            boolean success = false;
            try {
                final String policyId = info.v2();
                final long deleteStartTime = nowNanoSupplier.getAsLong();
                // TODO: Use snapshot multi-delete instead of this loop if all nodes in the cluster support it
                //       i.e are newer or equal to SnapshotsService#MULTI_DELETE_VERSION
                deleteSnapshot(policyId, repo, snapshotId, slmStats, ActionListener.runAfter(
                        ActionListener.wrap(acknowledgedResponse -> {
                            deleted.incrementAndGet();
                            assert acknowledgedResponse.isAcknowledged();
                            historyStore.putAsync(SnapshotHistoryItem.deletionSuccessRecord(Instant.now().toEpochMilli(),
                                    snapshotId.getName(), policyId, repo));
                            allDeletesListener.onResponse(null);
                        }, e -> {
                            failed.incrementAndGet();
                            try {
                                final SnapshotHistoryItem result = SnapshotHistoryItem.deletionFailureRecord(Instant.now().toEpochMilli(),
                                    snapshotId.getName(), policyId, repo, e);
                                historyStore.putAsync(result);
                            } catch (IOException ex) {
                                // This shouldn't happen unless there's an issue with serializing the original exception
                                logger.error(new ParameterizedMessage(
                                        "failed to record snapshot deletion failure for snapshot lifecycle policy [{}]",
                                        policyId), ex);
                            } finally {
                                allDeletesListener.onFailure(e);
                            }
                        }), () -> {
                            runningDeletions.remove(snapshotId);
                            long finishTime = nowNanoSupplier.getAsLong();
                            TimeValue deletionTime = TimeValue.timeValueNanos(finishTime - deleteStartTime);
                            logger.debug("elapsed time for deletion of [{}] snapshot: {}", snapshotId, deletionTime);
                        }));
                success = true;
            } catch (Exception e) {
                listener.onFailure(e);
            } finally {
                if (success == false) {
                    runningDeletions.remove(snapshotId);
                }
            }
        }
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
        // don't time out on this request to not produce failed SLM runs in case of a temporarily slow master node
        client.admin().cluster().prepareDeleteSnapshot(repo, snapshot.getName()).setMasterNodeTimeout(TimeValue.MAX_VALUE).execute(
            ActionListener.wrap(acknowledgedResponse -> {
                    slmStats.snapshotDeleted(slmPolicy);
                    listener.onResponse(acknowledgedResponse);
                },
                e -> {
                    try {
                        logger.warn(new ParameterizedMessage("[{}] failed to delete snapshot [{}] for retention",
                                repo, snapshot), e);
                        slmStats.snapshotDeleteFailure(slmPolicy);
                    } finally {
                        listener.onFailure(e);
                    }
                }));
    }

    void updateStateWithStats(SnapshotLifecycleStats newStats) {
        clusterService.submitStateUpdateTask("update_slm_stats", new UpdateSnapshotLifecycleStatsTask(newStats));
    }
}
