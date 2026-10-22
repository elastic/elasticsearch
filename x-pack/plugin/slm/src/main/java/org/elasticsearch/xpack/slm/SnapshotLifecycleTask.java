/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.RegisteredPolicySnapshots;
import org.elasticsearch.snapshots.RegisteredPolicySnapshots.PolicySnapshot;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicySecurityClient;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.slm.history.SnapshotHistoryItem;
import org.elasticsearch.xpack.slm.history.SnapshotHistoryStore;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentSLMMode;
import static org.elasticsearch.xpack.slm.SnapshotLifecycleService.getJobId;

public class SnapshotLifecycleTask implements SchedulerEngine.Listener {

    private static final Logger logger = LogManager.getLogger(SnapshotLifecycleTask.class);

    private final ProjectId projectId;
    private final Client client;
    private final ClusterService clusterService;
    private final SnapshotHistoryStore historyStore;

    public SnapshotLifecycleTask(
        final ProjectId projectId,
        final Client client,
        final ClusterService clusterService,
        final SnapshotHistoryStore historyStore
    ) {
        this.projectId = projectId;
        this.client = new OriginSettingClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN);
        this.clusterService = clusterService;
        this.historyStore = historyStore;
    }

    /**
     * Find {@link SnapshotId}s for the given policy that are registered but no longer running.
     * @param projectState the current project state
     * @param policyId the policy id for which to find completed registered snapshots
     * @return snapshot ids that are registered for {@code policyId} and not present in {@link SnapshotsInProgress}
     */
    static List<SnapshotId> findCompletedRegisteredSnapshotIds(ProjectState projectState, String policyId) {
        Set<SnapshotId> runningSnapshots = currentlyRunningSnapshots(projectState.cluster());

        RegisteredPolicySnapshots registeredSnapshots = projectState.metadata()
            .custom(RegisteredPolicySnapshots.TYPE, RegisteredPolicySnapshots.EMPTY);

        return registeredSnapshots.getSnapshots()
            .stream()
            // look for snapshots of this SLM policy, leave the rest to the policy that owns it
            .filter(policySnapshot -> policySnapshot.getPolicy().equals(policyId))
            // look for snapshots that are no longer running
            .filter(policySnapshot -> runningSnapshots.contains(policySnapshot.getSnapshotId()) == false)
            .map(PolicySnapshot::getSnapshotId)
            .toList();
    }

    /**
     * Snapshot infos retrieved for registered snapshots that were not running at lookup time, plus the ids that were queried.
     * The queried ids are used by {@link WriteJobStatus} to distinguish "missing from the repository" (infer failure) from
     * "was still running at lookup, so we never fetched it" (leave registered for a later cleanup).
     * <p>
     * Every {@link SnapshotInfo} should correspond to a queried id; infos are a subset of {@code queriedSnapshotIds}
     * (some queried snapshots may be missing from the repository). {@code GetSnapshots} is keyed by snapshot name, while
     * {@link SnapshotId} equality includes the uuid, so a deleted snapshot whose name was reused can yield info that is
     * not in the queried set. That mismatch is ignored rather than thrown so SLM cleanup can continue.
     */
    record CompletedRegisteredSnapshotInfos(Set<SnapshotId> queriedSnapshotIds, List<SnapshotInfo> snapshotInfos) {
        CompletedRegisteredSnapshotInfos {
            queriedSnapshotIds = Set.copyOf(queriedSnapshotIds);
            List<SnapshotInfo> matchedSnapshotInfos = new ArrayList<>(snapshotInfos.size());
            for (SnapshotInfo snapshotInfo : snapshotInfos) {
                if (queriedSnapshotIds.contains(snapshotInfo.snapshotId())) {
                    matchedSnapshotInfos.add(snapshotInfo);
                } else {
                    // GetSnapshots is keyed by name, while SnapshotId equality includes uuid. Do not throw:
                    // that would fail every subsequent SLM run while a stale registered name is reused.
                    logger.debug(
                        () -> "snapshot info for [" + snapshotInfo.snapshotId() + "] was not in the queried snapshot id set; ignoring it"
                    );
                    assert false : "snapshot info for [" + snapshotInfo.snapshotId() + "] was not in the queried snapshot id set";
                }
            }
            snapshotInfos = List.copyOf(matchedSnapshotInfos);
        }

        static final CompletedRegisteredSnapshotInfos EMPTY = new CompletedRegisteredSnapshotInfos(Set.of(), List.of());
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        logger.debug("snapshot lifecycle policy task triggered from job [{}]", event.jobName());
        ProjectMetadata projectMetadata = clusterService.state().getMetadata().getProject(projectId);
        final Optional<String> snapshotName = maybeTakeSnapshot(projectMetadata, event.jobName(), client, clusterService, historyStore);

        // Would be cleaner if we could use Optional#ifPresentOrElse
        snapshotName.ifPresent(
            name -> logger.info(
                "snapshot lifecycle policy job [{}] issued new snapshot creation for [{}] successfully",
                event.jobName(),
                name
            )
        );

        if (snapshotName.isPresent() == false) {
            logger.warn("snapshot lifecycle policy for job [{}] no longer exists, snapshot not created", event.jobName());
        }
    }

    /**
     * Find registered snapshots for {@code policyId} that are no longer running, and fetch their {@link SnapshotInfo}.
     * <p>
     * These entries should already have been removed from the registered set by {@link WriteJobStatus} when they completed. When they were
     * not (for example because the master shut down during an SLM run and {@code WriteJobStatus} failed), the next SLM run cleans them up
     * and retroactively records stats from their snapshot info.
     * <p>
     * The listener receives both the fetched infos and the set of snapshot ids that were queried. {@link WriteJobStatus} uses that queried
     * set so that a snapshot still running at lookup time, which later finishes before the cluster-state update, is left registered instead
     * of being inferred as a failure when its info was never fetched.
     */
    private static void findCompletedRegisteredSnapshotInfo(
        final ProjectState projectState,
        final String policyId,
        final Client client,
        final ActionListener<CompletedRegisteredSnapshotInfos> listener
    ) {
        var completedSnapshotIds = findCompletedRegisteredSnapshotIds(projectState, policyId);

        if (completedSnapshotIds.isEmpty() == false) {
            var policyMetadata = getSnapPolicyMetadataById(projectState.metadata(), policyId);
            if (policyMetadata.isPresent() == false) {
                listener.onFailure(new IllegalStateException(format("snapshot lifecycle policy [%s] no longer exists", policyId)));
                return;
            }
            SnapshotLifecyclePolicy policy = policyMetadata.get().getPolicy();
            final Set<SnapshotId> queriedSnapshotIds = Set.copyOf(completedSnapshotIds);

            GetSnapshotsRequest request = new GetSnapshotsRequest(
                TimeValue.MAX_VALUE,    // do not time out internal request in case of slow master node
                new String[] { policy.getRepository() },
                completedSnapshotIds.stream().map(SnapshotId::getName).toArray(String[]::new)
            );
            request.ignoreUnavailable(true);
            request.includeIndexNames(false);

            client.admin()
                .cluster()
                .execute(
                    TransportGetSnapshotsAction.TYPE,
                    request,
                    ActionListener.wrap(
                        response -> listener.onResponse(new CompletedRegisteredSnapshotInfos(queriedSnapshotIds, response.getSnapshots())),
                        listener::onFailure
                    )
                );
        } else {
            listener.onResponse(CompletedRegisteredSnapshotInfos.EMPTY);
        }
    }

    /**
     * For the given job id (a combination of policy id and version), issue a create snapshot
     * request. On a successful or failed create snapshot issuing the state is stored in the cluster
     * state in the policy's metadata
     * @return An optional snapshot name if the request was issued successfully
     */
    public static Optional<String> maybeTakeSnapshot(
        final ProjectMetadata projectMetadata,
        final String jobId,
        final Client client,
        final ClusterService clusterService,
        final SnapshotHistoryStore historyStore
    ) {
        ProjectId projectId = projectMetadata.id();
        Optional<SnapshotLifecyclePolicyMetadata> maybeMetadata = getSnapPolicyMetadata(projectMetadata, jobId);
        String snapshotName = maybeMetadata.map(policyMetadata -> {
            String policyId = policyMetadata.getPolicy().getId();
            // don't time out on this request to not produce failed SLM runs in case of a temporarily slow master node
            CreateSnapshotRequest request = policyMetadata.getPolicy().toRequest(TimeValue.MAX_VALUE);
            final SnapshotId snapshotId = new SnapshotId(request.snapshot(), request.uuid());

            final LifecyclePolicySecurityClient clientWithHeaders = new LifecyclePolicySecurityClient(
                client,
                ClientHelper.INDEX_LIFECYCLE_ORIGIN,
                policyMetadata.getHeaders()
            );
            logger.info(
                "snapshot lifecycle policy [{}] issuing create snapshot [{}]",
                policyMetadata.getPolicy().getId(),
                request.snapshot()
            );
            clientWithHeaders.admin().cluster().createSnapshot(request, new ActionListener<>() {
                @Override
                public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                    logger.debug(
                        "snapshot response for [{}]: {}",
                        policyMetadata.getPolicy().getId(),
                        Strings.toString(createSnapshotResponse)
                    );
                    final SnapshotInfo snapInfo = createSnapshotResponse.getSnapshotInfo();
                    assert snapInfo != null : "completed snapshot info is null";
                    // Check that there are no failed shards, since the request may not entirely
                    // fail, but may still have failures (such as in the case of an aborted snapshot)
                    if (snapInfo.failedShards() == 0) {
                        long snapshotStartTime = snapInfo.startTime();
                        final long timestamp = Instant.now().toEpochMilli();
                        historyStore.putAsync(
                            SnapshotHistoryItem.creationSuccessRecord(timestamp, policyMetadata.getPolicy(), request.snapshot())
                        );

                        // retrieve the current project state after snapshot is completed, since snapshotting can take a while
                        ProjectState currentProjectState = clusterService.state().projectState(projectId);
                        findCompletedRegisteredSnapshotInfo(currentProjectState, policyId, client, new ActionListener<>() {
                            @Override
                            public void onResponse(CompletedRegisteredSnapshotInfos completedRegisteredSnapshotInfos) {
                                submitUnbatchedTask(
                                    clusterService,
                                    "slm-record-success-" + policyId,
                                    WriteJobStatus.success(
                                        projectId,
                                        policyId,
                                        snapshotId,
                                        snapshotStartTime,
                                        timestamp,
                                        completedRegisteredSnapshotInfos
                                    )
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn(() -> format("failed to retrieve stale registered snapshots for job [%s]", jobId), e);
                                // still record the successful snapshot; leave other registered snapshots for a later run
                                submitUnbatchedTask(
                                    clusterService,
                                    "slm-record-success-" + policyId,
                                    WriteJobStatus.success(
                                        projectId,
                                        policyId,
                                        snapshotId,
                                        snapshotStartTime,
                                        timestamp,
                                        CompletedRegisteredSnapshotInfos.EMPTY
                                    )
                                );
                            }
                        });
                    } else {
                        int failures = snapInfo.failedShards();
                        int total = snapInfo.totalShards();
                        final SnapshotException e = new SnapshotException(
                            request.repository(),
                            request.snapshot(),
                            "failed to create snapshot successfully, " + failures + " out of " + total + " total shards failed"
                        );
                        // SnapshotInfo means the snapshot was started and registered; never treat as never-registered.
                        recordSnapshotFailure(e, false, clusterService.state().projectState(projectId));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // Capture never-registered from the same cluster state used for the completed-snapshot lookup.
                    // If it is registered now, a concurrent cleanup may remove it before WriteJobStatus runs; skip then.
                    // SnapshotInfo failures pass recordFailureIfUnregistered=false so a peer cleanup is not confused
                    // with a never-registered CreateSnapshot failure (#136759).
                    ProjectState currentProjectState = clusterService.state().projectState(projectId);
                    final boolean snapshotNeverRegistered = currentProjectState.metadata()
                        .custom(RegisteredPolicySnapshots.TYPE, RegisteredPolicySnapshots.EMPTY)
                        .contains(snapshotId) == false;
                    recordSnapshotFailure(e, snapshotNeverRegistered, currentProjectState);
                }

                private void recordSnapshotFailure(Exception e, boolean recordFailureIfUnregistered, ProjectState currentProjectState) {
                    logger.warn(
                        () -> format("failed to create snapshot for snapshot lifecycle policy [%s]", policyMetadata.getPolicy().getId()),
                        e
                    );
                    final long timestamp = Instant.now().toEpochMilli();

                    try {
                        final SnapshotHistoryItem failureRecord = SnapshotHistoryItem.creationFailureRecord(
                            timestamp,
                            policyMetadata.getPolicy(),
                            request.snapshot(),
                            e
                        );
                        historyStore.putAsync(failureRecord);
                    } catch (IOException ex) {
                        // This shouldn't happen unless there's an issue with serializing the original exception, which
                        // shouldn't happen
                        logger.error(
                            () -> format(
                                "failed to record snapshot creation failure for snapshot lifecycle policy [%s]",
                                policyMetadata.getPolicy().getId()
                            ),
                            e
                        );
                    }

                    findCompletedRegisteredSnapshotInfo(currentProjectState, policyId, client, new ActionListener<>() {
                        @Override
                        public void onResponse(CompletedRegisteredSnapshotInfos completedRegisteredSnapshotInfos) {
                            submitUnbatchedTask(
                                clusterService,
                                "slm-record-failure-" + policyMetadata.getPolicy().getId(),
                                WriteJobStatus.failure(
                                    projectId,
                                    policyMetadata.getPolicy().getId(),
                                    snapshotId,
                                    timestamp,
                                    completedRegisteredSnapshotInfos,
                                    e,
                                    recordFailureIfUnregistered
                                )
                            );
                        }

                        @Override
                        public void onFailure(Exception getSnapshotsException) {
                            logger.warn(
                                () -> format("failed to retrieve stale registered snapshots for job [%s]", jobId),
                                getSnapshotsException
                            );
                            // still record the failed snapshot; leave other registered snapshots for a later run
                            submitUnbatchedTask(
                                clusterService,
                                "slm-record-failure-" + policyMetadata.getPolicy().getId(),
                                WriteJobStatus.failure(
                                    projectId,
                                    policyMetadata.getPolicy().getId(),
                                    snapshotId,
                                    timestamp,
                                    CompletedRegisteredSnapshotInfos.EMPTY,
                                    e,
                                    recordFailureIfUnregistered
                                )
                            );
                        }
                    });
                }
            });
            return request.snapshot();
        }).orElse(null);

        return Optional.ofNullable(snapshotName);
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static void submitUnbatchedTask(
        ClusterService clusterService,
        @SuppressWarnings("SameParameterValue") String source,
        ClusterStateUpdateTask task
    ) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    /**
     * For the given job id, return an optional policy metadata object, if one exists
     */
    static Optional<SnapshotLifecyclePolicyMetadata> getSnapPolicyMetadata(final ProjectMetadata projectMetadata, final String jobId) {
        return Optional.ofNullable((SnapshotLifecycleMetadata) projectMetadata.custom(SnapshotLifecycleMetadata.TYPE))
            .map(SnapshotLifecycleMetadata::getSnapshotConfigurations)
            .flatMap(configMap -> configMap.values().stream().filter(policyMeta -> jobId.equals(getJobId(policyMeta))).findFirst());
    }

    /**
     * For the given policy id, return an optional policy metadata object, if one exists
     */
    static Optional<SnapshotLifecyclePolicyMetadata> getSnapPolicyMetadataById(
        final ProjectMetadata projectMetadata,
        final String policyId
    ) {
        return Optional.ofNullable((SnapshotLifecycleMetadata) projectMetadata.custom(SnapshotLifecycleMetadata.TYPE))
            .map(metadata -> metadata.getSnapshotConfigurations().get(policyId));
    }

    public static String exceptionToString(Exception ex) {
        return Strings.toString((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, params, ex);
            return builder;
        }, ToXContent.EMPTY_PARAMS);
    }

    static Set<SnapshotId> currentlyRunningSnapshots(ClusterState clusterState) {
        final SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        final Set<SnapshotId> currentlyRunning = new HashSet<>();
        @FixForMultiProject(description = "replace with snapshots.entriesByRepo(ProjectId) when SLM is project aware")
        final Iterable<List<SnapshotsInProgress.Entry>> entriesByRepo = snapshots.entriesByRepo();
        for (final List<SnapshotsInProgress.Entry> entriesForRepo : entriesByRepo) {
            for (SnapshotsInProgress.Entry entry : entriesForRepo) {
                currentlyRunning.add(entry.snapshot().getSnapshotId());
            }
        }
        return currentlyRunning;
    }

    static SnapshotInvocationRecord buildFailedSnapshotRecord(SnapshotId snapshot) {
        return new SnapshotInvocationRecord(
            snapshot.getName(),
            null,
            Instant.now().toEpochMilli(),
            String.format(Locale.ROOT, "found registered snapshot [%s] which is no longer running, assuming failed.", snapshot.getName())
        );
    }

    static SnapshotInvocationRecord buildSnapshotRecord(SnapshotInfo snapshotInfo, @Nullable String details) {
        return new SnapshotInvocationRecord(snapshotInfo.snapshotId().getName(), snapshotInfo.startTime(), snapshotInfo.endTime(), details);
    }

    static boolean isSnapshotSuccessful(SnapshotInfo snapshotInfo) {
        return snapshotInfo.state() != null && snapshotInfo.state().completed() && snapshotInfo.failedShards() == 0;
    }

    /**
     * A cluster state update task to write the result of a snapshot job to the cluster metadata for the associated policy.
     */
    static class WriteJobStatus extends ClusterStateUpdateTask {

        private final ProjectId projectId;
        private final String policyName;
        private final SnapshotId snapshotId;
        private final long snapshotStartTime;
        private final long snapshotFinishTime;
        private final Optional<Exception> exception;
        // preloaded snapshot info for registered snapshots that are no longer running
        private final List<SnapshotInfo> registeredSnapshotInfo;
        /**
         * Snapshot ids that were not running when {@link SnapshotLifecycleTask#findCompletedRegisteredSnapshotInfo} ran and for which
         * we attempted to load {@link SnapshotInfo}. Only these may be inferred as failures when their info is missing. Registered
         * snapshots that finish between that lookup and this cluster state update are left registered for a later cleanup.
         */
        private final Set<SnapshotId> queriedSnapshotIds;
        /**
         * When true, this failure is for a snapshot that was never added to the registered set (e.g., CreateSnapshot failed before
         * registration). WriteJobStatus must still record failure stats even though the snapshot is unregistered. When false, an
         * unregistered initiating snapshot means another cleanup already recorded it - skip to avoid double-counting.
         * Ignored when {@link #queriedSnapshotIds} contains this snapshot: lookup saw it registered, so a missing registered
         * entry is a peer cleanup, not a never-registered failure.
         */
        private final boolean recordFailureIfUnregistered;

        private WriteJobStatus(
            ProjectId projectId,
            String policyName,
            SnapshotId snapshotId,
            long snapshotStartTime,
            long snapshotFinishTime,
            CompletedRegisteredSnapshotInfos completedRegisteredSnapshotInfos,
            Optional<Exception> exception,
            boolean recordFailureIfUnregistered
        ) {
            this.projectId = projectId;
            this.policyName = policyName;
            this.snapshotId = snapshotId;
            this.exception = exception;
            this.snapshotStartTime = snapshotStartTime;
            this.snapshotFinishTime = snapshotFinishTime;
            this.registeredSnapshotInfo = completedRegisteredSnapshotInfos.snapshotInfos();
            this.queriedSnapshotIds = completedRegisteredSnapshotInfos.queriedSnapshotIds();
            this.recordFailureIfUnregistered = recordFailureIfUnregistered;
            assert recordFailureIfUnregistered == false || queriedSnapshotIds.contains(snapshotId) == false
                : "snapshot [" + snapshotId + "] was flagged as never registered but appeared in the queried set";
        }

        static WriteJobStatus success(
            ProjectId projectId,
            String policyId,
            SnapshotId snapshotId,
            long snapshotStartTime,
            long snapshotFinishTime,
            CompletedRegisteredSnapshotInfos completedRegisteredSnapshotInfos
        ) {
            return new WriteJobStatus(
                projectId,
                policyId,
                snapshotId,
                snapshotStartTime,
                snapshotFinishTime,
                completedRegisteredSnapshotInfos,
                Optional.empty(),
                false
            );
        }

        static WriteJobStatus failure(
            ProjectId projectId,
            String policyId,
            SnapshotId snapshotId,
            long timestamp,
            CompletedRegisteredSnapshotInfos completedRegisteredSnapshotInfos,
            Exception exception,
            boolean recordFailureIfUnregistered
        ) {
            return new WriteJobStatus(
                projectId,
                policyId,
                snapshotId,
                timestamp,
                timestamp,
                completedRegisteredSnapshotInfos,
                Optional.of(exception),
                recordFailureIfUnregistered
            );
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            final ProjectMetadata project = currentState.metadata().getProject(projectId);
            SnapshotLifecycleMetadata snapMeta = project.custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
            RegisteredPolicySnapshots registeredSnapshots = project.custom(RegisteredPolicySnapshots.TYPE, RegisteredPolicySnapshots.EMPTY);

            Map<String, SnapshotLifecyclePolicyMetadata> snapLifecycles = new HashMap<>(snapMeta.getSnapshotConfigurations());
            SnapshotLifecyclePolicyMetadata policyMetadata = snapLifecycles.get(policyName);
            if (policyMetadata == null) {
                logger.warn(
                    "failed to record snapshot [{}] for snapshot [{}] in policy [{}]: policy not found",
                    exception.isPresent() ? "failure" : "success",
                    snapshotId.getName(),
                    policyName
                );
                return currentState;
            }

            Map<SnapshotId, SnapshotInfo> snapshotInfoById = registeredSnapshotInfo.stream()
                .collect(Collectors.toMap(SnapshotInfo::snapshotId, Function.identity()));

            final SnapshotLifecyclePolicyMetadata.Builder newPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder(policyMetadata);
            SnapshotLifecycleStats newStats = snapMeta.getStats();

            final boolean snapshotIsRegistered = registeredSnapshots.contains(snapshotId);
            final Set<SnapshotId> runningSnapshots = currentlyRunningSnapshots(currentState);
            final List<PolicySnapshot> newRegistered = new ArrayList<>();

            // go through the registered set to find stale snapshots and calculate stats
            for (PolicySnapshot registeredSnapshot : registeredSnapshots.getSnapshots()) {
                SnapshotId registeredSnapshotId = registeredSnapshot.getSnapshotId();
                if (registeredSnapshotId.equals(snapshotId)) {
                    // skip the snapshot just completed, it will be handled later
                    continue;
                }
                if (snapLifecycles.containsKey(registeredSnapshot.getPolicy()) == false) {
                    // the SLM policy no longer exists, just remove the snapshot from registered set
                    continue;
                }
                if (registeredSnapshot.getPolicy().equals(policyName) == false || runningSnapshots.contains(registeredSnapshotId)) {
                    // the snapshot is for another policy, or is still running,
                    // keep it in the registered set and leave it to that policy to clean up
                    newRegistered.add(registeredSnapshot);
                } else {
                    // the snapshot was completed and should be removed from registered snapshots, update state accordingly
                    SnapshotInfo snapshotInfo = snapshotInfoById.get(registeredSnapshotId);
                    if (snapshotInfo != null) {
                        if (isSnapshotSuccessful(snapshotInfo)) {
                            newStats = newStats.withTakenIncremented(policyName);
                            newPolicyMetadata.setInvocationsSinceLastSuccess(0L).setLastSuccess(buildSnapshotRecord(snapshotInfo, null));
                        } else {
                            newStats = newStats.withFailedIncremented(policyName);
                            newPolicyMetadata.incrementInvocationsSinceLastSuccess()
                                .setLastFailure(
                                    buildSnapshotRecord(
                                        snapshotInfo,
                                        format(
                                            "found failed snapshot [%s] in the registered SLM snapshot set",
                                            snapshotInfo.snapshotId().getName()
                                        )
                                    )
                                );
                        }
                    } else if (queriedSnapshotIds.contains(registeredSnapshotId)) {
                        // we looked up this snapshot because it was already not running, and its info is unavailable - assume failure
                        // so it is not stuck in the registered set forever
                        newStats = newStats.withFailedIncremented(policyName);
                        newPolicyMetadata.incrementInvocationsSinceLastSuccess()
                            .setLastFailure(buildFailedSnapshotRecord(registeredSnapshotId));
                    } else {
                        // not running now, but we never fetched its info (it was still running at lookup time). Leave it registered
                        // so a later SLM run can record the true outcome instead of inferring a failure.
                        newRegistered.add(registeredSnapshot);
                    }
                }
            }

            // Add stats from the just completed snapshot execution, unless another cleanup already recorded it.
            //
            // CreateSnapshot can fail before registration (e.g. missing index). Those failures set
            // recordFailureIfUnregistered so they are still counted, but only when lookup did not see this id.
            // If queriedSnapshotIds contains this snapshot, it was registered at lookup time, so a missing
            // registered entry means a peer cleanup already recorded it.
            //
            // Do not infer "already recorded" from lastSuccess/lastFailure. Those fields store only the most
            // recent snapshot name. Example: snapshots A and B both fail; A's WriteJobStatus records B's failure
            // (lastFailure=B) then A's own failure (lastFailure=A). When B's WriteJobStatus runs, lastFailure no
            // longer names B, so matching on that name would count B a second time.
            if (snapshotIsRegistered == false) {
                // If lookup saw this id, it was registered then; a peer cleanup must not be treated as never-registered.
                if (exception.isPresent() && recordFailureIfUnregistered && queriedSnapshotIds.contains(snapshotId) == false) {
                    // Expected for CreateSnapshot failures that never reached registration (e.g. missing index).
                    logger.debug(
                        "Snapshot [{}] not found in registered set after snapshot failure. Recording failure stats"
                            + " (snapshot failed before registration).",
                        snapshotId.getName()
                    );
                    newStats = newStats.withFailedIncremented(policyName);
                    newPolicyMetadata.setLastFailure(
                        new SnapshotInvocationRecord(
                            snapshotId.getName(),
                            null,
                            snapshotFinishTime,
                            exception.map(SnapshotLifecycleTask::exceptionToString).orElse(null)
                        )
                    );
                    newPolicyMetadata.incrementInvocationsSinceLastSuccess();
                } else {
                    logger.warn(
                        "Snapshot [{}] not found in registered set after snapshot {}. This means snapshot was"
                            + " already recorded by another snapshot's cleanup run.",
                        snapshotId.getName(),
                        exception.isPresent() ? "failure" : "completion"
                    );
                }
            } else if (exception.isPresent()) {
                newStats = newStats.withFailedIncremented(policyName);
                newPolicyMetadata.setLastFailure(
                    new SnapshotInvocationRecord(
                        snapshotId.getName(),
                        null,
                        snapshotFinishTime,
                        exception.map(SnapshotLifecycleTask::exceptionToString).orElse(null)
                    )
                );
                newPolicyMetadata.incrementInvocationsSinceLastSuccess();
            } else {
                newStats = newStats.withTakenIncremented(policyName);
                newPolicyMetadata.setLastSuccess(
                    new SnapshotInvocationRecord(snapshotId.getName(), snapshotStartTime, snapshotFinishTime, null)
                );
                newPolicyMetadata.setInvocationsSinceLastSuccess(0L);
            }

            snapLifecycles.put(policyName, newPolicyMetadata.build());
            SnapshotLifecycleMetadata lifecycleMetadata = new SnapshotLifecycleMetadata(snapLifecycles, currentSLMMode(project), newStats);
            return currentState.copyAndUpdateProject(
                project.id(),
                builder -> builder.putCustom(SnapshotLifecycleMetadata.TYPE, lifecycleMetadata)
                    .putCustom(RegisteredPolicySnapshots.TYPE, new RegisteredPolicySnapshots(newRegistered))
            );
        }

        @Override
        public void onFailure(Exception e) {
            logger.log(
                MasterService.isPublishFailureException(e) ? Level.INFO : Level.WARN,
                format(
                    "failed to record snapshot policy execution status [%s] for snapshot [%s] in policy [%s]",
                    exception.isPresent() ? "failure" : "success",
                    snapshotId.getName(),
                    policyName
                ),
                e
            );
        }
    }
}
