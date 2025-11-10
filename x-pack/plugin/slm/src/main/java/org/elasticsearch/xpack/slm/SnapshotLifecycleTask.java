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
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
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
import java.util.Collections;
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
     * Find {@link RegisteredPolicySnapshots} for the given policy that are no longer running.
     * @param projectState the current project state
     * @param policyId the policy id for which to find completed registered snapshots
     * @return a list of snapshot names
     */
    static List<String> findCompletedRegisteredSnapshotNames(ProjectState projectState, String policyId) {
        Set<SnapshotId> runningSnapshots = currentlyRunningSnapshots(projectState.cluster());

        RegisteredPolicySnapshots registeredSnapshots = projectState.metadata()
            .custom(RegisteredPolicySnapshots.TYPE, RegisteredPolicySnapshots.EMPTY);

        return registeredSnapshots.getSnapshots()
            .stream()
            // look for snapshots of this SLM policy, leave the rest to the policy that owns it
            .filter(policySnapshot -> policySnapshot.getPolicy().equals(policyId))
            // look for snapshots that are no longer running
            .filter(policySnapshot -> runningSnapshots.contains(policySnapshot.getSnapshotId()) == false)
            .map(policySnapshot -> policySnapshot.getSnapshotId().getName())
            .toList();
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
     * Find {@link RegisteredPolicySnapshots} that are no longer running, and fetch their snapshot info. These snapshots should have been
     * removed from the registered set by WriteJobStatus when they were completed. However, they were not removed likely due to the master
     * being shutdown at the same time of a SLM run, causing WriteJobStatus to fail. These registered snapshots will be cleaned up in the
     * next SLM run and their stats will be retroactively recorded in SLM cluster state based on their status.
     */
    private static void findCompletedRegisteredSnapshotInfo(
        final ProjectState projectState,
        final String policyId,
        final Client client,
        final ActionListener<List<SnapshotInfo>> listener
    ) {
        var snapshotNames = findCompletedRegisteredSnapshotNames(projectState, policyId);

        if (snapshotNames.isEmpty() == false) {
            var policyMetadata = getSnapPolicyMetadataById(projectState.metadata(), policyId);
            if (policyMetadata.isPresent() == false) {
                listener.onFailure(new IllegalStateException(format("snapshot lifecycle policy [%s] no longer exists", policyId)));
                return;
            }
            SnapshotLifecyclePolicy policy = policyMetadata.get().getPolicy();

            GetSnapshotsRequest request = new GetSnapshotsRequest(
                TimeValue.MAX_VALUE,    // do not time out internal request in case of slow master node
                new String[] { policy.getRepository() },
                snapshotNames.toArray(new String[0])
            );
            request.ignoreUnavailable(true);
            request.includeIndexNames(false);

            client.admin()
                .cluster()
                .execute(
                    TransportGetSnapshotsAction.TYPE,
                    request,
                    ActionListener.wrap(response -> listener.onResponse(response.getSnapshots()), listener::onFailure)
                );
        } else {
            listener.onResponse(Collections.emptyList());
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
                            public void onResponse(List<SnapshotInfo> snapshotInfo) {
                                submitUnbatchedTask(
                                    clusterService,
                                    "slm-record-success-" + policyId,
                                    WriteJobStatus.success(projectId, policyId, snapshotId, snapshotStartTime, timestamp, snapshotInfo)
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn(() -> format("failed to retrieve stale registered snapshots for job [%s]", jobId), e);
                                // still record the successful snapshot
                                submitUnbatchedTask(
                                    clusterService,
                                    "slm-record-success-" + policyId,
                                    WriteJobStatus.success(
                                        projectId,
                                        policyId,
                                        snapshotId,
                                        snapshotStartTime,
                                        timestamp,
                                        Collections.emptyList()
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
                        // Call the failure handler to register this as a failure and persist it
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    SnapshotHistoryStore.logErrorOrWarning(
                        logger,
                        clusterService.state(),
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

                    // retrieve the current project state after snapshot is completed, since snapshotting can take a while
                    ProjectState currentProjectState = clusterService.state().projectState(projectId);
                    findCompletedRegisteredSnapshotInfo(currentProjectState, policyId, client, new ActionListener<>() {
                        @Override
                        public void onResponse(List<SnapshotInfo> snapshotInfo) {
                            submitUnbatchedTask(
                                clusterService,
                                "slm-record-failure-" + policyMetadata.getPolicy().getId(),
                                WriteJobStatus.failure(
                                    projectId,
                                    policyMetadata.getPolicy().getId(),
                                    snapshotId,
                                    timestamp,
                                    snapshotInfo,
                                    e
                                )
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn(() -> format("failed to retrieve stale registered snapshots for job [%s]", jobId), e);
                            // still record the failed snapshot
                            submitUnbatchedTask(
                                clusterService,
                                "slm-record-failure-" + policyMetadata.getPolicy().getId(),
                                WriteJobStatus.failure(
                                    projectId,
                                    policyMetadata.getPolicy().getId(),
                                    snapshotId,
                                    timestamp,
                                    Collections.emptyList(),
                                    e
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

        private WriteJobStatus(
            ProjectId projectId,
            String policyName,
            SnapshotId snapshotId,
            long snapshotStartTime,
            long snapshotFinishTime,
            List<SnapshotInfo> registeredSnapshotInfo,
            Optional<Exception> exception
        ) {
            this.projectId = projectId;
            this.policyName = policyName;
            this.snapshotId = snapshotId;
            this.exception = exception;
            this.snapshotStartTime = snapshotStartTime;
            this.snapshotFinishTime = snapshotFinishTime;
            this.registeredSnapshotInfo = registeredSnapshotInfo;
        }

        static WriteJobStatus success(
            ProjectId projectId,
            String policyId,
            SnapshotId snapshotId,
            long snapshotStartTime,
            long snapshotFinishTime,
            List<SnapshotInfo> registeredSnapshotInfo
        ) {
            return new WriteJobStatus(
                projectId,
                policyId,
                snapshotId,
                snapshotStartTime,
                snapshotFinishTime,
                registeredSnapshotInfo,
                Optional.empty()
            );
        }

        static WriteJobStatus failure(
            ProjectId projectId,
            String policyId,
            SnapshotId snapshotId,
            long timestamp,
            List<SnapshotInfo> registeredSnapshotInfo,
            Exception exception
        ) {
            return new WriteJobStatus(
                projectId,
                policyId,
                snapshotId,
                timestamp,
                timestamp,
                registeredSnapshotInfo,
                Optional.of(exception)
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
            if (snapshotIsRegistered == false) {
                logger.warn(
                    "Snapshot [{}] not found in registered set after snapshot completion. This means snapshot was"
                        + " recorded as a failure by another snapshot's cleanup run.",
                    snapshotId.getName()
                );
            }

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
                    } else {
                        // either the snapshot no longer exist in the repo or its info failed to be retrieved, assume failure to clean it up
                        // so it is not stuck in the registered set forever
                        newStats = newStats.withFailedIncremented(policyName);
                        newPolicyMetadata.incrementInvocationsSinceLastSuccess()
                            .setLastFailure(buildFailedSnapshotRecord(registeredSnapshotId));
                    }
                }
            }

            // Add stats from the just completed snapshot execution
            if (exception.isPresent()) {
                newStats = newStats.withFailedIncremented(policyName);
                newPolicyMetadata.setLastFailure(
                    new SnapshotInvocationRecord(
                        snapshotId.getName(),
                        null,
                        snapshotFinishTime,
                        exception.map(SnapshotLifecycleTask::exceptionToString).orElse(null)
                    )
                );
                // If the snapshot was not registered, it means it was already counted as a failure by another snapshot's cleanup run
                // so we should not increment the invocationsSinceLastSuccess again.
                if (snapshotIsRegistered) {
                    newPolicyMetadata.incrementInvocationsSinceLastSuccess();
                }
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
                e instanceof NotMasterException ? Level.INFO : Level.ERROR,
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
