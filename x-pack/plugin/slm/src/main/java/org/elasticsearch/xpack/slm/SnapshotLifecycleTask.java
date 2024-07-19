/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicySecurityClient;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
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

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentSLMMode;

public class SnapshotLifecycleTask implements SchedulerEngine.Listener {

    private static final Logger logger = LogManager.getLogger(SnapshotLifecycleTask.class);

    private final Client client;
    private final ClusterService clusterService;
    private final SnapshotHistoryStore historyStore;

    public SnapshotLifecycleTask(final Client client, final ClusterService clusterService, final SnapshotHistoryStore historyStore) {
        this.client = client;
        this.clusterService = clusterService;
        this.historyStore = historyStore;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        logger.debug("snapshot lifecycle policy task triggered from job [{}]", event.getJobName());

        final Optional<String> snapshotName = maybeTakeSnapshot(event.getJobName(), client, clusterService, historyStore);

        // Would be cleaner if we could use Optional#ifPresentOrElse
        snapshotName.ifPresent(
            name -> logger.info(
                "snapshot lifecycle policy job [{}] issued new snapshot creation for [{}] successfully",
                event.getJobName(),
                name
            )
        );

        if (snapshotName.isPresent() == false) {
            logger.warn("snapshot lifecycle policy for job [{}] no longer exists, snapshot not created", event.getJobName());
        }
    }

    /**
     * For the given job id (a combination of policy id and version), issue a create snapshot
     * request. On a successful or failed create snapshot issuing the state is stored in the cluster
     * state in the policy's metadata
     * @return An optional snapshot name if the request was issued successfully
     */
    public static Optional<String> maybeTakeSnapshot(
        final String jobId,
        final Client client,
        final ClusterService clusterService,
        final SnapshotHistoryStore historyStore
    ) {
        Optional<SnapshotLifecyclePolicyMetadata> maybeMetadata = getSnapPolicyMetadata(jobId, clusterService.state());
        String snapshotName = maybeMetadata.map(policyMetadata -> {
            // don't time out on this request to not produce failed SLM runs in case of a temporarily slow master node
            CreateSnapshotRequest request = policyMetadata.getPolicy().toRequest(TimeValue.MAX_VALUE);
            final SnapshotId snapshotId = new SnapshotId(request.snapshot(), request.uuid());

            submitUnbatchedTask(
                clusterService,
                "slm-pre-register-snapshot-" + policyMetadata.getPolicy().getId(),
                new PreRegisterSLMRun(policyMetadata.getPolicy().getId(), snapshotId, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
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
                                // Check that there are no failed shards, since the request may not entirely
                                // fail, but may still have failures (such as in the case of an aborted snapshot)
                                if (snapInfo.failedShards() == 0) {
                                    long snapshotStartTime = snapInfo.startTime();
                                    final long timestamp = Instant.now().toEpochMilli();
                                    submitUnbatchedTask(
                                        clusterService,
                                        "slm-record-success-" + policyMetadata.getPolicy().getId(),
                                        WriteJobStatus.success(policyMetadata.getPolicy().getId(), snapshotId, snapshotStartTime, timestamp)
                                    );
                                    historyStore.putAsync(
                                        SnapshotHistoryItem.creationSuccessRecord(timestamp, policyMetadata.getPolicy(), request.snapshot())
                                    );
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
                                logger.error(
                                    "failed to create snapshot for snapshot lifecycle policy [{}]: {}",
                                    policyMetadata.getPolicy().getId(),
                                    e
                                );
                                final long timestamp = Instant.now().toEpochMilli();
                                submitUnbatchedTask(
                                    clusterService,
                                    "slm-record-failure-" + policyMetadata.getPolicy().getId(),
                                    WriteJobStatus.failure(policyMetadata.getPolicy().getId(), snapshotId, timestamp, e)
                                );
                                final SnapshotHistoryItem failureRecord;
                                try {
                                    failureRecord = SnapshotHistoryItem.creationFailureRecord(
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
                            }
                        });
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            "failed to start snapshot for snapshot lifecycle policy [{}]: {}",
                            policyMetadata.getPolicy().getId(),
                            e
                        );
                    }
                })
            );

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
    static Optional<SnapshotLifecyclePolicyMetadata> getSnapPolicyMetadata(final String jobId, final ClusterState state) {
        return Optional.ofNullable((SnapshotLifecycleMetadata) state.metadata().custom(SnapshotLifecycleMetadata.TYPE))
            .map(SnapshotLifecycleMetadata::getSnapshotConfigurations)
            .flatMap(
                configMap -> configMap.values()
                    .stream()
                    .filter(policyMeta -> jobId.equals(SnapshotLifecycleService.getJobId(policyMeta)))
                    .findFirst()
            );
    }

    public static String exceptionToString(Exception ex) {
        return Strings.toString((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, params, ex);
            return builder;
        }, ToXContent.EMPTY_PARAMS);
    }

    static Set<SnapshotId> currentlyRunningSnapshots(String policyId, ClusterState clusterState) {
        SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE);
        if (snapshots == null) {
            return Set.of();
        }
        Set<SnapshotId> currentlyRunning = new HashSet<>();
        for (final List<SnapshotsInProgress.Entry> entriesForRepo : snapshots.entriesByRepo()) {
            for (SnapshotsInProgress.Entry entry : entriesForRepo) {
                Map<String, Object> metadata = entry.userMetadata();
                if (metadata != null && policyId.equals(metadata.get(SnapshotsService.POLICY_ID_METADATA_FIELD))) {
                    currentlyRunning.add(entry.snapshot().getSnapshotId());
                }
            }
        }
        return currentlyRunning;
    }

    /**
     * A cluster state update task which adds a snapshotId to a set of preRegistered snapshots.
     * This happens before snapshot creation so that, if the snapshot fails and the fact that
     * it failed is lost due to a master shutdown, its failure can subsequently be inferred
     * from the fact that the snapshotId is in the preRegistered set but the snapshot
     * is no longer running. In this case, the failure stats are updated in the run of this
     * task for the following snapshot.
     */
    static class PreRegisterSLMRun extends ClusterStateUpdateTask {
        private final String policyName;
        private final SnapshotId snapshotId;
        private final ActionListener<Void> listener;

        PreRegisterSLMRun(String policyName, SnapshotId snapshotId, ActionListener<Void> listener) {
            this.policyName = policyName;
            this.snapshotId = snapshotId;
            this.listener = listener;
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            listener.onResponse(null);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            SnapshotLifecycleMetadata snapMeta = currentState.metadata()
                .custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);

            Map<String, SnapshotLifecyclePolicyMetadata> snapLifecycles = new HashMap<>(snapMeta.getSnapshotConfigurations());
            SnapshotLifecyclePolicyMetadata policyMetadata = snapLifecycles.get(policyName);
            if (policyMetadata == null) {
                logger.debug("failed to pre-register snapshot [{}] in policy [{}]: policy not found", snapshotId.getName(), policyName);
                return currentState;
            }

            List<SnapshotId> preRegisteredSnapshots = policyMetadata.getPreRegisteredSnapshots();
            Set<SnapshotId> runningSnapshots = currentlyRunningSnapshots(policyName, currentState);
            SnapshotLifecyclePolicyMetadata.Builder newPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder(policyMetadata);
            SnapshotLifecycleStats stats = snapMeta.getStats();
            long unrecordedFailures = 0;
            List<SnapshotId> newPreRegisteredSnapshots = new ArrayList<>();
            for (SnapshotId snapshot : preRegisteredSnapshots) {
                if (runningSnapshots.contains(snapshot)) {
                    newPreRegisteredSnapshots.add(snapshot);
                } else {
                    stats.snapshotFailed(policyName);
                    unrecordedFailures++;
                    newPolicyMetadata.setLastFailure(
                        new SnapshotInvocationRecord(
                            snapshot.getName(),
                            null,
                            Instant.now().toEpochMilli(),
                            String.format(
                                Locale.ROOT,
                                "found pre-registered snapshot [%s] which is no longer running, assuming failed.",
                                snapshot.getName()
                            )
                        )
                    );
                }
            }

            assert newPreRegisteredSnapshots.contains(snapshotId) == false : "the same snapshotId cannot be preRegistered twice";
            newPreRegisteredSnapshots.add(snapshotId);
            newPolicyMetadata.setPreRegisteredSnapshots(newPreRegisteredSnapshots);
            newPolicyMetadata.setInvocationsSinceLastSuccess(policyMetadata.getInvocationsSinceLastSuccess() + unrecordedFailures);

            snapLifecycles.put(policyName, newPolicyMetadata.build());
            SnapshotLifecycleMetadata lifecycleMetadata = new SnapshotLifecycleMetadata(
                snapLifecycles,
                currentSLMMode(currentState),
                stats
            );
            Metadata currentMeta = currentState.metadata();
            return ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentMeta).putCustom(SnapshotLifecycleMetadata.TYPE, lifecycleMetadata))
                .build();
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(
                "failed to pre-register snapshot policy execution for snapshot [{}] in policy [{}]: {}",
                snapshotId.getName(),
                policyName,
                e
            );
        }
    }

    /**
     * A cluster state update task to write the result of a snapshot job to the cluster metadata for the associated policy.
     */
    static class WriteJobStatus extends ClusterStateUpdateTask {

        private final String policyName;
        private final SnapshotId snapshotId;
        private final long snapshotStartTime;
        private final long snapshotFinishTime;
        private final Optional<Exception> exception;

        private WriteJobStatus(
            String policyName,
            SnapshotId snapshotId,
            long snapshotStartTime,
            long snapshotFinishTime,
            Optional<Exception> exception
        ) {
            this.policyName = policyName;
            this.snapshotId = snapshotId;
            this.exception = exception;
            this.snapshotStartTime = snapshotStartTime;
            this.snapshotFinishTime = snapshotFinishTime;
        }

        static WriteJobStatus success(String policyId, SnapshotId snapshotId, long snapshotStartTime, long snapshotFinishTime) {
            return new WriteJobStatus(policyId, snapshotId, snapshotStartTime, snapshotFinishTime, Optional.empty());
        }

        static WriteJobStatus failure(String policyId, SnapshotId snapshotId, long timestamp, Exception exception) {
            return new WriteJobStatus(policyId, snapshotId, timestamp, timestamp, Optional.of(exception));
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            SnapshotLifecycleMetadata snapMeta = currentState.metadata().custom(SnapshotLifecycleMetadata.TYPE);

            assert snapMeta != null : "this should never be called while the snapshot lifecycle cluster metadata is null";
            if (snapMeta == null) {
                logger.error(
                    "failed to record snapshot [{}] for snapshot [{}] in policy [{}]: snapshot lifecycle metadata is null",
                    exception.isPresent() ? "failure" : "success",
                    snapshotId.getName(),
                    policyName
                );
                return currentState;
            }

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

            SnapshotLifecyclePolicyMetadata.Builder newPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder(policyMetadata);
            final SnapshotLifecycleStats stats = snapMeta.getStats();

            if (exception.isPresent()) {
                stats.snapshotFailed(policyName);
                newPolicyMetadata.setLastFailure(
                    new SnapshotInvocationRecord(
                        snapshotId.getName(),
                        null,
                        snapshotFinishTime,
                        exception.map(SnapshotLifecycleTask::exceptionToString).orElse(null)
                    )
                );
                newPolicyMetadata.setInvocationsSinceLastSuccess(policyMetadata.getInvocationsSinceLastSuccess() + 1L);
            } else {
                stats.snapshotTaken(policyName);
                newPolicyMetadata.setLastSuccess(
                    new SnapshotInvocationRecord(snapshotId.getName(), snapshotStartTime, snapshotFinishTime, null)
                );
                newPolicyMetadata.setInvocationsSinceLastSuccess(0L);
            }

            List<SnapshotId> preRegisteredSnapshots = new ArrayList<>(policyMetadata.getPreRegisteredSnapshots());
            assert preRegisteredSnapshots.contains(snapshotId)
                : "PreRegisteredSnapshots must contain a running snapshotId until a success/failure is emitted to acquiesce it.";
            preRegisteredSnapshots.remove(snapshotId);
            newPolicyMetadata.setPreRegisteredSnapshots(preRegisteredSnapshots);

            snapLifecycles.put(policyName, newPolicyMetadata.build());
            SnapshotLifecycleMetadata lifecycleMetadata = new SnapshotLifecycleMetadata(
                snapLifecycles,
                currentSLMMode(currentState),
                stats
            );
            Metadata currentMeta = currentState.metadata();
            return ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentMeta).putCustom(SnapshotLifecycleMetadata.TYPE, lifecycleMetadata))
                .build();
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(
                "failed to record snapshot policy execution status [{}] for snapshot [{}] in policy [{}]: {}",
                exception.isPresent() ? "failure" : "success",
                snapshotId.getName(),
                policyName,
                e
            );
        }
    }
}
