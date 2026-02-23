/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;

import java.time.Clock;
import java.util.List;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;

/**
 * A DLM step that takes a snapshot of the specified index into a configured repository.
 * <p>
 * The step is considered complete when the {@link #DLM_SNAPSHOT_COMPLETED_SETTING} index setting is present.
 * <p>
 * During execution, if an in-flight snapshot request has been running for longer than {@link #SNAPSHOT_TIMEOUT},
 * the step cancels it, deletes the partial work, and restarts the snapshot. If a completed snapshot exists in
 * the repository but the completion setting is absent (and no snapshot is currently running in the cluster
 * for this index), the snapshot is deleted and recreated from scratch.
 */
public class SnapshotStep implements DlmStep {

    private static final Logger logger = getLogger(SnapshotStep.class);

    public static final String DLM_SNAPSHOT_COMPLETED_KEY = "index.dlm.snapshot_completed";
    public static final Setting<Boolean> DLM_SNAPSHOT_COMPLETED_SETTING = Setting.boolSetting(
        DLM_SNAPSHOT_COMPLETED_KEY,
        false,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    static final TimeValue SNAPSHOT_TIMEOUT = TimeValue.timeValueHours(12);
    static final String SNAPSHOT_NAME_PREFIX = "dlm-frozen-";

    private final Clock clock;

    /**
     * @param clock supplies current time for timeout comparisons
     */
    public SnapshotStep(Clock clock) {
        this.clock = clock;
    }

    @Override
    public boolean stepCompleted(Index index, ProjectState projectState) {
        IndexMetadata indexMetadata = projectState.metadata().index(index.getName());
        if (indexMetadata == null) {
            logger.warn("DLM snapshot step: index [{}] not found in project state", index.getName());
            return false;
        }
        return DLM_SNAPSHOT_COMPLETED_SETTING.get(indexMetadata.getSettings());
    }

    @Override
    public void execute(DlmStepContext stepContext) {
        String indexName = stepContext.indexName();
        String repositoryName = resolveRepositoryName(stepContext);
        String snapshotName = snapshotName(indexName);

        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(stepContext.projectState().cluster());
        long snapshotStartTime = findSnapshotStartTime(snapshotsInProgress, stepContext.projectId(), repositoryName, snapshotName);

        if (snapshotStartTime >= 0) {
            handleInProgressSnapshot(stepContext, repositoryName, snapshotName, snapshotStartTime);
        } else {
            checkForOrphanedSnapshotAndStart(stepContext, repositoryName, snapshotName);
        }
    }

    private static String resolveRepositoryName(DlmStepContext stepContext) {
        return RepositoriesService.DEFAULT_REPOSITORY_SETTING.get(stepContext.projectState().cluster().metadata().settings());
    }

    /**
     * A snapshot for this index is currently running in the cluster. If it has been running longer
     * than {@link #SNAPSHOT_TIMEOUT}, delete it and start again; otherwise leave it alone.
     */
    private void handleInProgressSnapshot(DlmStepContext stepContext, String repositoryName, String snapshotName, long snapshotStartTime) {
        String indexName = stepContext.indexName();

        if ((clock.millis() - snapshotStartTime) > SNAPSHOT_TIMEOUT.millis()) {
            logger.warn(
                "DLM snapshot [{}] for index [{}] has been running for over [{}], cancelling and restarting",
                snapshotName,
                indexName,
                SNAPSHOT_TIMEOUT
            );
            deleteAndRestartSnapshot(stepContext, repositoryName, snapshotName);
        } else {
            logger.trace("DLM snapshot [{}] for index [{}] is still in progress, skipping", snapshotName, indexName);
        }
    }

    /**
     * No snapshot is currently running for this index. Check whether a completed snapshot already
     * exists in the repository (without the completion setting on the index). If a valid, successful
     * snapshot exists, just mark the completion setting directly rather than deleting and recreating.
     * If the snapshot exists but is invalid (e.g. partial or failed), delete and recreate it.
     * Otherwise, start a fresh snapshot.
     */
    private void checkForOrphanedSnapshotAndStart(DlmStepContext stepContext, String repositoryName, String snapshotName) {
        String indexName = stepContext.indexName();
        ProjectId projectId = stepContext.projectId();

        GetSnapshotsRequest getRequest = new GetSnapshotsRequest(INFINITE_MASTER_NODE_TIMEOUT, repositoryName);
        getRequest.snapshots(new String[] { snapshotName });
        getRequest.ignoreUnavailable(true);

        stepContext.client().projectClient(projectId).admin().cluster().getSnapshots(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSnapshotsResponse response) {
                List<SnapshotInfo> snapshots = response.getSnapshots();
                if (snapshots.isEmpty()) {
                    startSnapshot(stepContext, repositoryName, snapshotName);
                    return;
                }

                SnapshotInfo existingSnapshot = snapshots.getFirst();
                if (existingSnapshot.state() == SnapshotState.SUCCESS && existingSnapshot.failedShards() == 0) {
                    logger.info(
                        "DLM found valid orphaned snapshot [{}] for index [{}] without completion marker, marking as complete",
                        snapshotName,
                        indexName
                    );
                    markSnapshotCompleteStandalone(stepContext);
                } else {
                    logger.info(
                        "DLM found invalid orphaned snapshot [{}] for index [{}] (state [{}], failed shards [{}]), deleting and recreating",
                        snapshotName,
                        indexName,
                        existingSnapshot.state(),
                        existingSnapshot.failedShards()
                    );
                    deleteAndRestartSnapshot(stepContext, repositoryName, snapshotName);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof SnapshotMissingException) {
                    startSnapshot(stepContext, repositoryName, snapshotName);
                } else {
                    stepContext.errorStore()
                        .recordAndLogError(
                            projectId,
                            indexName,
                            e,
                            Strings.format("DLM failed to check for existing snapshot [%s] for index [%s]", snapshotName, indexName),
                            stepContext.signallingErrorRetryThreshold()
                        );
                }
            }
        });
    }

    private void deleteAndRestartSnapshot(DlmStepContext stepContext, String repositoryName, String snapshotName) {
        String indexName = stepContext.indexName();
        ProjectId projectId = stepContext.projectId();
        DeleteSnapshotRequest deleteRequest = new DeleteSnapshotRequest(INFINITE_MASTER_NODE_TIMEOUT, repositoryName, snapshotName);

        stepContext.client().projectClient(projectId).execute(TransportDeleteSnapshotAction.TYPE, deleteRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                logger.info("DLM deleted stale snapshot [{}] for index [{}], starting new snapshot", snapshotName, indexName);
                startSnapshot(stepContext, repositoryName, snapshotName);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof SnapshotMissingException) {
                    startSnapshot(stepContext, repositoryName, snapshotName);
                } else {
                    stepContext.errorStore()
                        .recordAndLogError(
                            projectId,
                            indexName,
                            e,
                            Strings.format("DLM failed to delete stale snapshot [%s] for index [%s]", snapshotName, indexName),
                            stepContext.signallingErrorRetryThreshold()
                        );
                }
            }
        });
    }

    private void startSnapshot(DlmStepContext stepContext, String repositoryName, String snapshotName) {
        String indexName = stepContext.indexName();
        CreateSnapshotRequest createRequest = buildCreateSnapshotRequest(repositoryName, indexName, snapshotName);

        stepContext.executeDeduplicatedRequest(
            TransportCreateSnapshotAction.TYPE.name(),
            createRequest,
            Strings.format("DLM failed to create snapshot [%s] for index [%s]", snapshotName, indexName),
            (req, listener) -> {
                logger.trace("DLM issuing create snapshot request [{}] for index [{}]", snapshotName, indexName);
                stepContext.client()
                    .projectClient(stepContext.projectId())
                    .admin()
                    .cluster()
                    .createSnapshot(createRequest, listener.delegateFailureAndWrap((l, response) -> {
                        if (response.getSnapshotInfo() != null && response.getSnapshotInfo().failedShards() == 0) {
                            logger.info("DLM successfully created snapshot [{}] for index [{}]", snapshotName, indexName);
                            markSnapshotComplete(stepContext, l);
                        } else {
                            int failedShards = response.getSnapshotInfo() != null ? response.getSnapshotInfo().failedShards() : -1;
                            l.onFailure(
                                new ElasticsearchException(
                                    Strings.format(
                                        "snapshot [%s] for index [%s] completed with [%d] failed shards",
                                        snapshotName,
                                        indexName,
                                        failedShards
                                    )
                                )
                            );
                        }
                    }));
            }
        );
    }

    /**
     * Marks the snapshot as complete, recording errors directly to the error store. Used when the snapshot already
     * exists in the repository and only the completion marker needs to be set (i.e. outside the deduplicated create path).
     */
    private void markSnapshotCompleteStandalone(DlmStepContext stepContext) {
        markSnapshotComplete(stepContext, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                stepContext.errorStore().clearRecordedError(stepContext.projectId(), stepContext.indexName());
            }

            @Override
            public void onFailure(Exception e) {
                stepContext.errorStore()
                    .recordAndLogError(
                        stepContext.projectId(),
                        stepContext.indexName(),
                        e,
                        Strings.format("DLM failed to mark existing snapshot as complete for index [%s]", stepContext.indexName()),
                        stepContext.signallingErrorRetryThreshold()
                    );
            }
        });
    }

    private void markSnapshotComplete(DlmStepContext stepContext, ActionListener<Void> listener) {
        String indexName = stepContext.indexName();
        ProjectId projectId = stepContext.projectId();

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName);
        updateSettingsRequest.masterNodeTimeout(INFINITE_MASTER_NODE_TIMEOUT);
        updateSettingsRequest.settings(Settings.builder().put(DLM_SNAPSHOT_COMPLETED_KEY, true).build());

        stepContext.client()
            .projectClient(projectId)
            .admin()
            .indices()
            .updateSettings(updateSettingsRequest, listener.delegateFailureAndWrap((l, response) -> {
                if (response.isAcknowledged()) {
                    logger.info("DLM marked snapshot as complete for index [{}]", indexName);
                    l.onResponse(null);
                } else {
                    l.onFailure(
                        new ElasticsearchException("request to mark snapshot complete for index [" + indexName + "] was not acknowledged")
                    );
                }
            }));
    }

    /**
     * Finds the start time of a running snapshot with the given name. Returns {@code -1} if not found.
     */
    private static long findSnapshotStartTime(
        SnapshotsInProgress snapshotsInProgress,
        ProjectId projectId,
        String repositoryName,
        String snapshotName
    ) {
        return snapshotsInProgress.forRepo(projectId, repositoryName)
            .stream()
            .filter(entry -> entry.snapshot().getSnapshotId().getName().equals(snapshotName))
            .map(SnapshotsInProgress.Entry::startTime)
            .findFirst()
            .orElse(-1L);
    }

    private static CreateSnapshotRequest buildCreateSnapshotRequest(String repositoryName, String indexName, String snapshotName) {
        CreateSnapshotRequest request = new CreateSnapshotRequest(INFINITE_MASTER_NODE_TIMEOUT, repositoryName, snapshotName);
        request.indices(indexName);
        request.waitForCompletion(true);
        request.includeGlobalState(false);
        return request;
    }

    static String snapshotName(String indexName) {
        return SNAPSHOT_NAME_PREFIX + indexName;
    }

    @Override
    public String stepName() {
        return "Snapshot Index";
    }

}
