/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * Master-node service that periodically scans for orphaned DLM frozen transition artifacts
 * (cloned indices and snapshots) and removes them. Thread pool is started when the node becomes
 * master and stopped when it loses mastership or the service is closed.
 */
class DLMFrozenCleanupService extends AbstractDLMPeriodicMasterOnlyService {

    static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "dlm.frozen_cleanup.poll_interval",
        TimeValue.timeValueDays(1),
        TimeValue.timeValueHours(1),
        Setting.Property.NodeScope
    );
    private static final Logger logger = getLogger(DLMFrozenCleanupService.class);
    private final Client client;

    DLMFrozenCleanupService(ClusterService clusterService, Client client) {
        this(
            clusterService,
            client,
            Math.min(TimeValue.timeValueMinutes(5).millis(), POLL_INTERVAL_SETTING.get(clusterService.getSettings()).millis())
        );
    }

    // visible for testing
    DLMFrozenCleanupService(ClusterService clusterService, Client client, long initialDelayMillis) {
        super(clusterService, POLL_INTERVAL_SETTING.get(clusterService.getSettings()), initialDelayMillis);
        this.client = client;
    }

    @Override
    Runnable getScheduledTask() {
        return this::checkForOrphanedResources;
    }

    @Override
    String getSchedulerThreadName() {
        return "dlm-frozen-cleanup";
    }

    /**
     * Checks for and removes orphaned DLM frozen transition artifacts across all projects.
     * Scans for orphaned {@code dlm-clone-*} indices whose source index no longer exists in
     * the same project and deletes them. Also scans the default snapshot repository for
     * DLM-managed snapshots that are no longer needed and deletes those too.
     */
    // visible for testing
    void checkForOrphanedResources() {
        try {
            String defaultRepository = RepositoriesService.DEFAULT_REPOSITORY_SETTING.get(clusterService.state().metadata().settings());
            if (defaultRepository.isEmpty()) {
                logger.debug("No default repository configured, skipping snapshot cleanup");
                defaultRepository = null;
            }

            for (ProjectMetadata projectMetadata : clusterService.state().metadata().projects().values()) {
                if (Thread.currentThread().isInterrupted() || isClosing()) {
                    return;
                }

                Collection<String> indicesToDelete = getOrphanedIndicesInProject(projectMetadata);

                if (Thread.currentThread().isInterrupted() || isClosing()) {
                    return;
                }

                Collection<String> snapshotsToDelete;
                if (defaultRepository != null) {
                    snapshotsToDelete = getOrphanedSnapshotsInProject(projectMetadata, defaultRepository);
                } else {
                    snapshotsToDelete = Collections.emptyList();
                }

                if (Thread.currentThread().isInterrupted() || isClosing()) {
                    return;
                }

                if (indicesToDelete.isEmpty() == false) {
                    deleteIndices(indicesToDelete, projectMetadata.id());
                }

                if (Thread.currentThread().isInterrupted() || isClosing()) {
                    return;
                }

                if (snapshotsToDelete.isEmpty() == false) {
                    deleteSnapshots(defaultRepository, snapshotsToDelete, projectMetadata.id());
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to clean up orphaned frozen transition artifacts", e);
        }
    }

    private Collection<String> getOrphanedIndicesInProject(ProjectMetadata projectMetadata) {
        return projectMetadata.indices()
            .values()
            .stream()
            .filter(indexMetadata -> DLMConvertToFrozen.DLM_CREATED_SETTING.get(indexMetadata.getSettings()))
            .map(IndexMetadata::getIndex)
            .map(Index::getName)
            .filter(indexName -> isIndexOrphaned(indexName, projectMetadata))
            .toList();
    }

    private List<String> getOrphanedSnapshotsInProject(ProjectMetadata projectMetadata, String repositoryName) {
        GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest(TimeValue.MAX_VALUE, repositoryName).snapshots(
            new String[] { DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + "*" }
        );
        PlainActionFuture<GetSnapshotsResponse> future = new PlainActionFuture<>();

        ProjectId projectId = projectMetadata.id();

        try {
            client.projectClient(projectId).admin().cluster().getSnapshots(getSnapshotsRequest, future);

            GetSnapshotsResponse getSnapshotsResponse = future.get();

            return getSnapshotsResponse.getSnapshots().stream().filter(snapshotInfo -> {
                Map<String, Object> metadata = snapshotInfo.userMetadata();
                if (metadata == null || Boolean.TRUE.equals(metadata.get(DLMConvertToFrozen.DLM_CREATED_METADATA_KEY)) == false) {
                    return false;
                }

                List<String> indices = snapshotInfo.indices();
                if (indices.size() != 1) {
                    return false;
                }

                return isIndexOrphaned(indices.getFirst(), projectMetadata);
            }).map(s -> s.snapshotId().getName()).toList();

        } catch (Exception e) {
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logger.warn("Failed to list snapshots from repository [{}] in project [{}]", repositoryName, projectId, e);
        }

        return Collections.emptyList();
    }

    private boolean isIndexOrphaned(String indexName, ProjectMetadata projectMetadata) {
        String indexToCheck = indexName;

        // If the index itself is part of the datastream, then it's not orphaned
        if (isInADataStream(indexToCheck, projectMetadata)) {
            return false;
        }

        // Possibly resolve mounted snapshot to source index
        indexToCheck = Optional.ofNullable(indexName)
            .map(projectMetadata::index)
            .map(IndexMetadata::getSettings)
            .map(s -> s.get(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_INDEX_NAME_SETTING_KEY))
            .orElse(indexToCheck);

        // possibly resolve force merged index to source index
        indexToCheck = Optional.ofNullable(projectMetadata.index(indexToCheck))
            .map(IndexMetadata::getResizeSourceIndex)
            .map(Index::getName)
            .orElse(indexToCheck);

        // Source index has been deleted
        if (projectMetadata.index(indexToCheck) == null) {
            return true;
        }

        // Is source index still in a valid datastream
        return isInADataStream(indexToCheck, projectMetadata) == false;
    }

    /**
     * Returns true if {@code indexName} resolves to an index that is part of a datastream
     */
    private static boolean isInADataStream(String indexName, ProjectMetadata projectMetadata) {
        IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(indexName);
        if (indexAbstraction == null) {
            return false;
        }
        return indexAbstraction.getParentDataStream() != null;
    }

    private void deleteIndices(Collection<String> indicesToDelete, ProjectId projectId) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indicesToDelete.toArray(new String[0])).indicesOptions(
            DLMConvertToFrozen.IGNORE_MISSING_OPTIONS
        ).masterNodeTimeout(TimeValue.MAX_VALUE);

        String indexNames = String.join(",", indicesToDelete);
        logger.debug("DLM cleanup issuing request to delete indices [{}]", indexNames);
        try {
            AcknowledgedResponse resp = client.projectClient(projectId).admin().indices().delete(deleteIndexRequest).get();
            if (resp.isAcknowledged()) {
                logger.info("DLM cleanup successfully deleted indices [{}]", indexNames);
            } else {
                logger.warn("DLM cleanup failed to acknowledge deletion of indices [{}]", indexNames);
            }
        } catch (Exception e) {
            logger.warn("DLM cleanup failed to delete indices [{}]", indexNames, e);
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void deleteSnapshots(String repository, Collection<String> snapshotNames, ProjectId projectId) {
        DeleteSnapshotRequest deleteSnapshotRequest = new DeleteSnapshotRequest(
            TimeValue.MAX_VALUE,
            repository,
            snapshotNames.toArray(new String[0])
        );

        String joinedSnapshotNames = String.join(",", snapshotNames);
        logger.debug("DLM cleanup issuing request to delete snapshots [{}] from repository [{}]", joinedSnapshotNames, repository);
        try {
            PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
            client.projectClient(projectId).admin().cluster().deleteSnapshot(deleteSnapshotRequest, future);
            AcknowledgedResponse resp = future.get();
            if (resp.isAcknowledged()) {
                logger.info("DLM cleanup successfully deleted snapshots [{}] from repository [{}]", joinedSnapshotNames, repository);
            } else {
                logger.warn(
                    "DLM cleanup failed to acknowledge deletion of snapshots [{}] from repository [{}]",
                    joinedSnapshotNames,
                    repository
                );
            }
        } catch (Exception e) {
            logger.warn("DLM cleanup failed to delete snapshots [{}] from repository [{}]", joinedSnapshotNames, repository, e);
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
