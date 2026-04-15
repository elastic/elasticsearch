/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * Master-node service that periodically scans for orphaned DLM frozen transition artifacts
 * (cloned indices and snapshots) and removes them. Thread pool is started when the node becomes
 * master and stopped when it loses mastership or the service is closed.
 */
class DLMFrozenCleanupService implements ClusterStateListener, Closeable {

    static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "dlm.frozen_cleanup.poll_interval",
        TimeValue.timeValueDays(1),
        TimeValue.timeValueHours(1),
        Setting.Property.NodeScope
    );
    private static final Logger logger = getLogger(DLMFrozenCleanupService.class);

    private final ClusterService clusterService;
    private final Client client;
    private final AtomicBoolean isMaster = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final TimeValue pollInterval;
    private final long initialDelayMillis;
    private ScheduledExecutorService schedulerThreadExecutor;

    DLMFrozenCleanupService(ClusterService clusterService, Client client) {
        this(clusterService, client, POLL_INTERVAL_SETTING.get(clusterService.getSettings()).millis());
    }

    // visible for testing
    DLMFrozenCleanupService(ClusterService clusterService, Client client, long initialDelayMillis) {
        this.clusterService = clusterService;
        this.client = client;
        this.pollInterval = POLL_INTERVAL_SETTING.get(clusterService.getSettings());
        this.initialDelayMillis = initialDelayMillis;
    }

    /**
     * Registers this service as a {@link ClusterStateListener} so that master election events trigger thread pool
     * lifecycle. Must be called after construction to avoid publishing a self-reference from the constructor.
     */
    void init() {
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // wait for the cluster state to be recovered
        if (closing.get() || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        var isNodeMaster = event.localNodeMaster();
        if (isMaster.getAndSet(isNodeMaster) != isNodeMaster) {
            if (isNodeMaster) {
                startThreadPools();
            } else {
                stopThreadPools();
            }
        }
    }

    private void startThreadPools() {
        synchronized (this) {
            if (closing.get() == false) {
                assert schedulerThreadExecutor == null : "previous executor existed but it should not";
                schedulerThreadExecutor = Executors.newSingleThreadScheduledExecutor(
                    EsExecutors.daemonThreadFactory(clusterService.getSettings(), "dlm-frozen-cleanup-scheduler")
                );
                schedulerThreadExecutor.scheduleWithFixedDelay(
                    this::checkForOrphanedResources,
                    initialDelayMillis,
                    pollInterval.millis(),
                    TimeUnit.MILLISECONDS
                );
            }
        }
    }

    private void stopThreadPools() {
        synchronized (this) {
            if (schedulerThreadExecutor != null) {
                schedulerThreadExecutor.shutdownNow();
                schedulerThreadExecutor = null;
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closing.compareAndSet(false, true)) {
                clusterService.removeListener(this);
                if (schedulerThreadExecutor != null) {
                    ThreadPool.terminate(schedulerThreadExecutor, 10, TimeUnit.SECONDS);
                    schedulerThreadExecutor = null;
                }
            }
        }
    }

    // Visible for testing
    boolean isSchedulerThreadRunning() {
        return schedulerThreadExecutor != null && schedulerThreadExecutor.isShutdown() == false;
    }

    // Visible for testing
    boolean isClosing() {
        return closing.get();
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
            checkForOrphanedClones();
        } catch (Exception e) {
            logger.warn("Error during DLM orphaned clone cleanup", e);
        }

        try {
            checkForOrphanedSnapshots();
        } catch (Exception e) {
            logger.warn("Error during DLM orphaned snapshot cleanup", e);
        }
    }

    private void checkForOrphanedClones() {
        for (ProjectMetadata projectMetadata : clusterService.state().metadata().projects().values()) {
            if (Thread.currentThread().isInterrupted() || closing.get()) {
                return;
            }

            Set<String> projectIndexUUIDs = projectMetadata.indices()
                .values()
                .stream()
                .map(IndexMetadata::getIndex)
                .map(Index::getUUID)
                .collect(Collectors.toSet());

            List<Index> indicesToDelete = projectMetadata.indices()
                .values()
                .stream()
                .filter(imd -> imd.getIndex().getName().startsWith(DLMConvertToFrozen.CLONE_INDEX_PREFIX))
                .filter(im -> im.getResizeSourceIndex() != null)
                .filter(im -> projectIndexUUIDs.contains(im.getResizeSourceIndex().getUUID()) == false)
                .map(IndexMetadata::getIndex)
                .toList();

            // TODO: These deletes could be collected and issued as a single batched request in the
            // future. Since orphaned clones are expected to be rare, individual deletes are sufficient for now.
            for (Index index : indicesToDelete) {
                if (Thread.currentThread().isInterrupted() || closing.get()) {
                    return;
                }
                logger.info(
                    "Source index [{}] for DLM-created clone [{}] no longer exists, deleting clone",
                    index.getUUID(),
                    index.getName()
                );
                deleteIndex(index.getName(), projectMetadata.id());
            }
        }
    }

    private void checkForOrphanedSnapshots() {
        String defaultRepository = RepositoriesService.DEFAULT_REPOSITORY_SETTING.get(clusterService.state().metadata().settings());
        if (defaultRepository == null || defaultRepository.isEmpty()) {
            logger.debug("No default repository configured, skipping snapshot cleanup");
            return;
        }

        for (ProjectMetadata projectMetadata : clusterService.state().metadata().projects().values()) {
            if (Thread.currentThread().isInterrupted() || closing.get()) {
                return;
            }
            GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest(TimeValue.MAX_VALUE, defaultRepository).snapshots(
                new String[] { DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + "*" }
            );
            PlainActionFuture<GetSnapshotsResponse> future = new PlainActionFuture<>();

            ProjectId projectId = projectMetadata.id();
            client.projectClient(projectId).admin().cluster().getSnapshots(getSnapshotsRequest, future);

            try {
                processSnapshots(future.get(), defaultRepository, projectId);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                logger.warn("Failed to list snapshots from repository [{}] in project [{}]", defaultRepository, projectId, e);
            }
        }
    }

    private void processSnapshots(GetSnapshotsResponse response, String defaultRepository, ProjectId projectId) {
        for (SnapshotInfo snapshotInfo : response.getSnapshots()) {
            if (Thread.currentThread().isInterrupted() || closing.get()) {
                return;
            }

            // TODO: Snapshot checks
        }
    }

    void deleteIndex(String indexToDelete, ProjectId projectId) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexToDelete).indicesOptions(
            DLMConvertToFrozen.IGNORE_MISSING_OPTIONS
        ).masterNodeTimeout(TimeValue.MAX_VALUE);
        logger.debug("DLM cleanup issuing request to delete index [{}]", indexToDelete);
        try {
            AcknowledgedResponse resp = client.projectClient(projectId).admin().indices().delete(deleteIndexRequest).get();
            if (resp.isAcknowledged()) {
                logger.info("DLM cleanup successfully deleted index [{}]", indexToDelete);
            } else {
                logger.warn("DLM cleanup failed to acknowledge deletion of index [{}]", indexToDelete);
            }
        } catch (IndexNotFoundException e) {
            logger.debug("Index [{}] was not found during DLM cleanup delete attempt, it may have already been deleted", indexToDelete);
        } catch (Exception e) {
            logger.warn(Strings.format("DLM cleanup failed to delete index [%s]", indexToDelete), e);
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void deleteSnapshot(String repository, String snapshotName, ProjectId projectId) {
        DeleteSnapshotRequest deleteSnapshotRequest = new DeleteSnapshotRequest(TimeValue.MAX_VALUE, repository, snapshotName);
        logger.debug("DLM cleanup issuing request to delete snapshot [{}] from repository [{}]", snapshotName, repository);
        var listener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse resp) {
                if (resp.isAcknowledged()) {
                    logger.info("DLM cleanup successfully deleted snapshot [{}] from repository [{}]", snapshotName, repository);
                } else {
                    logger.warn(
                        "DLM cleanup failed to acknowledge deletion of snapshot [{}] from repository [{}]",
                        snapshotName,
                        repository
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("DLM cleanup failed to delete snapshot [{}] from repository [{}]", snapshotName, repository, e);
            }
        };
        client.projectClient(projectId).admin().cluster().deleteSnapshot(deleteSnapshotRequest, listener);
    }
}
