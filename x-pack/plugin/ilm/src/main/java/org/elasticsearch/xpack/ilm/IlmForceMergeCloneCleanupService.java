/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Master-node service that periodically scans for orphaned ILM force-merge clone indices
 * ({@code fm-clone-*}) and deletes them. A clone becomes an orphan when the ILM policy is
 * removed and re-added mid-action, resetting the {@code LifecycleExecutionState} and losing
 * the pointer to the clone.
 *
 * <p>Only indices stamped with {@link LifecycleSettings#LIFECYCLE_FORCE_MERGE_CLONE_SOURCE_UUID}
 * are eligible for deletion, so only provably ILM-created clones are reclaimed. Clones created
 * before this setting was introduced are left in place.
 *
 * <p>{@link #init()} must be called after construction to register this service as a
 * {@link ClusterStateListener}; calling it from the constructor would publish a self-reference
 * before the object is fully constructed.
 */
public class IlmForceMergeCloneCleanupService implements ClusterStateListener, Closeable {

    private static final Logger logger = LogManager.getLogger(IlmForceMergeCloneCleanupService.class);

    /**
     * How often the service scans for and deletes orphaned force-merge clone indices. The floor
     * is deliberately low (1 s) so integration tests can set a short interval without using
     * internal APIs.
     */
    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "indices.lifecycle.force_merge_clone_cleanup.poll_interval",
        TimeValue.timeValueDays(1),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);

    private final ClusterService clusterService;
    private final Client client;
    private final long initialDelayMillis;
    private final AtomicBoolean isMaster = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private volatile TimeValue pollInterval;
    private ScheduledExecutorService schedulerThreadExecutor;

    public IlmForceMergeCloneCleanupService(ClusterService clusterService, Client client) {
        this.clusterService = clusterService;
        this.client = client;
        this.initialDelayMillis = Math.min(
            TimeValue.timeValueMinutes(5).millis(),
            POLL_INTERVAL_SETTING.get(clusterService.getSettings()).millis()
        );
        this.pollInterval = POLL_INTERVAL_SETTING.get(clusterService.getSettings());
    }

    // visible for testing
    IlmForceMergeCloneCleanupService(ClusterService clusterService, Client client, long initialDelayMillis) {
        this.clusterService = clusterService;
        this.client = client;
        this.initialDelayMillis = initialDelayMillis;
        this.pollInterval = POLL_INTERVAL_SETTING.get(clusterService.getSettings());
    }

    /**
     * Registers this service as a {@link ClusterStateListener} so that master election events trigger
     * thread pool lifecycle. Must be called after construction to avoid publishing a self-reference
     * from the constructor.
     */
    public void init() {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::updatePollInterval);
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (closing.get() || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        boolean isNodeMaster = event.localNodeMaster();
        if (isMaster.getAndSet(isNodeMaster) != isNodeMaster) {
            if (isNodeMaster) {
                startScheduler(initialDelayMillis);
            } else {
                stopScheduler();
            }
        }
    }

    private synchronized void startScheduler(long initialDelay) {
        try {
            if (closing.get() == false) {
                assert schedulerThreadExecutor == null : "previous executor existed but it should not";
                schedulerThreadExecutor = Executors.newSingleThreadScheduledExecutor(
                    EsExecutors.daemonThreadFactory(clusterService.getSettings(), "ilm-fm-clone-cleanup")
                );
                schedulerThreadExecutor.scheduleWithFixedDelay(
                    this::runWithErrorLogging,
                    initialDelay,
                    pollInterval.millis(),
                    TimeUnit.MILLISECONDS
                );
            }
        } catch (Exception e) {
            logger.error("Unexpected exception while starting ILM force-merge clone cleanup scheduler", e);
            stopScheduler();
        }
    }

    private synchronized void stopScheduler() {
        if (schedulerThreadExecutor != null) {
            schedulerThreadExecutor.shutdownNow();
            schedulerThreadExecutor = null;
        }
    }

    private synchronized void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        if (schedulerThreadExecutor != null) {
            // Restart with no initial delay so an operator lowering the interval to force a prompt sweep
            // is not made to wait out the original (up to 5-minute) startup delay.
            stopScheduler();
            startScheduler(0L);
        }
    }

    private void runWithErrorLogging() {
        try {
            cleanUpOrphanedForceMergeClones();
        } catch (Exception e) {
            logger.error("Unexpected exception in ILM force-merge clone cleanup task", e);
        }
    }

    // visible for testing
    void cleanUpOrphanedForceMergeClones() {
        for (ProjectMetadata project : clusterService.state().metadata().projects().values()) {
            if (Thread.currentThread().isInterrupted() || closing.get()) {
                return;
            }
            if (LifecycleOperationMetadata.currentILMMode(project) != OperationMode.RUNNING) {
                continue;
            }
            List<String> orphans = findOrphanedClones(project);
            if (orphans.isEmpty() == false) {
                deleteIndices(orphans, project);
            }
        }
    }

    // visible for testing
    static List<String> findOrphanedClones(ProjectMetadata project) {
        Set<String> referencedCloneNames = project.indices()
            .values()
            .stream()
            .map(imd -> imd.getLifecycleExecutionState().forceMergeCloneIndexName())
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableSet());

        return project.indices()
            .values()
            .stream()
            .filter(imd -> Strings.hasText(LifecycleSettings.LIFECYCLE_FORCE_MERGE_CLONE_SOURCE_UUID_SETTING.get(imd.getSettings())))
            .filter(imd -> imd.isSearchableSnapshot() == false)
            .filter(imd -> imd.getIndex().getName().startsWith(SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX))
            .map(imd -> imd.getIndex().getName())
            .filter(name -> referencedCloneNames.contains(name) == false)
            .toList();
    }

    private void deleteIndices(List<String> orphans, ProjectMetadata project) {
        DeleteIndexRequest request = new DeleteIndexRequest(orphans.toArray(String[]::new)).indicesOptions(IGNORE_MISSING_OPTIONS)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        String indexNames = String.join(",", orphans);
        logger.debug("ILM force-merge clone cleanup issuing request to delete orphaned indices [{}]", indexNames);
        try {
            AcknowledgedResponse response = client.projectClient(project.id()).admin().indices().delete(request).get();
            if (response.isAcknowledged()) {
                logger.info(
                    "ILM force-merge clone cleanup successfully deleted orphaned force-merge clone indices [{}] in project [{}]: "
                        + "they carry [{}] but are not referenced by any lifecycle execution state",
                    indexNames,
                    project.id(),
                    LifecycleSettings.LIFECYCLE_FORCE_MERGE_CLONE_SOURCE_UUID
                );
            } else {
                logger.warn(
                    "ILM force-merge clone cleanup failed to acknowledge deletion of orphaned indices [{}] in project [{}]",
                    indexNames,
                    project.id()
                );
            }
        } catch (Exception e) {
            logger.warn(
                "ILM force-merge clone cleanup failed to delete orphaned indices [{}] in project [{}]",
                indexNames,
                project.id(),
                e
            );
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closing.compareAndSet(false, true)) {
            clusterService.removeListener(this);
            if (schedulerThreadExecutor != null) {
                ThreadPool.terminate(schedulerThreadExecutor, 10, TimeUnit.SECONDS);
                schedulerThreadExecutor = null;
            }
        }
    }

    // visible for testing
    synchronized boolean isSchedulerRunning() {
        return schedulerThreadExecutor != null && schedulerThreadExecutor.isShutdown() == false;
    }
}
