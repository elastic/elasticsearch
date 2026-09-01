/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.Logger;

import java.time.Clock;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DLM_CREATED_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.indexMarkedForFrozen;
import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * Master-node service that periodically scans all project indices for the frozen-candidate marker and submits matching indices to
 * {@link DLMFrozenTransitionExecutor} for conversion. Thread pools are started when the node becomes master and stopped when it loses
 * mastership or the service is closed.
 */
class DLMFrozenTransitionService extends AbstractDLMPeriodicMasterOnlyService {

    static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "dlm.frozen.transition.poll_interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    private static final Logger logger = getLogger(DLMFrozenTransitionService.class);

    private final BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory;
    private final DLMFrozenTransitionExecutor transitionExecutor;
    private final DLMFrozenTransitionSettings transitionSettings;

    /**
     * Whether a scan has done all the submitting it could since this node became master. Reset on every start of the
     * service, so that after a master failover it stays {@code false} until the new master has re-submitted the
     * outstanding marked indices. The health publisher uses it to decide when marked-but-unsubmitted indices are worth
     * reporting.
     *
     * <p>{@link DLMFrozenTransitionPlugin} must keep registering this service as a cluster-state listener before
     * {@link DLMFrozenTransitionHealthInfoPublisher}. Listeners run in registration order, so that order guarantees
     * this reset happens before the publisher records the start of the new master's tenure. In the opposite order, a
     * node that was master before would combine a fresh tenure start with a {@code true} flag left over from the
     * previous tenure, and report a marked index as stalled for one publish cycle before its first scan.
     */
    private volatile boolean completedScanSinceStart = false;

    DLMFrozenTransitionService(
        ClusterService clusterService,
        Client client,
        Supplier<XPackLicenseState> licenseStateSupplier,
        DLMFrozenTransitionExecutor transitionExecutor,
        DLMFrozenTransitionSettings transitionSettings
    ) {
        this(
            clusterService,
            (index, pid) -> new DLMConvertToFrozen(index, pid, client, clusterService, licenseStateSupplier, Clock.systemUTC()),
            POLL_INTERVAL_SETTING.get(clusterService.getSettings()).millis(),
            transitionExecutor,
            transitionSettings
        );
    }

    // visible for testing
    DLMFrozenTransitionService(
        ClusterService clusterService,
        BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory,
        DLMFrozenTransitionExecutor transitionExecutor,
        DLMFrozenTransitionSettings transitionSettings
    ) {
        this(clusterService, transitionRunnableFactory, 0, transitionExecutor, transitionSettings);
    }

    private DLMFrozenTransitionService(
        ClusterService clusterService,
        BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory,
        long initialDelayMillis,
        DLMFrozenTransitionExecutor transitionExecutor,
        DLMFrozenTransitionSettings transitionSettings
    ) {
        super(clusterService, POLL_INTERVAL_SETTING.get(clusterService.getSettings()), initialDelayMillis);
        this.transitionRunnableFactory = transitionRunnableFactory;
        this.transitionExecutor = transitionExecutor;
        this.transitionSettings = transitionSettings;
    }

    @Override
    Runnable getScheduledTask() {
        return this::checkForFrozenIndices;
    }

    @Override
    String getSchedulerThreadName() {
        return "dlm-frozen-transition";
    }

    @Override
    void onStart() {
        completedScanSinceStart = false;
        transitionExecutor.start();
    }

    @Override
    void onStop() {
        transitionExecutor.stop();
    }

    // visible for testing
    DLMFrozenTransitionExecutor getTransitionExecutor() {
        return transitionExecutor;
    }

    /**
     * Has a scan completed since this node became master, submitting all the marked indices it could.
     * Used by the health indicators to prevent an immediate YELLOW status on master failover
     */
    boolean hasCompletedScanSinceStart() {
        return completedScanSinceStart;
    }

    // visible for testing
    void checkForFrozenIndices() {
        if (transitionSettings.isTransitionEnabled() == false) {
            logger.debug(
                "DLM frozen transition is disabled via [{}], skipping scan",
                DLMFrozenTransitionSettings.TRANSITION_ENABLED_SETTING.getKey()
            );
            return;
        }
        for (ProjectMetadata projectMetadata : clusterService.state().metadata().projects().values()) {
            for (IndexMetadata indexMetadata : projectMetadata.indices().values()) {
                if (Thread.currentThread().isInterrupted() || isClosing()) {
                    return;
                }
                if (DLM_CREATED_SETTING.get(indexMetadata.getSettings())) {
                    logger.debug(
                        "Skipping frozen transition for index [{}] because it was created by DLM",
                        indexMetadata.getIndex().getName()
                    );
                    continue;
                }
                if (indexMarkedForFrozen(indexMetadata) == false) {
                    continue;
                }
                if (IndexMetadata.LIFECYCLE_SKIP_SETTING.get(indexMetadata.getSettings())) {
                    logger.info(
                        "Skipping frozen transition for index [{}] because [{}] is set to true",
                        indexMetadata.getIndex().getName(),
                        IndexMetadata.LIFECYCLE_SKIP_SETTING.getKey()
                    );
                    continue;
                }
                String indexName = indexMetadata.getIndex().getName();
                logger.debug("Frozen index to process detected: {}", indexName);
                if (transitionExecutor.transitionSubmitted(projectMetadata.id(), indexName)) {
                    logger.debug("Transition already running for index [{}], skipping", indexName);
                    continue;
                } else if (transitionExecutor.hasCapacity() == false) {
                    logger.debug("No transition threads available. Stopping loop at {}", indexName);
                    completedScanSinceStart = true;
                    return;
                }
                try {
                    transitionExecutor.submit(transitionRunnableFactory.apply(indexName, projectMetadata.id()));
                } catch (RejectedExecutionException e) {
                    logger.debug(
                        () -> LoggerMessageFormat.format(
                            "Unable to submit transition task for index [{}], Possibly shutting down?",
                            indexName
                        ),
                        e
                    );
                    return;
                }
            }
        }
        completedScanSinceStart = true;
    }
}
