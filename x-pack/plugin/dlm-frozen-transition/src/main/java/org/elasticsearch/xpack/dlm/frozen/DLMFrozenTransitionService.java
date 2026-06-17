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
        "dlm.frozen_transition.poll_interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    private static final Logger logger = getLogger(DLMFrozenTransitionService.class);

    private final BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory;
    private final DLMFrozenTransitionExecutor transitionExecutor;

    DLMFrozenTransitionService(
        ClusterService clusterService,
        Client client,
        Supplier<XPackLicenseState> licenseStateSupplier,
        DLMFrozenTransitionExecutor transitionExecutor
    ) {
        this(
            clusterService,
            (index, pid) -> new DLMConvertToFrozen(index, pid, client, clusterService, licenseStateSupplier, Clock.systemUTC()),
            POLL_INTERVAL_SETTING.get(clusterService.getSettings()).millis(),
            transitionExecutor
        );
    }

    // visible for testing
    DLMFrozenTransitionService(
        ClusterService clusterService,
        BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory,
        DLMFrozenTransitionExecutor transitionExecutor
    ) {
        this(clusterService, transitionRunnableFactory, 0, transitionExecutor);
    }

    private DLMFrozenTransitionService(
        ClusterService clusterService,
        BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory,
        long initialDelayMillis,
        DLMFrozenTransitionExecutor transitionExecutor
    ) {
        super(clusterService, POLL_INTERVAL_SETTING.get(clusterService.getSettings()), initialDelayMillis);
        this.transitionRunnableFactory = transitionRunnableFactory;
        this.transitionExecutor = transitionExecutor;
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
        transitionExecutor.start();
    }

    @Override
    void onStop() {
        transitionExecutor.stop();
    }

    // Visible for testing
    DLMFrozenTransitionExecutor getTransitionExecutor() {
        return transitionExecutor;
    }

    // visible for testing
    void checkForFrozenIndices() {
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
                if (transitionExecutor.transitionSubmitted(indexName)) {
                    logger.debug("Transition already running for index [{}], skipping", indexName);
                    continue;
                } else if (transitionExecutor.hasCapacity() == false) {
                    logger.debug("No transition threads available. Stopping loop at {}", indexName);
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
    }
}
