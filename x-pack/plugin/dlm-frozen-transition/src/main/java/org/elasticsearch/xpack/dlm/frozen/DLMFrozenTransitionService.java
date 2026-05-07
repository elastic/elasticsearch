/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.Logger;

import java.time.Clock;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.indexMarkedForFrozen;
import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * Master-node service that periodically scans data stream backing indices for the frozen-candidate marker and submits matching indices to
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

    static final Setting<Integer> MAX_CONCURRENCY_SETTING = Setting.intSetting(
        "dlm.frozen_transition.max_concurrency",
        10,
        1,
        100,
        Setting.Property.NodeScope
    );

    static final Setting<Integer> MAX_QUEUE_SIZE = Setting.intSetting(
        "dlm.frozen_transition.max_queue_size",
        500,
        1,
        10000,
        Setting.Property.NodeScope
    );

    private static final Logger logger = getLogger(DLMFrozenTransitionService.class);
    private final int maxConcurrency;
    private final int maxQueueSize;
    private final DLMFrozenTransitionSettings transitionSettings;
    private final DataStreamLifecycleErrorStore errorStore;
    private final BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory;
    private volatile DLMFrozenTransitionExecutor transitionExecutor;

    DLMFrozenTransitionService(
        ClusterService clusterService,
        Client client,
        Supplier<XPackLicenseState> licenseStateSupplier,
        DLMFrozenTransitionSettings transitionSettings,
        DataStreamLifecycleErrorStore errorStore
    ) {
        this(
            clusterService,
            (index, pid) -> new DLMConvertToFrozen(index, pid, client, clusterService, licenseStateSupplier, Clock.systemUTC()),
            POLL_INTERVAL_SETTING.get(clusterService.getSettings()).millis(),
            transitionSettings,
            errorStore
        );
    }

    // visible for testing
    DLMFrozenTransitionService(
        ClusterService clusterService,
        BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory
    ) {
        this(
            clusterService,
            transitionRunnableFactory,
            0,
            DLMFrozenTransitionSettings.create(clusterService),
            new DataStreamLifecycleErrorStore(System::currentTimeMillis)
        );
    }

    private DLMFrozenTransitionService(
        ClusterService clusterService,
        BiFunction<String, ProjectId, DLMFrozenTransitionRunnable> transitionRunnableFactory,
        long initialDelayMillis,
        DLMFrozenTransitionSettings transitionSettings,
        DataStreamLifecycleErrorStore errorStore
    ) {
        super(clusterService, POLL_INTERVAL_SETTING.get(clusterService.getSettings()), initialDelayMillis);
        this.maxConcurrency = MAX_CONCURRENCY_SETTING.get(clusterService.getSettings());
        this.maxQueueSize = MAX_QUEUE_SIZE.get(clusterService.getSettings());
        this.transitionRunnableFactory = transitionRunnableFactory;
        this.transitionSettings = transitionSettings;
        this.errorStore = errorStore;
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
        transitionExecutor = new DLMFrozenTransitionExecutor(
            clusterService,
            maxConcurrency,
            maxQueueSize,
            clusterService.getSettings(),
            transitionSettings,
            errorStore
        );
    }

    @Override
    void onStop() {
        if (transitionExecutor != null) {
            transitionExecutor.shutdownNow();
            transitionExecutor = null;
        }
    }

    @Override
    void onClose() {
        if (transitionExecutor != null) {
            transitionExecutor.close();
            transitionExecutor = null;
        }
    }

    // Visible for testing
    boolean isTransitionExecutorRunning() {
        return transitionExecutor != null;
    }

    // Visible for testing
    DLMFrozenTransitionExecutor getTransitionExecutor() {
        return transitionExecutor;
    }

    // visible for testing
    void checkForFrozenIndices() {
        final DLMFrozenTransitionExecutor executor = transitionExecutor;
        if (executor == null) {
            return;
        }
        for (ProjectMetadata projectMetadata : clusterService.state().metadata().projects().values()) {
            for (DataStream dataStream : projectMetadata.dataStreams().values()) {
                for (Index index : dataStream.getIndices()) {
                    if (Thread.currentThread().isInterrupted() || isClosing()) {
                        return;
                    }
                    if (indexMarkedForFrozen(projectMetadata.index(index))) {
                        logger.debug("Frozen index to process detected: {}", index);
                        if (executor.transitionSubmitted(index.getName())) {
                            logger.debug("Transition already running for index [{}], skipping", index);
                            continue;
                        } else if (executor.hasCapacity() == false) {
                            logger.debug("No transition threads available. Stopping loop at {}", index);
                            return;
                        }
                        try {
                            executor.submit(transitionRunnableFactory.apply(index.getName(), projectMetadata.id()));
                        } catch (RejectedExecutionException e) {
                            logger.debug(
                                () -> LoggerMessageFormat.format(
                                    "Unable to submit transition task for index [{}], Possibly shutting down?",
                                    index
                                ),
                                e
                            );
                            return;
                        }
                    }
                }
            }
        }
    }
}
