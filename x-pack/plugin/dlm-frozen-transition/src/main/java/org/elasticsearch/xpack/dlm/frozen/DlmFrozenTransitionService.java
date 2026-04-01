/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.indexMarkedForFrozen;
import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * Master-node service that periodically scans data stream backing indices for the frozen-candidate marker and submits matching indices to
 * {@link DlmFrozenTransitionExecutor} for conversion. Thread pools are started when the node becomes master and stopped when it loses
 * mastership or the service is closed.
 */
class DlmFrozenTransitionService implements ClusterStateListener, Closeable {

    private static final Logger logger = getLogger(DlmFrozenTransitionService.class);

    static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "dlm.frozen_transition.poll_interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueMinutes(1),
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

    private final ClusterService clusterService;
    private final AtomicBoolean isMaster = new AtomicBoolean(false);
    private ScheduledExecutorService schedulerThreadExecutor;
    private DlmFrozenTransitionExecutor transitionExecutor;
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final TimeValue pollInterval;
    private final int maxConcurrency;
    private final int maxQueueSize;
    private final long initialDelayMillis;
    private final DataStreamLifecycleErrorStore errorStore;

    private final BiFunction<String, ProjectId, DlmFrozenTransitionRunnable> transitionRunnableFactory;

    DlmFrozenTransitionService(
        ClusterService clusterService,
        Client client,
        XPackLicenseState licenseState,
        DataStreamLifecycleErrorStore errorStore
    ) {
        this(
            clusterService,
            (index, pid) -> new DataStreamLifecycleConvertToFrozen(index, pid, client, clusterService, licenseState),
            POLL_INTERVAL_SETTING.get(clusterService.getSettings()).millis(),
            errorStore
        );
    }

    // visible for testing
    DlmFrozenTransitionService(
        ClusterService clusterService,
        BiFunction<String, ProjectId, DlmFrozenTransitionRunnable> transitionRunnableFactory
    ) {
        this(clusterService, transitionRunnableFactory, 0, new DataStreamLifecycleErrorStore(System::currentTimeMillis));
    }

    private DlmFrozenTransitionService(
        ClusterService clusterService,
        BiFunction<String, ProjectId, DlmFrozenTransitionRunnable> transitionRunnableFactory,
        long initialDelayMillis,
        DataStreamLifecycleErrorStore errorStore
    ) {
        this.clusterService = clusterService;
        this.pollInterval = POLL_INTERVAL_SETTING.get(clusterService.getSettings());
        this.maxConcurrency = MAX_CONCURRENCY_SETTING.get(clusterService.getSettings());
        this.maxQueueSize = MAX_QUEUE_SIZE.get(clusterService.getSettings());
        this.transitionRunnableFactory = transitionRunnableFactory;
        this.initialDelayMillis = initialDelayMillis;
        this.errorStore = errorStore;
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
                transitionExecutor = new DlmFrozenTransitionExecutor(
                    maxConcurrency,
                    maxQueueSize,
                    clusterService.getSettings(),
                    errorStore
                );
                schedulerThreadExecutor = Executors.newSingleThreadScheduledExecutor(
                    EsExecutors.daemonThreadFactory(clusterService.getSettings(), "dlm-frozen-transition-scheduler")
                );
                schedulerThreadExecutor.scheduleAtFixedRate(
                    this::checkForFrozenIndices,
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
            if (transitionExecutor != null) {
                transitionExecutor.shutdownNow();
                transitionExecutor = null;
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
                if (transitionExecutor != null) {
                    transitionExecutor.close();
                    transitionExecutor = null;
                }
            }
        }
    }

    // Visible for testing
    boolean isSchedulerThreadRunning() {
        return schedulerThreadExecutor != null && schedulerThreadExecutor.isShutdown() == false;
    }

    // Visible for testing
    boolean isTransitionExecutorRunning() {
        return transitionExecutor != null;
    }

    // Visible for testing
    DlmFrozenTransitionExecutor getTransitionExecutor() {
        return transitionExecutor;
    }

    // Visible for testing
    boolean isClosing() {
        return closing.get();
    }

    // visible for testing
    void checkForFrozenIndices() {
        final DlmFrozenTransitionExecutor executor = transitionExecutor;
        if (executor == null) {
            return;
        }
        for (ProjectMetadata projectMetadata : clusterService.state().metadata().projects().values()) {
            for (DataStream dataStream : projectMetadata.dataStreams().values()) {
                for (Index index : dataStream.getIndices()) {
                    if (Thread.currentThread().isInterrupted() || closing.get()) {
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
