/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * Base class for master-node services that periodically execute a scheduled task. Handles the full
 * lifecycle of a {@link ScheduledExecutorService}: started when this node becomes master, stopped when
 * it loses mastership, and terminated on close.
 *
 * <p>Subclasses implement {@link #getScheduledTask()} and {@link #getSchedulerThreadName()} to provide
 * the work and the thread name. Optional lifecycle hooks ({@link #onStart()}, {@link #onStop()}) allow
 * subclasses to manage additional resources alongside the scheduler.
 *
 * <p>{@link #init()} must be called after construction to register this service as a
 * {@link ClusterStateListener}; calling it from the constructor would publish a self-reference
 * before the object is fully constructed.
 */
abstract class AbstractDLMPeriodicMasterOnlyService implements ClusterStateListener, Closeable {

    private static final Logger logger = getLogger(AbstractDLMPeriodicMasterOnlyService.class);

    protected final ClusterService clusterService;

    private final AtomicBoolean isMaster = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final TimeValue pollInterval;
    private final long initialDelayMillis;
    private ScheduledExecutorService schedulerThreadExecutor;

    AbstractDLMPeriodicMasterOnlyService(ClusterService clusterService, TimeValue pollInterval, long initialDelayMillis) {
        this.clusterService = clusterService;
        this.pollInterval = pollInterval;
        this.initialDelayMillis = initialDelayMillis;
    }

    /**
     * Registers this service as a {@link ClusterStateListener} so that master election events trigger
     * thread pool lifecycle. Must be called after construction to avoid publishing a self-reference
     * from the constructor.
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
            try {
                if (closing.get() == false) {
                    assert schedulerThreadExecutor == null : "previous executor existed but it should not";
                    onStart();
                    schedulerThreadExecutor = Executors.newSingleThreadScheduledExecutor(
                        EsExecutors.daemonThreadFactory(clusterService.getSettings(), getSchedulerThreadName())
                    );
                    schedulerThreadExecutor.scheduleWithFixedDelay(
                        wrapScheduledTask(),
                        initialDelayMillis,
                        pollInterval.millis(),
                        TimeUnit.MILLISECONDS
                    );
                }
            } catch (Exception ex) {
                logger.error("Unexpected exception while starting periodic DLM task", ex);
                stopThreadPools();
            }
        }
    }

    /*
     * Wraps the scheduled task so that exceptions are logged and not rethrown to prevent future executions being suppressed due
     * to an unhandled exception.
     */
    private Runnable wrapScheduledTask() {
        return () -> {
            try {
                getScheduledTask().run();
            } catch (Exception ex) {
                logger.error("Unexpected exception in periodic DLM task", ex);
            }
        };
    }

    private void stopThreadPools() {
        synchronized (this) {
            if (schedulerThreadExecutor != null) {
                schedulerThreadExecutor.shutdownNow();
                schedulerThreadExecutor = null;
            }
            onStop();
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
                onClose();
            }
        }
    }

    /**
     * An optional hook that is called when the service is closed.
     * Defaults to calling {@link #onStop()}.
     */
    void onClose() {
        onStop();
    }

    // Visible for testing
    boolean isSchedulerThreadRunning() {
        return schedulerThreadExecutor != null && schedulerThreadExecutor.isShutdown() == false;
    }

    boolean isClosing() {
        return closing.get();
    }

    /**
     * Returns the {@link Runnable} to be scheduled periodically when this node is master.
     */
    abstract Runnable getScheduledTask();

    /**
     * Returns the daemon thread name used for the scheduler thread.
     */
    abstract String getSchedulerThreadName();

    /**
     * Called inside {@link #startThreadPools()} (under the instance lock, before the scheduler is created)
     * when this node becomes master. Override to initialize additional resources such as worker thread pools.
     */
    void onStart() {}

    /**
     * Called when this node loses mastership (inside {@link #stopThreadPools()}, under the instance
     * lock, after the scheduler is shut down) and also unconditionally from {@link #close()}.
     * Implementations must be safe to call even when {@link #onStart()} was never invoked.
     */
    void onStop() {}
}
