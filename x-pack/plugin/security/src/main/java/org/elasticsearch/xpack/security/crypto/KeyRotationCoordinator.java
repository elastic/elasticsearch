/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.crypto.KeyRotationHandler;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drives primary encryption key rotation on a timer.
 *
 * <p>On the elected master, it ticks at a fixed cadence. Each tick either begins a new rotation (if one is due), invokes registered
 * {@link KeyRotationHandler}s (if a rotation is in progress), or retires old keys (once handlers are done).
 *
 * <p>The tick cadence is independent of the rotation interval, so master failover mid-rotation stalls re-encryption by at most one tick.
 */
public class KeyRotationCoordinator implements LocalNodeMasterListener, Closeable {

    private static final Logger logger = LogManager.getLogger(KeyRotationCoordinator.class);

    // Number of check_intervals a non-active key persists before being retired.
    private static final int GRACE_TICKS = 10;

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final PrimaryEncryptionKeyService pekService;
    private final TimeValue rotationInterval;
    private final TimeValue checkInterval;

    private volatile Scheduler.Cancellable scheduledTask;
    private volatile boolean closed = false;
    private final AtomicBoolean rotating = new AtomicBoolean(false);
    // Used for logging when a rotation is in progress for an unexpectedly long time.
    private final AtomicLong rotatingSince = new AtomicLong(0L);

    public static KeyRotationCoordinator create(
        ClusterService clusterService,
        ThreadPool threadPool,
        PrimaryEncryptionKeyService pekService,
        Settings settings
    ) {
        TimeValue rotationInterval = PrimaryEncryptionKeyService.ROTATION_INTERVAL_SETTING.get(settings);
        TimeValue checkInterval = PrimaryEncryptionKeyService.CHECK_INTERVAL_SETTING.get(settings);
        KeyRotationCoordinator coordinator = new KeyRotationCoordinator(
            clusterService,
            threadPool,
            pekService,
            rotationInterval,
            checkInterval
        );
        clusterService.addLocalNodeMasterListener(coordinator);
        return coordinator;
    }

    KeyRotationCoordinator(
        ClusterService clusterService,
        ThreadPool threadPool,
        PrimaryEncryptionKeyService pekService,
        TimeValue rotationInterval,
        TimeValue checkInterval
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.pekService = pekService;
        this.rotationInterval = rotationInterval;
        this.checkInterval = checkInterval;
    }

    @Override
    public void onMaster() {
        startSchedule();
    }

    @Override
    public void offMaster() {
        rotating.set(false);
        rotatingSince.set(0L);
        stopSchedule();
    }

    private boolean rotationDisabled() {
        return rotationInterval.duration() == 0;
    }

    private synchronized void startSchedule() {
        if (closed || rotationDisabled() || scheduledTask != null) {
            return;
        }
        logger.debug("starting key rotation schedule (rotation_interval={}, check_interval={})", rotationInterval, checkInterval);
        scheduledTask = threadPool.scheduleWithFixedDelay(this::tick, checkInterval, threadPool.generic());
    }

    private synchronized void stopSchedule() {
        if (scheduledTask != null) {
            logger.debug("stopping key rotation schedule");
            scheduledTask.cancel();
            scheduledTask = null;
        }
    }

    void tick() {
        if (closed || rotationDisabled()) {
            return;
        }
        ClusterState state = clusterService.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) || state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        PrimaryEncryptionKeyMetadata metadata = pekService.getCurrentMetadata(state);
        if (metadata == null) {
            return;
        }

        long now = threadPool.absoluteTimeInMillis();
        long activeKeyAge = now - metadata.getGeneratedAt(metadata.getActiveKeyId());

        rotate(metadata, now);

        if (activeKeyAge >= rotationInterval.millis()) {
            logger.info("primary encryption key due for rotation (active key generated {} ago)", TimeValue.timeValueMillis(activeKeyAge));
            pekService.submitBeginRotation(state);
        }

        long retireCutoff = now - GRACE_TICKS * checkInterval.millis();
        if (metadata.findRetireableKeyIds(retireCutoff).isEmpty() == false) {
            pekService.submitRetireKeys(state, retireCutoff);
        }
    }

    private void rotate(PrimaryEncryptionKeyMetadata metadata, long now) {
        if (rotating.compareAndSet(false, true) == false) {
            logger.warn(
                "rotation already in progress, skipping this tick (in progress for {})",
                TimeValue.timeValueMillis(now - rotatingSince.get())
            );
            return;
        }
        rotatingSince.set(now);
        String activeKeyId = metadata.getActiveKeyId();
        Collection<KeyRotationHandler> handlers = pekService.getRegisteredHandlers();
        try (
            var listeners = new RefCountingListener(
                ActionListener.runAfter(
                    ActionListener.wrap(unused -> {}, e -> logger.warn("rotation handler failed; will retry on next tick", e)),
                    () -> {
                        rotatingSince.set(0L);
                        rotating.set(false);
                    }
                )
            )
        ) {
            for (KeyRotationHandler handler : handlers) {
                ActionListener<Void> l = listeners.acquire();
                try {
                    handler.reEncrypt(activeKeyId, l);
                } catch (Exception e) {
                    l.onFailure(e);
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        clusterService.removeListener(this);
        stopSchedule();
    }
}
