/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.OptionalLong;
import java.util.function.Consumer;

public class StoreHeartbeatService implements LeaderHeartbeatService {
    public static final Setting<TimeValue> HEARTBEAT_FREQUENCY = Setting.timeSetting(
        "cluster.stateless.heartbeat_frequency",
        TimeValue.timeValueSeconds(15),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MAX_MISSED_HEARTBEATS = Setting.intSetting(
        "cluster.stateless.max_missed_heartbeats",
        2,
        1,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(StoreHeartbeatService.class);

    private final HeartbeatStore heartbeatStore;
    private final ThreadPool threadPool;
    private final TimeValue heartbeatFrequency;
    private final TimeValue retryAfterTermReadFailureDelay;
    private final TimeValue maxTimeSinceLastHeartbeat;
    private final Consumer<ActionListener<OptionalLong>> currentTermSupplier;

    private volatile HeartbeatTask heartbeatTask;

    public static StoreHeartbeatService create(
        HeartbeatStore heartbeatStore,
        ThreadPool threadPool,
        Settings settings,
        Consumer<ActionListener<OptionalLong>> currentTermSupplier
    ) {
        TimeValue heartbeatFrequency = HEARTBEAT_FREQUENCY.get(settings);
        return new StoreHeartbeatService(
            heartbeatStore,
            threadPool,
            heartbeatFrequency,
            TimeValue.timeValueMillis(MAX_MISSED_HEARTBEATS.get(settings) * heartbeatFrequency.millis()),
            currentTermSupplier
        );
    }

    public StoreHeartbeatService(
        HeartbeatStore heartbeatStore,
        ThreadPool threadPool,
        TimeValue heartbeatFrequency,
        TimeValue maxTimeSinceLastHeartbeat,
        Consumer<ActionListener<OptionalLong>> currentTermSupplier
    ) {
        this.heartbeatStore = heartbeatStore;
        this.threadPool = threadPool;
        this.heartbeatFrequency = heartbeatFrequency;
        this.retryAfterTermReadFailureDelay = TimeValue.timeValueMillis(heartbeatFrequency.millis() / 2);
        this.maxTimeSinceLastHeartbeat = maxTimeSinceLastHeartbeat;
        this.currentTermSupplier = currentTermSupplier;
    }

    @Override
    public void start(DiscoveryNode currentLeader, long term, ActionListener<Long> completionListener) {
        final var newHeartbeatTask = new HeartbeatTask(term, completionListener);
        heartbeatTask = newHeartbeatTask;
        newHeartbeatTask.run();
    }

    @Override
    public void stop() {
        heartbeatTask = null;
    }

    protected long absoluteTimeInMillis() {
        return threadPool.absoluteTimeInMillis();
    }

    void runIfNoRecentLeader(Runnable runnable) {
        heartbeatStore.readLatestHeartbeat(new ActionListener<>() {
            @Override
            public void onResponse(Heartbeat heartBeat) {
                if (heartBeat == null
                    || maxTimeSinceLastHeartbeat.millis() <= heartBeat.timeSinceLastHeartbeatInMillis(absoluteTimeInMillis())) {
                    runnable.run();
                } else {
                    logger.trace("runIfNoRecentLeader: found recent leader");
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("failed to read heartbeat from store", e);
            }
        });
    }

    private class HeartbeatTask extends ActionRunnable<Long> {
        private final long heartbeatTerm;
        private final ActionListener<TimeValue> rerunListener;

        HeartbeatTask(long heartbeatTerm, ActionListener<Long> listener) {
            super(listener);
            assert 0 < heartbeatTerm : heartbeatTerm;
            this.heartbeatTerm = heartbeatTerm;
            this.rerunListener = listener.delegateFailureAndWrap(
                (l, scheduleDelay) -> threadPool.schedule(HeartbeatTask.this, scheduleDelay, ThreadPool.Names.GENERIC)
            );
        }

        @Override
        protected void doRun() throws Exception {
            if (heartbeatTask != HeartbeatTask.this) {
                // already cancelled
                return;
            }

            currentTermSupplier.accept(rerunListener.delegateFailure((delegate, registerTermOpt) -> {
                if (registerTermOpt.isEmpty()) {
                    rerunListener.onResponse(retryAfterTermReadFailureDelay);
                } else {
                    final var registerTerm = registerTermOpt.getAsLong();
                    if (registerTerm == heartbeatTerm) {
                        heartbeatStore.writeHeartbeat(
                            new Heartbeat(heartbeatTerm, absoluteTimeInMillis()),
                            rerunListener.map(unused -> heartbeatFrequency)
                        );
                    } else {
                        assert heartbeatTerm < registerTerm : heartbeatTerm + " vs " + registerTerm;
                        listener.onResponse(registerTerm);
                    }
                }
            }));
        }
    }
}
