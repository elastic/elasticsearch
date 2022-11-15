/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import static org.elasticsearch.common.settings.Setting.timeSetting;

public class WriteAckDelay implements Consumer<Runnable> {

    public static final Setting<TimeValue> WRITE_ACK_DELAY_INTERVAL = timeSetting(
        "indices.write_ack_delay_interval",
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> WRITE_ACK_DELAY_RANDOMNESS_BOUND = timeSetting(
        "indices.write_ack_delay_randomness_bound",
        TimeValue.timeValueMillis(70),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(WriteAckDelay.class);
    private final ThreadPool threadPool;
    private final ConcurrentLinkedQueue<Runnable> writeCallbacks = new ConcurrentLinkedQueue<>();
    private final long writeDelayIntervalNanos;
    private final long writeDelayRandomnessBound;

    public WriteAckDelay(long writeDelayIntervalNanos, long writeDelayRandomnessBound, ThreadPool threadPool) {
        this.writeDelayIntervalNanos = writeDelayIntervalNanos;
        this.writeDelayRandomnessBound = writeDelayRandomnessBound;
        this.threadPool = threadPool;
        this.threadPool.scheduleWithFixedDelay(
            new ScheduleTask(),
            TimeValue.timeValueNanos(writeDelayIntervalNanos),
            ThreadPool.Names.GENERIC
        );
    }

    @Override
    public void accept(Runnable runnable) {
        writeCallbacks.add(runnable);
    }

    private class ScheduleTask implements Runnable {

        @Override
        public void run() {
            ArrayList<Runnable> tasks = new ArrayList<>();
            synchronized (writeCallbacks) {
                Runnable task;
                while ((task = writeCallbacks.poll()) != null) {
                    tasks.add(task);
                }
            }

            long delayRandomness = Randomness.get().nextLong(writeDelayRandomnessBound);
            threadPool.schedule(new CompletionTask(tasks), TimeValue.timeValueNanos(delayRandomness), ThreadPool.Names.GENERIC);
        }
    }

    private static class CompletionTask implements Runnable {

        private final ArrayList<Runnable> tasks;

        public CompletionTask(ArrayList<Runnable> tasks) {
            this.tasks = tasks;
        }

        @Override
        public void run() {
            for (Runnable task : tasks) {
                try {
                    task.run();
                } catch (Exception e) {
                    logger.error("unexpected exception while completing write task after delay", e);
                }
            }
        }
    }

    public static WriteAckDelay create(Settings settings, ThreadPool threadPool) {
        TimeValue timeValue = WRITE_ACK_DELAY_INTERVAL.get(settings);
        if (timeValue.getNanos() <= 0) {
            return null;
        } else {
            return new WriteAckDelay(
                WRITE_ACK_DELAY_INTERVAL.get(settings).nanos(),
                WRITE_ACK_DELAY_RANDOMNESS_BOUND.get(settings).nanos(),
                threadPool
            );
        }
    }
}
