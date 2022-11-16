/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

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

    // Controls the interval in which write acknowledgement scheduled task will be executed
    public static final Setting<TimeValue> WRITE_ACK_DELAY_INTERVAL = timeSetting(
        "indices.write_ack_delay_interval",
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    // Controls a max time bound after which the write acknowledgements will be completed after the scheduling task runs
    public static final Setting<TimeValue> WRITE_ACK_DELAY_RANDOMNESS_BOUND = timeSetting(
        "indices.write_ack_delay_randomness_bound",
        TimeValue.timeValueMillis(70),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(WriteAckDelay.class);
    private final ThreadPool threadPool;
    private final ConcurrentLinkedQueue<Runnable> writeCallbacks = new ConcurrentLinkedQueue<>();
    private final TimeValue writeDelayInterval;
    private final long writeDelayRandomnessBoundMillis;

    public WriteAckDelay(long writeDelayIntervalNanos, long writeDelayRandomnessBoundMillis, ThreadPool threadPool) {
        this.writeDelayInterval = TimeValue.timeValueNanos(writeDelayIntervalNanos);
        this.writeDelayRandomnessBoundMillis = writeDelayRandomnessBoundMillis;
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

            long delayRandomness = Randomness.get().nextLong(writeDelayRandomnessBoundMillis) + 1;
            TimeValue randomDelay = TimeValue.timeValueMillis(delayRandomness);
            logger.trace(
                "scheduling write ack completion task [{} writes; {} interval; {} random delay]",
                tasks.size(),
                writeDelayInterval,
                randomDelay
            );
            threadPool.schedule(new CompletionTask(tasks), randomDelay, ThreadPool.Names.GENERIC);
        }
    }

    private record CompletionTask(ArrayList<Runnable> tasks) implements Runnable {

        @Override
        public void run() {
            logger.trace("completing {} writes", tasks.size());
            for (Runnable task : tasks) {
                try {
                    task.run();
                } catch (Exception e) {
                    logger.error("unexpected exception while completing write task after delay", e);
                }
            }
        }
    }

    /**
     * Creates a potential WriteAckDelay object based on settings. If indices.write_ack_delay_interval is less
     * than or equal to 0 null will be returned.
     */
    public static WriteAckDelay create(Settings settings, ThreadPool threadPool) {
        if (WRITE_ACK_DELAY_INTERVAL.get(settings).nanos() <= 0) {
            return null;
        } else {
            return new WriteAckDelay(
                WRITE_ACK_DELAY_INTERVAL.get(settings).nanos(),
                WRITE_ACK_DELAY_RANDOMNESS_BOUND.get(settings).millis(),
                threadPool
            );
        }
    }
}
