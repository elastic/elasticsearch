/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Watchdog mechanism for making sure that no transport thread spends too long blocking the event loop.
 */
public class ThreadWatchdog {

    public static final Setting<TimeValue> NETWORK_THREAD_WATCHDOG_INTERVAL = Setting.timeSetting(
        "network.thread.watchdog.interval",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> NETWORK_THREAD_WATCHDOG_QUIET_TIME = Setting.timeSetting(
        "network.thread.watchdog.quiet_time",
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(ThreadWatchdog.class);

    private final ThreadLocal<ActivityTracker> activityTrackerThreadLocal = new ThreadLocal<>();
    private final List<WeakReference<ActivityTracker>> knownTrackers = new ArrayList<>();

    /**
     * @return an activity tracker for read activities on the current thread
     */
    public ActivityTracker getActivityTrackerForCurrentThread() {
        var result = activityTrackerThreadLocal.get();
        if (result == null) {
            result = new ActivityTracker(Thread.currentThread());
            synchronized (knownTrackers) {
                knownTrackers.add(new WeakReference<>(result));
            }
            activityTrackerThreadLocal.set(result);
        }
        return result;
    }

    // exposed for testing
    List<String> getStuckThreadNames() {
        List<String> stuckThreadNames = null;
        synchronized (knownTrackers) {
            final var iterator = knownTrackers.iterator();
            while (iterator.hasNext()) {
                final var tracker = iterator.next().get();
                if (tracker == null) {
                    iterator.remove();
                } else if (tracker.isIdleOrMakingProgress() == false) {
                    if (stuckThreadNames == null) {
                        stuckThreadNames = new ArrayList<>();
                    }
                    stuckThreadNames.add(tracker.trackedThread.getName());
                }
            }
        }
        return stuckThreadNames == null ? List.of() : stuckThreadNames;
    }

    /**
     * Per-thread class which keeps track of activity on that thread, represented as a {@code long} which is incremented every time an
     * activity starts or stops. Thus the parity of its value indicates whether the thread is idle or not.
     */
    public static final class ActivityTracker extends AtomicLong {

        private final Thread trackedThread;
        private long lastObservedValue;

        public ActivityTracker(Thread trackedThread) {
            this.trackedThread = trackedThread;
        }

        public void startActivity() {
            assert trackedThread == Thread.currentThread() : trackedThread.getName() + " vs " + Thread.currentThread().getName();
            final var prevValue = getAndIncrement();
            assert (prevValue & 1) == 0 : "thread [" + trackedThread.getName() + "] was already active";
        }

        public void stopActivity() {
            assert trackedThread == Thread.currentThread() : trackedThread.getName() + " vs " + Thread.currentThread().getName();
            final var prevValue = getAndIncrement();
            assert (prevValue & 1) != 0 : "thread [" + trackedThread.getName() + "] was already inactive";
        }

        boolean isIdleOrMakingProgress() {
            final var value = get();
            if ((value & 1) == 0) {
                // idle
                return true;
            }
            if (value == lastObservedValue) {
                // no change since last check
                return false;
            } else {
                // made progress since last check
                lastObservedValue = value;
                return true;
            }
        }
    }

    public void run(Settings settings, ThreadPool threadPool, Lifecycle lifecycle) {
        new Checker(threadPool, NETWORK_THREAD_WATCHDOG_INTERVAL.get(settings), NETWORK_THREAD_WATCHDOG_QUIET_TIME.get(settings), lifecycle)
            .run();
    }

    private final class Checker extends AbstractRunnable {
        private final ThreadPool threadPool;
        private final TimeValue interval;
        private final TimeValue quietTime;
        private final Lifecycle lifecycle;

        Checker(ThreadPool threadPool, TimeValue interval, TimeValue quietTime, Lifecycle lifecycle) {
            this.threadPool = threadPool;
            this.interval = interval;
            this.quietTime = quietTime.compareTo(interval) <= 0 ? interval : quietTime;
            this.lifecycle = lifecycle;
            assert this.interval.millis() <= this.quietTime.millis();
        }

        @Override
        protected void doRun() {
            if (isRunning() == false) {
                return;
            }

            boolean rescheduleImmediately = true;
            try {
                final var stuckThreadNames = getStuckThreadNames();
                if (stuckThreadNames.isEmpty() == false) {
                    logger.warn("the following threads are active but did not make progress since the last scan: {}", stuckThreadNames);
                    rescheduleImmediately = false;
                    threadPool.generic().execute(threadDumper);
                }
            } finally {
                if (rescheduleImmediately) {
                    scheduleNext(interval);
                }
            }
        }

        @Override
        public boolean isForceExecution() {
            return true;
        }

        private boolean isRunning() {
            return 0 < interval.millis() && lifecycle.stoppedOrClosed() == false;
        }

        private void scheduleNext(TimeValue delay) {
            if (isRunning()) {
                threadPool.scheduleUnlessShuttingDown(delay, EsExecutors.DIRECT_EXECUTOR_SERVICE, Checker.this);
            }
        }

        private final AbstractRunnable threadDumper = new AbstractRunnable() {
            @Override
            protected void doRun() {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                if (isRunning()) {
                    HotThreads.logLocalHotThreads(
                        logger,
                        Level.WARN,
                        "hot threads dump due to active threads not making progress",
                        ReferenceDocs.NETWORK_THREADING_MODEL
                    );
                }
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            public void onFailure(Exception e) {
                Checker.this.onFailure(e);
            }

            @Override
            public void onRejection(Exception e) {
                Checker.this.onRejection(e);
            }

            @Override
            public void onAfter() {
                scheduleNext(quietTime);
            }

            @Override
            public String toString() {
                return "ThreadWatchDog$Checker#threadDumper";
            }
        };

        @Override
        public void onFailure(Exception e) {
            logger.error("exception in ThreadWatchDog$Checker", e);
            assert false : e;
        }

        @Override
        public void onRejection(Exception e) {
            logger.debug("ThreadWatchDog$Checker execution rejected", e);
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
        }

        @Override
        public String toString() {
            return "ThreadWatchDog$Checker";
        }
    }
}
