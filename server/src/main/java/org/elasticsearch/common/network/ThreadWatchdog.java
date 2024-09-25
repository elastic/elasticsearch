/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Watchdog mechanism for making sure that no transport thread spends too long blocking the event loop.
 */
// Today we only use this to track activity processing reads on network threads. Tracking time when we're busy processing writes is a little
// trickier because that code is more re-entrant, both within the network layer and also it may complete a listener from the wider codebase
// that ends up calling back into the network layer again. But also we don't see many network threads blocking for ages on the write path,
// so we focus on reads for now.
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

    /**
     * Activity tracker for the current thread. Thread-locals are only retained by the owning thread so these will be GCd after thread exit.
     */
    private final ThreadLocal<ActivityTracker> activityTrackerThreadLocal = new ThreadLocal<>();

    /**
     * Collection of known activity trackers to be scanned for stuck threads. Uses {@link WeakReference} so that we don't prevent trackers
     * from being GCd if a thread exits. There aren't many such trackers, O(#cpus), and they almost never change, so an {@link ArrayList}
     * with explicit synchronization is fine.
     */
    private final List<WeakReference<ActivityTracker>> knownTrackers = new ArrayList<>();

    /**
     * @return an activity tracker for activities on the current thread.
     */
    public ActivityTracker getActivityTrackerForCurrentThread() {
        var result = activityTrackerThreadLocal.get();
        if (result == null) {
            // this is a previously-untracked thread; thread creation is assumed to be very rare, no need to optimize this path at all
            result = new ActivityTracker();
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
        // this is not called very often, and only on a single thread, with almost no contention on this mutex since thread creation is rare
        synchronized (knownTrackers) {
            final var iterator = knownTrackers.iterator();
            while (iterator.hasNext()) {
                final var tracker = iterator.next().get();
                if (tracker == null) {
                    // tracker was GCd because its thread exited - very rare, no need to optimize this case
                    iterator.remove();
                } else if (tracker.isIdleOrMakingProgress() == false) {
                    if (stuckThreadNames == null) {
                        stuckThreadNames = new ArrayList<>();
                    }
                    stuckThreadNames.add(tracker.getTrackedThreadName());
                }
            }
        }
        if (stuckThreadNames == null) {
            return List.of();
        } else {
            stuckThreadNames.sort(Comparator.naturalOrder());
            return stuckThreadNames;
        }
    }

    /**
     * Per-thread class which keeps track of activity on that thread, represented as a {@code long} which is incremented every time an
     * activity starts or stops. Thus the parity of its value indicates whether the thread is idle or not. Crucially, the activity tracking
     * is very lightweight (on the tracked thread).
     */
    public static final class ActivityTracker extends AtomicLong {

        private final Thread trackedThread;
        private long lastObservedValue;

        public ActivityTracker() {
            this.trackedThread = Thread.currentThread();
        }

        String getTrackedThreadName() {
            return trackedThread.getName();
        }

        public void startActivity() {
            assert trackedThread == Thread.currentThread() : trackedThread.getName() + " vs " + Thread.currentThread().getName();
            final var prevValue = getAndIncrement();
            assert isIdle(prevValue) : "thread [" + trackedThread.getName() + "] was already active";
        }

        public boolean maybeStartActivity() {
            assert trackedThread == Thread.currentThread() : trackedThread.getName() + " vs " + Thread.currentThread().getName();
            if (isIdle(get())) {
                getAndIncrement();
                return true;
            } else {
                return false;
            }
        }

        public void stopActivity() {
            assert trackedThread == Thread.currentThread() : trackedThread.getName() + " vs " + Thread.currentThread().getName();
            final var prevValue = getAndIncrement();
            assert isIdle(prevValue) == false : "thread [" + trackedThread.getName() + "] was already idle";
        }

        boolean isIdleOrMakingProgress() {
            final var value = get();
            if (isIdle(value)) {
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

        private static boolean isIdle(long value) {
            // the parity of the value indicates the idle state: initially zero (idle), so active == odd
            return (value & 1) == 0;
        }
    }

    public void run(Settings settings, ThreadPool threadPool, Lifecycle lifecycle) {
        new Checker(threadPool, NETWORK_THREAD_WATCHDOG_INTERVAL.get(settings), NETWORK_THREAD_WATCHDOG_QUIET_TIME.get(settings), lifecycle)
            .run();
    }

    /**
     * Action which runs itself periodically, calling {@link #getStuckThreadNames} to check for active threads that didn't make progress
     * since the last call, and if it finds any then it dispatches {@link #threadDumper} to log the current hot threads.
     */
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
                    logger.warn(
                        "the following threads are active but did not make progress in the preceding [{}]: {}",
                        interval,
                        stuckThreadNames
                    );
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
