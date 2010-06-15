/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.timer;

import org.elasticsearch.common.collect.MapBackedSet;
import org.elasticsearch.common.collect.ReusableIterator;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.util.concurrent.ConcurrentIdentityHashMap;
import org.elasticsearch.util.ThreadRenamingRunnable;

import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A {@link Timer} optimized for approximated I/O timeout scheduling.
 *
 * <h3>Tick Duration</h3>
 *
 * As described with 'approximated', this timer does not execute the scheduled
 * {@link TimerTask} on time.  {@link HashedWheelTimer}, on every tick, will
 * check if there are any {@link TimerTask}s behind the schedule and execute
 * them.
 * <p>
 * You can increase or decrease the accuracy of the execution timing by
 * specifying smaller or larger tick duration in the constructor.  In most
 * network applications, I/O timeout does not need to be accurate.  Therefore,
 * the default tick duration is 100 milliseconds and you will not need to try
 * different configurations in most cases.
 *
 * <h3>Ticks per Wheel (Wheel Size)</h3>
 *
 * {@link HashedWheelTimer} maintains a data structure called 'wheel'.
 * To put simply, a wheel is a hash table of {@link TimerTask}s whose hash
 * function is 'dead line of the task'.  The default number of ticks per wheel
 * (i.e. the size of the wheel) is 512.  You could specify a larger value
 * if you are going to schedule a lot of timeouts.
 *
 * <h3>Implementation Details</h3>
 *
 * {@link HashedWheelTimer} is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/>George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://www-cse.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 *
 * @author kimchy (shay.banon)
 */
public class HashedWheelTimer implements Timer {

    private final ESLogger logger;

    private static final AtomicInteger id = new AtomicInteger();

    // I'd say 64 active timer threads are obvious misuse.
    private static final int MISUSE_WARNING_THRESHOLD = 64;
    private static final AtomicInteger activeInstances = new AtomicInteger();
    private static final AtomicBoolean loggedMisuseWarning = new AtomicBoolean();

    private final Worker worker = new Worker();
    final Thread workerThread;
    final AtomicBoolean shutdown = new AtomicBoolean();

    private final long roundDuration;
    final long tickDuration;
    final Set<HashedWheelTimeout>[] wheel;
    final ReusableIterator<HashedWheelTimeout>[] iterators;
    final int mask;
    final ReadWriteLock lock = new ReentrantReadWriteLock();
    volatile int wheelCursor;

    /**
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     */
    public HashedWheelTimer(ESLogger logger, ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(logger, threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     */
    public HashedWheelTimer(ESLogger logger, ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this.logger = logger;

        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException(
                    "tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException(
                    "ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }

        // Normalize ticksPerWheel to power of two and initialize the wheel.
        wheel = createWheel(ticksPerWheel);
        iterators = createIterators(wheel);
        mask = wheel.length - 1;

        // Convert tickDuration to milliseconds.
        this.tickDuration = tickDuration = unit.toMillis(tickDuration);

        // Prevent overflow.
        if (tickDuration == Long.MAX_VALUE ||
                tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(
                    "tickDuration is too long: " +
                            tickDuration + ' ' + unit);
        }

        roundDuration = tickDuration * wheel.length;

        workerThread = threadFactory.newThread(new ThreadRenamingRunnable(
                worker, "Hashed wheel timer #" + id.incrementAndGet()));

        // Misuse check
        int activeInstances = HashedWheelTimer.activeInstances.incrementAndGet();
        if (activeInstances >= MISUSE_WARNING_THRESHOLD &&
                loggedMisuseWarning.compareAndSet(false, true)) {
            logger.debug(
                    "There are too many active " +
                            HashedWheelTimer.class.getSimpleName() + " instances (" +
                            activeInstances + ") - you should share the small number " +
                            "of instances to avoid excessive resource consumption.");
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<HashedWheelTimeout>[] createWheel(int ticksPerWheel) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException(
                    "ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException(
                    "ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }

        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        Set<HashedWheelTimeout>[] wheel = new Set[ticksPerWheel];
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new MapBackedSet<HashedWheelTimeout>(
                    new ConcurrentIdentityHashMap<HashedWheelTimeout, Boolean>(16, 0.95f, 4));
        }
        return wheel;
    }

    @SuppressWarnings("unchecked")
    private static ReusableIterator<HashedWheelTimeout>[] createIterators(Set<HashedWheelTimeout>[] wheel) {
        ReusableIterator<HashedWheelTimeout>[] iterators = new ReusableIterator[wheel.length];
        for (int i = 0; i < wheel.length; i++) {
            iterators[i] = (ReusableIterator<HashedWheelTimeout>) wheel[i].iterator();
        }
        return iterators;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public synchronized void start() {
        if (shutdown.get()) {
            throw new IllegalStateException("cannot be started once stopped");
        }

        if (!workerThread.isAlive()) {
            workerThread.start();
        }
    }

    public synchronized Set<Timeout> stop() {
        if (!shutdown.compareAndSet(false, true)) {
            return Collections.emptySet();
        }

        boolean interrupted = false;
        while (workerThread.isAlive()) {
            workerThread.interrupt();
            try {
                workerThread.join(100);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        activeInstances.decrementAndGet();

        Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();
        for (Set<HashedWheelTimeout> bucket : wheel) {
            unprocessedTimeouts.addAll(bucket);
            bucket.clear();
        }

        return Collections.unmodifiableSet(unprocessedTimeouts);
    }

    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        final long currentTime = System.currentTimeMillis();

        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        delay = unit.toMillis(delay);
        if (delay < tickDuration) {
            delay = tickDuration;
        }

        if (!workerThread.isAlive()) {
            start();
        }

        // Prepare the required parameters to create the timeout object.
        HashedWheelTimeout timeout;
        final long lastRoundDelay = delay % roundDuration;
        final long lastTickDelay = delay % tickDuration;
        final long relativeIndex =
                lastRoundDelay / tickDuration + (lastTickDelay != 0 ? 1 : 0);
        final long deadline = currentTime + delay;

        final long remainingRounds =
                delay / roundDuration - (delay % roundDuration == 0 ? 1 : 0);

        // Add the timeout to the wheel.
        lock.readLock().lock();
        try {
            timeout =
                    new HashedWheelTimeout(
                            task, deadline,
                            (int) (wheelCursor + relativeIndex & mask),
                            remainingRounds);

            wheel[timeout.stopIndex].add(timeout);
        } finally {
            lock.readLock().unlock();
        }

        return timeout;
    }

    private final class Worker implements Runnable {

        private long startTime;
        private long tick;

        Worker() {
            super();
        }

        public void run() {
            List<HashedWheelTimeout> expiredTimeouts =
                    new ArrayList<HashedWheelTimeout>();

            startTime = System.currentTimeMillis();
            tick = 1;

            while (!shutdown.get()) {
                waitForNextTick();
                fetchExpiredTimeouts(expiredTimeouts);
                notifyExpiredTimeouts(expiredTimeouts);
            }
        }

        private void fetchExpiredTimeouts(
                List<HashedWheelTimeout> expiredTimeouts) {

            // Find the expired timeouts and decrease the round counter
            // if necessary.  Note that we don't send the notification
            // immediately to make sure the listeners are called without
            // an exclusive lock.
            lock.writeLock().lock();
            try {
                int oldBucketHead = wheelCursor;
                int newBucketHead = oldBucketHead + 1 & mask;
                wheelCursor = newBucketHead;

                ReusableIterator<HashedWheelTimeout> i = iterators[oldBucketHead];
                fetchExpiredTimeouts(expiredTimeouts, i);
            } finally {
                lock.writeLock().unlock();
            }
        }

        private void fetchExpiredTimeouts(
                List<HashedWheelTimeout> expiredTimeouts,
                ReusableIterator<HashedWheelTimeout> i) {

            long currentDeadline = System.currentTimeMillis() + tickDuration;
            i.rewind();
            while (i.hasNext()) {
                HashedWheelTimeout timeout = i.next();
                if (timeout.remainingRounds <= 0) {
                    if (timeout.deadline < currentDeadline) {
                        i.remove();
                        expiredTimeouts.add(timeout);
                    } else {
                        // A rare case where a timeout is put for the next
                        // round: just wait for the next round.
                    }
                } else {
                    timeout.remainingRounds--;
                }
            }
        }

        private void notifyExpiredTimeouts(
                List<HashedWheelTimeout> expiredTimeouts) {
            // Notify the expired timeouts.
            for (int i = expiredTimeouts.size() - 1; i >= 0; i--) {
                expiredTimeouts.get(i).expire();
            }

            // Clean up the temporary list.
            expiredTimeouts.clear();
        }

        private void waitForNextTick() {
            for (; ;) {
                final long currentTime = System.currentTimeMillis();
                final long sleepTime = tickDuration * tick - (currentTime - startTime);

                if (sleepTime <= 0) {
                    break;
                }

                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    if (shutdown.get()) {
                        return;
                    }
                }
            }

            // Reset the tick if overflow is expected.
            if (tickDuration * tick > Long.MAX_VALUE - tickDuration) {
                startTime = System.currentTimeMillis();
                tick = 1;
            } else {
                // Increase the tick if overflow is not likely to happen.
                tick++;
            }
        }
    }

    private final class HashedWheelTimeout implements Timeout {

        private final TimerTask task;
        final int stopIndex;
        final long deadline;
        volatile long remainingRounds;
        private volatile boolean cancelled;

        HashedWheelTimeout(
                TimerTask task, long deadline, int stopIndex, long remainingRounds) {
            this.task = task;
            this.deadline = deadline;
            this.stopIndex = stopIndex;
            this.remainingRounds = remainingRounds;
        }

        public Timer getTimer() {
            return HashedWheelTimer.this;
        }

        public TimerTask getTask() {
            return task;
        }

        public void cancel() {
            if (isExpired()) {
                return;
            }

            cancelled = true;

            // Might be called more than once, but doesn't matter.
            wheel[stopIndex].remove(this);
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public boolean isExpired() {
            return cancelled || System.currentTimeMillis() > deadline;
        }

        public void expire() {
            if (cancelled) {
                return;
            }

            try {
                task.run(this);
            } catch (Throwable t) {
                logger.warn(
                        "An exception was thrown by " +
                                TimerTask.class.getSimpleName() + ".", t);
            }
        }

        @Override
        public String toString() {
            long currentTime = System.currentTimeMillis();
            long remaining = deadline - currentTime;

            StringBuilder buf = new StringBuilder(192);
            buf.append(getClass().getSimpleName());
            buf.append('(');

            buf.append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining);
                buf.append(" ms later, ");
            } else if (remaining < 0) {
                buf.append(-remaining);
                buf.append(" ms ago, ");
            } else {
                buf.append("now, ");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            return buf.append(')').toString();
        }
    }
}
