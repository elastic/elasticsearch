/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A prioritizing executor which uses a priority queue as a work queue. The jobs that will be submitted will be treated
 * as {@link PrioritizedRunnable} and/or {@link PrioritizedCallable}, those tasks that are not instances of these two will
 * be wrapped and assign a default {@link Priority#NORMAL} priority.
 * <p>
 * Note, if two tasks have the same priority, the first to arrive will be executed first (FIFO style).
 */
public class PrioritizedEsThreadPoolExecutor extends EsThreadPoolExecutor {

    private static final TimeValue NO_WAIT_TIME_VALUE = TimeValue.timeValueMillis(0);
    private final AtomicLong insertionOrder = new AtomicLong();
    private final Queue<Runnable> current = ConcurrentCollections.newQueue();
    private final ScheduledExecutorService timer;

    PrioritizedEsThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                    ThreadFactory threadFactory, ThreadContext contextHolder, ScheduledExecutorService timer) {
        super(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<>(), threadFactory, contextHolder);
        this.timer = timer;
    }

    public Pending[] getPending() {
        List<Pending> pending = new ArrayList<>();
        addPending(new ArrayList<>(current), pending, true);
        addPending(new ArrayList<>(getQueue()), pending, false);
        return pending.toArray(new Pending[pending.size()]);
    }

    public int getNumberOfPendingTasks() {
        int size = current.size();
        size += getQueue().size();
        return size;
    }

    /**
     * Returns the waiting time of the first task in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        if (getQueue().size() == 0) {
            return NO_WAIT_TIME_VALUE;
        }

        long now = System.nanoTime();
        long oldestCreationDateInNanos = now;
        for (Runnable queuedRunnable : getQueue()) {
            if (queuedRunnable instanceof PrioritizedRunnable) {
                oldestCreationDateInNanos = Math.min(oldestCreationDateInNanos,
                        ((PrioritizedRunnable) queuedRunnable).getCreationDateInNanos());
            }
        }

        return TimeValue.timeValueNanos(now - oldestCreationDateInNanos);
    }

    private void addPending(List<Runnable> runnables, List<Pending> pending, boolean executing) {
        for (Runnable runnable : runnables) {
            if (runnable instanceof TieBreakingPrioritizedRunnable) {
                TieBreakingPrioritizedRunnable t = (TieBreakingPrioritizedRunnable) runnable;
                Runnable innerRunnable = t.runnable;
                if (innerRunnable != null) {
                    /** innerRunnable can be null if task is finished but not removed from executor yet,
                     * see {@link TieBreakingPrioritizedRunnable#run} and {@link TieBreakingPrioritizedRunnable#runAndClean}
                     */
                    pending.add(new Pending(unwrap(innerRunnable), t.priority(), t.insertionOrder, executing));
                }
            } else if (runnable instanceof PrioritizedFutureTask) {
                PrioritizedFutureTask t = (PrioritizedFutureTask) runnable;
                Object task = t.task;
                if (t.task instanceof Runnable) {
                    task = unwrap((Runnable) t.task);
                }
                pending.add(new Pending(task, t.priority, t.insertionOrder, executing));
            }
        }
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        current.add(r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        current.remove(r);
    }

    public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
        command = wrapRunnable(command);
        doExecute(command);
        if (timeout.nanos() >= 0) {
            if (command instanceof TieBreakingPrioritizedRunnable) {
                ((TieBreakingPrioritizedRunnable) command).scheduleTimeout(timer, timeoutCallback, timeout);
            } else {
                // We really shouldn't be here. The only way we can get here if somebody created PrioritizedFutureTask
                // and passed it to execute, which doesn't make much sense
                throw new UnsupportedOperationException("Execute with timeout is not supported for future tasks");
            }
        }
    }

    @Override
    protected Runnable wrapRunnable(Runnable command) {
        if (command instanceof PrioritizedRunnable) {
            if ((command instanceof TieBreakingPrioritizedRunnable)) {
                return command;
            }
            Priority priority = ((PrioritizedRunnable) command).priority();
            return new TieBreakingPrioritizedRunnable(super.wrapRunnable(command), priority, insertionOrder.incrementAndGet());
        } else if (command instanceof PrioritizedFutureTask) {
            return command;
        } else { // it might be a callable wrapper...
            if (command instanceof TieBreakingPrioritizedRunnable) {
                return command;
            }
            return new TieBreakingPrioritizedRunnable(super.wrapRunnable(command), Priority.NORMAL, insertionOrder.incrementAndGet());
        }
    }


    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        if (!(runnable instanceof PrioritizedRunnable)) {
            runnable = PrioritizedRunnable.wrap(runnable, Priority.NORMAL);
        }
        Priority priority = ((PrioritizedRunnable) runnable).priority();
        return new PrioritizedFutureTask<>(runnable, priority, value, insertionOrder.incrementAndGet());
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        if (!(callable instanceof PrioritizedCallable)) {
            callable = PrioritizedCallable.wrap(callable, Priority.NORMAL);
        }
        return new PrioritizedFutureTask<>((PrioritizedCallable)callable, insertionOrder.incrementAndGet());
    }

    public static class Pending {
        public final Object task;
        public final Priority priority;
        public final long insertionOrder;
        public final boolean executing;

        public Pending(Object task, Priority priority, long insertionOrder, boolean executing) {
            this.task = task;
            this.priority = priority;
            this.insertionOrder = insertionOrder;
            this.executing = executing;
        }
    }

    private final class TieBreakingPrioritizedRunnable extends PrioritizedRunnable {

        private Runnable runnable;
        private final long insertionOrder;

        // these two variables are protected by 'this'
        private ScheduledFuture<?> timeoutFuture;
        private boolean started = false;

        TieBreakingPrioritizedRunnable(Runnable runnable, Priority priority, long insertionOrder) {
            super(priority);
            this.runnable = runnable;
            this.insertionOrder = insertionOrder;
        }

        @Override
        public void run() {
            synchronized (this) {
                // make the task as stared. This is needed for synchronization with the timeout handling
                // see  #scheduleTimeout()
                started = true;
                FutureUtils.cancel(timeoutFuture);
            }
            runAndClean(runnable);
        }

        @Override
        public int compareTo(PrioritizedRunnable pr) {
            int res = super.compareTo(pr);
            if (res != 0 || !(pr instanceof TieBreakingPrioritizedRunnable)) {
                return res;
            }
            return insertionOrder < ((TieBreakingPrioritizedRunnable) pr).insertionOrder ? -1 : 1;
        }

        public void scheduleTimeout(ScheduledExecutorService timer, final Runnable timeoutCallback, TimeValue timeValue) {
            synchronized (this) {
                if (timeoutFuture != null) {
                    throw new IllegalStateException("scheduleTimeout may only be called once");
                }
                if (started == false) {
                    timeoutFuture = timer.schedule(new Runnable() {
                        @Override
                        public void run() {
                            if (remove(TieBreakingPrioritizedRunnable.this)) {
                                runAndClean(timeoutCallback);
                            }
                        }
                    }, timeValue.nanos(), TimeUnit.NANOSECONDS);
                }
            }
        }

        /**
         * Timeout callback might remain in the timer scheduling queue for some time and it might hold
         * the pointers to other objects. As a result it's possible to run out of memory if a large number of
         * tasks are executed
         */
        private void runAndClean(Runnable run) {
            try {
                run.run();
            } finally {
                runnable = null;
                timeoutFuture = null;
            }

        }
    }

    private final class PrioritizedFutureTask<T> extends FutureTask<T> implements Comparable<PrioritizedFutureTask> {

        final Object task;
        final Priority priority;
        final long insertionOrder;

        PrioritizedFutureTask(Runnable runnable, Priority priority, T value, long insertionOrder) {
            super(runnable, value);
            this.task = runnable;
            this.priority = priority;
            this.insertionOrder = insertionOrder;
        }

        PrioritizedFutureTask(PrioritizedCallable<T> callable, long insertionOrder) {
            super(callable);
            this.task = callable;
            this.priority = callable.priority();
            this.insertionOrder = insertionOrder;
        }

        @Override
        public int compareTo(PrioritizedFutureTask pft) {
            int res = priority.compareTo(pft.priority);
            if (res != 0) {
                return res;
            }
            return insertionOrder < pft.insertionOrder ? -1 : 1;
        }
    }

}
