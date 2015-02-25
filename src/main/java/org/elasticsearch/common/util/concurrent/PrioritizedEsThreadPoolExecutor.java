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

import com.google.common.collect.Lists;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A prioritizing executor which uses a priority queue as a work queue. The jobs that will be submitted will be treated
 * as {@link PrioritizedRunnable} and/or {@link PrioritizedCallable}, those tasks that are not instances of these two will
 * be wrapped and assign a default {@link Priority#NORMAL} priority.
 * <p/>
 * Note, if two tasks have the same priority, the first to arrive will be executed first (FIFO style).
 */
public class PrioritizedEsThreadPoolExecutor extends EsThreadPoolExecutor {

    private AtomicLong insertionOrder = new AtomicLong();
    private Queue<Runnable> current = ConcurrentCollections.newQueue();

    PrioritizedEsThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<Runnable>(), threadFactory);
    }

    public Pending[] getPending() {
        List<Pending> pending = Lists.newArrayList();
        addPending(Lists.newArrayList(current), pending, true);
        addPending(Lists.newArrayList(getQueue()), pending, false);
        return pending.toArray(new Pending[pending.size()]);
    }

    public int getNumberOfPendingTasks() {
        int size = current.size();
        size += getQueue().size();
        return size;
    }

    private void addPending(List<Runnable> runnables, List<Pending> pending, boolean executing) {
        for (Runnable runnable : runnables) {
            if (runnable instanceof TieBreakingPrioritizedRunnable) {
                TieBreakingPrioritizedRunnable t = (TieBreakingPrioritizedRunnable) runnable;
                pending.add(new Pending(t.runnable, t.priority(), t.insertionOrder, executing));
            } else if (runnable instanceof PrioritizedFutureTask) {
                PrioritizedFutureTask t = (PrioritizedFutureTask) runnable;
                pending.add(new Pending(t.task, t.priority, t.insertionOrder, executing));
            }
        }
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        current.add(r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        current.remove(r);
    }

    public void execute(Runnable command, final ScheduledExecutorService timer, final TimeValue timeout, final Runnable timeoutCallback) {
        if (command instanceof PrioritizedRunnable) {
            command = new TieBreakingPrioritizedRunnable((PrioritizedRunnable) command, insertionOrder.incrementAndGet());
        } else if (!(command instanceof PrioritizedFutureTask)) { // it might be a callable wrapper...
            command = new TieBreakingPrioritizedRunnable(command, Priority.NORMAL, insertionOrder.incrementAndGet());
        }
        if (timeout.nanos() >= 0) {
            if (command instanceof TieBreakingPrioritizedRunnable) {
                ((TieBreakingPrioritizedRunnable) command).scheduleTimeout(timer, timeoutCallback, timeout);
            } else {
                // We really shouldn't be here. The only way we can get here if somebody created PrioritizedFutureTask
                // and passed it to execute, which doesn't make much sense
                throw new UnsupportedOperationException("Execute with timeout is not supported for future tasks");
            }
        }
        super.execute(command);
    }

    @Override
    public void execute(Runnable command) {
        if (command instanceof PrioritizedRunnable) {
            command = new TieBreakingPrioritizedRunnable((PrioritizedRunnable) command, insertionOrder.incrementAndGet());
        } else if (!(command instanceof PrioritizedFutureTask)) { // it might be a callable wrapper...
            command = new TieBreakingPrioritizedRunnable(command, Priority.NORMAL, insertionOrder.incrementAndGet());
        }
        super.execute(command);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        if (!(runnable instanceof PrioritizedRunnable)) {
            runnable = PrioritizedRunnable.wrap(runnable, Priority.NORMAL);
        }
        return new PrioritizedFutureTask<>((PrioritizedRunnable) runnable, value, insertionOrder.incrementAndGet());
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        if (!(callable instanceof PrioritizedCallable)) {
            callable = PrioritizedCallable.wrap(callable, Priority.NORMAL);
        }
        return new PrioritizedFutureTask<>((PrioritizedCallable<T>) callable, insertionOrder.incrementAndGet());
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
        private ScheduledFuture<?> timeoutFuture;

        TieBreakingPrioritizedRunnable(PrioritizedRunnable runnable, long insertionOrder) {
            this(runnable, runnable.priority(), insertionOrder);
        }

        TieBreakingPrioritizedRunnable(Runnable runnable, Priority priority, long insertionOrder) {
            super(priority);
            this.runnable = runnable;
            this.insertionOrder = insertionOrder;
        }

        @Override
        public void run() {
            FutureUtils.cancel(timeoutFuture);
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
            timeoutFuture = timer.schedule(new Runnable() {
                @Override
                public void run() {
                    if (remove(TieBreakingPrioritizedRunnable.this)) {
                        runAndClean(timeoutCallback);
                    }
                }
            }, timeValue.nanos(), TimeUnit.NANOSECONDS);
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

        public PrioritizedFutureTask(PrioritizedRunnable runnable, T value, long insertionOrder) {
            super(runnable, value);
            this.task = runnable;
            this.priority = runnable.priority();
            this.insertionOrder = insertionOrder;
        }

        public PrioritizedFutureTask(PrioritizedCallable<T> callable, long insertionOrder) {
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
