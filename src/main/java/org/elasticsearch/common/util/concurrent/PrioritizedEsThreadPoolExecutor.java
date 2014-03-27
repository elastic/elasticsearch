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

    PrioritizedEsThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<Runnable>(), threadFactory);
    }

    public Pending[] getPending() {
        Object[] objects = getQueue().toArray();
        Pending[] infos = new Pending[objects.length];
        for (int i = 0; i < objects.length; i++) {
            Object obj = objects[i];
            if (obj instanceof TieBreakingPrioritizedRunnable) {
                TieBreakingPrioritizedRunnable t = (TieBreakingPrioritizedRunnable) obj;
                infos[i] = new Pending(t.runnable, t.priority(), t.insertionOrder);
            } else if (obj instanceof PrioritizedFutureTask) {
                PrioritizedFutureTask t = (PrioritizedFutureTask) obj;
                infos[i] = new Pending(t.task, t.priority, t.insertionOrder);
            }
        }
        return infos;
    }

    public void execute(Runnable command, final ScheduledExecutorService timer, final TimeValue timeout, final Runnable timeoutCallback) {
        if (command instanceof PrioritizedRunnable) {
            command = new TieBreakingPrioritizedRunnable((PrioritizedRunnable) command, insertionOrder.incrementAndGet());
        } else if (!(command instanceof PrioritizedFutureTask)) { // it might be a callable wrapper...
            command = new TieBreakingPrioritizedRunnable(command, Priority.NORMAL, insertionOrder.incrementAndGet());
        }
        super.execute(command);
        if (timeout.nanos() >= 0) {
            final Runnable fCommand = command;
            timer.schedule(new Runnable() {
                @Override
                public void run() {
                    boolean removed = getQueue().remove(fCommand);
                    if (removed) {
                        timeoutCallback.run();
                    }
                }
            }, timeout.nanos(), TimeUnit.NANOSECONDS);
        }
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

        public Pending(Object task, Priority priority, long insertionOrder) {
            this.task = task;
            this.priority = priority;
            this.insertionOrder = insertionOrder;
        }
    }

    static class TieBreakingPrioritizedRunnable extends PrioritizedRunnable {

        final Runnable runnable;
        final long insertionOrder;

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
            runnable.run();
        }

        @Override
        public int compareTo(PrioritizedRunnable pr) {
            int res = super.compareTo(pr);
            if (res != 0 || !(pr instanceof TieBreakingPrioritizedRunnable)) {
                return res;
            }
            return insertionOrder < ((TieBreakingPrioritizedRunnable) pr).insertionOrder ? -1 : 1;
        }
    }

    static class PrioritizedFutureTask<T> extends FutureTask<T> implements Comparable<PrioritizedFutureTask> {

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
