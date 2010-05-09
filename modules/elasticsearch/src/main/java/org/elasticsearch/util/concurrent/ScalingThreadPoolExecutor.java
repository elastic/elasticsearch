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

package org.elasticsearch.util.concurrent;

import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.concurrent.jsr166y.LinkedTransferQueue;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author kimchy (shay.banon)
 */
public class ScalingThreadPoolExecutor extends AbstractExecutorService {

    private final BlockingQueue<Runnable> workQueue = new LinkedTransferQueue<Runnable>();

    private final AtomicInteger queueSize = new AtomicInteger();

    /**
     * Lock held on updates to poolSize, corePoolSize,
     * maximumPoolSize, runState, and workers set.
     */
    private final ReentrantLock mainLock = new ReentrantLock();

    /**
     * Wait condition to support awaitTermination
     */
    private final Condition termination = mainLock.newCondition();

    /**
     * Set containing all worker threads in pool. Accessed only when
     * holding mainLock.
     */
    private final HashSet<Worker> workers = new HashSet<Worker>();


    /**
     * Factory for new threads. All threads are created using this
     * factory (via method addThread).  All callers must be prepared
     * for addThread to fail by returning null, which may reflect a
     * system or user's policy limiting the number of threads.  Even
     * though it is not treated as an error, failure to create threads
     * may result in new tasks being rejected or existing ones
     * remaining stuck in the queue. On the other hand, no special
     * precautions exist to handle OutOfMemoryErrors that might be
     * thrown while trying to create threads, since there is generally
     * no recourse from within this class.
     */
    private final ThreadFactory threadFactory;

    /**
     * runState provides the main lifecyle control, taking on values:
     *
     * RUNNING:  Accept new tasks and process queued tasks
     * SHUTDOWN: Don't accept new tasks, but process queued tasks
     * STOP:     Don't accept new tasks, don't process queued tasks,
     * and interrupt in-progress tasks
     * TERMINATED: Same as STOP, plus all threads have terminated
     *
     * The numerical order among these values matters, to allow
     * ordered comparisons. The runState monotonically increases over
     * time, but need not hit each state. The transitions are:
     *
     * RUNNING -> SHUTDOWN
     * On invocation of shutdown(), perhaps implicitly in finalize()
     * (RUNNING or SHUTDOWN) -> STOP
     * On invocation of shutdownNow()
     * SHUTDOWN -> TERMINATED
     * When both queue and pool are empty
     * STOP -> TERMINATED
     * When pool is empty
     */
    volatile int runState;
    static final int RUNNING = 0;
    static final int SHUTDOWN = 1;
    static final int STOP = 2;
    static final int TERMINATED = 3;


    /**
     * Core pool size, updated only while holding mainLock, but
     * volatile to allow concurrent readability even during updates.
     */
    private final int corePoolSize;

    /**
     * Maximum pool size, updated only while holding mainLock but
     * volatile to allow concurrent readability even during updates.
     */
    private final int maximumPoolSize;

    /**
     * Timeout in nanoseconds for idle threads waiting for work.
     * Threads use this timeout when there are more than corePoolSize
     * present or if allowCoreThreadTimeOut. Otherwise they wait
     * forever for new work.
     */
    private final long keepAliveTime;

    /**
     * Current pool size, updated only while holding mainLock but
     * volatile to allow concurrent readability even during updates.
     */
    private volatile int poolSize;


    private final ScheduledFuture scheduledFuture;

    public ScalingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, TimeValue keepAlive,
                                     ThreadFactory threadFactory,
                                     ScheduledExecutorService scheduler, TimeValue schedulerInterval) {
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.keepAliveTime = keepAlive.nanos();
        this.threadFactory = threadFactory;

        for (int i = 0; i < corePoolSize; i++) {
            Thread t = addThread();
            if (t != null)
                t.start();
        }

        this.scheduledFuture = scheduler.scheduleWithFixedDelay(new Scheduler(), schedulerInterval.nanos(),
                schedulerInterval.nanos(), TimeUnit.NANOSECONDS);
    }


    @Override public void execute(Runnable command) {
        queueSize.incrementAndGet();
        workQueue.add(command);
    }

    @Override public void shutdown() {
        if (!scheduledFuture.isCancelled()) {
            scheduledFuture.cancel(false);
        }
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            int state = runState;
            if (state < SHUTDOWN)
                runState = SHUTDOWN;

            try {
                for (Worker w : workers) {
                    w.interruptIfIdle();
                }
            } catch (SecurityException se) { // Try to back out
                runState = state;
                // tryTerminate() here would be a no-op
                throw se;
            }

            tryTerminate(); // Terminate now if pool and queue empty
        } finally {
            mainLock.unlock();
        }
    }

    @Override public List<Runnable> shutdownNow() {
        if (!scheduledFuture.isCancelled()) {
            scheduledFuture.cancel(false);
        }
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            int state = runState;
            if (state < STOP)
                runState = STOP;

            try {
                for (Worker w : workers) {
                    w.interruptNow();
                }
            } catch (SecurityException se) { // Try to back out
                runState = state;
                // tryTerminate() here would be a no-op
                throw se;
            }

            List<Runnable> tasks = drainQueue();
            tryTerminate(); // Terminate now if pool and queue empty
            return tasks;
        } finally {
            mainLock.unlock();
        }
    }

    @Override public boolean isShutdown() {
        return runState != RUNNING;
    }

    @Override public boolean isTerminated() {
        return runState == TERMINATED;
    }

    @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            for (; ;) {
                if (runState == TERMINATED)
                    return true;
                if (nanos <= 0)
                    return false;
                nanos = termination.awaitNanos(nanos);
            }
        } finally {
            mainLock.unlock();
        }
    }

    /**
     * Returns the current number of threads in the pool.
     *
     * @return the number of threads
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Returns the approximate number of threads that are actively
     * executing tasks.
     *
     * @return the number of threads
     */
    public int getActiveCount() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            int n = 0;
            for (Worker w : workers) {
                if (w.isActive())
                    ++n;
            }
            return n;
        } finally {
            mainLock.unlock();
        }
    }

    private final class Scheduler implements Runnable {
        @Override public void run() {
            if (queueSize.get() > 0 && poolSize < maximumPoolSize) {
                final ReentrantLock mainLock = ScalingThreadPoolExecutor.this.mainLock;
                mainLock.lock();
                try {
                    int currentQueueSize = queueSize.get();
                    if (currentQueueSize > 0 && poolSize < maximumPoolSize) {
                        int incrementBy = currentQueueSize;
                        if (poolSize + incrementBy > maximumPoolSize) {
                            incrementBy = maximumPoolSize - poolSize;
                        }
                        for (int i = 0; i < incrementBy; i++) {
                            Thread t = addThread();
                            if (t != null)
                                t.start();
                        }
                    }
                } finally {
                    mainLock.unlock();
                }
            }
        }
    }


    private final class Worker implements Runnable {
        /**
         * The runLock is acquired and released surrounding each task
         * execution. It mainly protects against interrupts that are
         * intended to cancel the worker thread from instead
         * interrupting the task being run.
         */
        private final ReentrantLock runLock = new ReentrantLock();

        /**
         * Thread this worker is running in.  Acts as a final field,
         * but cannot be set until thread is created.
         */
        Thread thread;

        Worker() {
        }

        boolean isActive() {
            return runLock.isLocked();
        }

        /**
         * Interrupts thread if not running a task.
         */
        void interruptIfIdle() {
            final ReentrantLock runLock = this.runLock;
            if (runLock.tryLock()) {
                try {
                    if (thread != Thread.currentThread())
                        thread.interrupt();
                } finally {
                    runLock.unlock();
                }
            }
        }

        /**
         * Interrupts thread even if running a task.
         */
        void interruptNow() {
            thread.interrupt();
        }

        /**
         * Runs a single task between before/after methods.
         */
        private void runTask(Runnable task) {
            final ReentrantLock runLock = this.runLock;
            runLock.lock();
            try {
                /*
                 * Ensure that unless pool is stopping, this thread
                 * does not have its interrupt set. This requires a
                 * double-check of state in case the interrupt was
                 * cleared concurrently with a shutdownNow -- if so,
                 * the interrupt is re-enabled.
                 */
                if (runState < STOP && Thread.interrupted() && runState >= STOP)
                    thread.interrupt();

                task.run();
            } finally {
                runLock.unlock();
            }
        }

        /**
         * Main run loop
         */
        public void run() {
            try {
                Runnable task;
                while ((task = getTask()) != null) {
                    runTask(task);
                }
            } finally {
                workerDone(this);
            }
        }
    }


    Runnable getTask() {
        for (; ;) {
            try {
                int state = runState;
                if (state > SHUTDOWN)
                    return null;
                Runnable r;
                if (state == SHUTDOWN)  // Help drain queue
                    r = workQueue.poll();
                else if (poolSize > corePoolSize)
                    r = workQueue.poll(keepAliveTime, TimeUnit.NANOSECONDS);
                else
                    r = workQueue.take();
                if (r != null) {
                    queueSize.decrementAndGet();
                    return r;
                }
                if (workerCanExit()) {
                    if (runState >= SHUTDOWN) // Wake up others
                        interruptIdleWorkers();
                    return null;
                }
                // Else retry
            } catch (InterruptedException ie) {
                // On interruption, re-check runState
            }
        }
    }

    /**
     * Check whether a worker thread that fails to get a task can
     * exit.  We allow a worker thread to die if the pool is stopping,
     * or the queue is empty, or there is at least one thread to
     * handle possibly non-empty queue, even if core timeouts are
     * allowed.
     */
    private boolean workerCanExit() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        boolean canExit;
        try {
            canExit = runState >= STOP || queueSize.get() == 0;
        } finally {
            mainLock.unlock();
        }
        return canExit;
    }

    /**
     * Wakes up all threads that might be waiting for tasks so they
     * can check for termination. Note: this method is also called by
     * ScheduledThreadPoolExecutor.
     */
    void interruptIdleWorkers() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            for (Worker w : workers)
                w.interruptIfIdle();
        } finally {
            mainLock.unlock();
        }
    }

    /**
     * Performs bookkeeping for an exiting worker thread.
     *
     * @param w the worker
     */
    void workerDone(Worker w) {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            workers.remove(w);
            if (--poolSize == 0)
                tryTerminate();
        } finally {
            mainLock.unlock();
        }
    }

    /**
     * Transitions to TERMINATED state if either (SHUTDOWN and pool
     * and queue empty) or (STOP and pool empty), otherwise unless
     * stopped, ensuring that there is at least one live thread to
     * handle queued tasks.
     *
     * This method is called from the three places in which
     * termination can occur: in workerDone on exit of the last thread
     * after pool has been shut down, or directly within calls to
     * shutdown or shutdownNow, if there are no live threads.
     */
    private void tryTerminate() {
        if (poolSize == 0) {
            int state = runState;
            if (state < STOP && queueSize.get() > 0) {
                state = RUNNING; // disable termination check below
                Thread t = addThread();
                if (t != null)
                    t.start();
            }
            if (state == STOP || state == SHUTDOWN) {
                runState = TERMINATED;
                termination.signalAll();
            }
        }
    }

    /**
     * Creates and returns a new thread running firstTask as its first
     * task. Call only while holding mainLock.
     */
    private Thread addThread() {
        Worker w = new Worker();
        Thread t = threadFactory.newThread(w);
        if (t != null) {
            w.thread = t;
            workers.add(w);
            ++poolSize;
        }
        return t;
    }

    /**
     * Drains the task queue into a new list. Used by shutdownNow.
     * Call only while holding main lock.
     */
    private List<Runnable> drainQueue() {
        List<Runnable> taskList = new ArrayList<Runnable>();
        workQueue.drainTo(taskList);
        queueSize.getAndAdd(taskList.size() * -1);
        /*
         * If the queue is a DelayQueue or any other kind of queue
         * for which poll or drainTo may fail to remove some elements,
         * we need to manually traverse and remove remaining tasks.
         * To guarantee atomicity wrt other threads using this queue,
         * we need to create a new iterator for each element removed.
         */
        while (!workQueue.isEmpty()) {
            Iterator<Runnable> it = workQueue.iterator();
            try {
                if (it.hasNext()) {
                    Runnable r = it.next();
                    if (workQueue.remove(r)) {
                        taskList.add(r);
                        queueSize.decrementAndGet();
                    }
                }
            } catch (ConcurrentModificationException ignore) {
            }
        }
        return taskList;
    }
}
