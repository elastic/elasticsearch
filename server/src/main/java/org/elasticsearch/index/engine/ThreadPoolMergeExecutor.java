/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.MergeTask;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class ThreadPoolMergeExecutor {
    /**
     * Floor for IO write rate limit (we will never go any lower than this)
     */
    private static final double MIN_MERGE_MB_PER_SEC = 5.0;
    /**
     * Ceiling for IO write rate limit (we will never go any higher than this)
     */
    private static final double MAX_MERGE_MB_PER_SEC = 10240.0;
    /**
     * Initial value for IO write rate limit when doAutoIOThrottle is true
     */
    private static final double START_MB_PER_SEC = 20.0;
    /**
     * Current IO write throttle rate, for all merge, across all merge schedulers (shards) on the node
     */
    private double targetMBPerSec = START_MB_PER_SEC;
    private final SortedSet<ThreadPoolMergeScheduler> registeredMergeSchedulers = new TreeSet<>(new Comparator<ThreadPoolMergeScheduler>() {
        @Override
        public int compare(ThreadPoolMergeScheduler tpms1, ThreadPoolMergeScheduler tpms2) {
            MergeTask mergeTask1 = tpms1.peekMergeTaskToExecute();
            MergeTask mergeTask2 = tpms2.peekMergeTaskToExecute();
            if (mergeTask1 == null && mergeTask2 == null) {
                // arbitrary order between schedulers that cannot run any merge right now
                return System.identityHashCode(tpms1) - System.identityHashCode(tpms2);
            } else if (mergeTask1 == null) {
                // "merge task 2" can run because "merge scheduler 1" cannot run any merges
                return 1;
            } else if (mergeTask2 == null) {
                // "merge task 1" can run because "merge scheduler 2" cannot run any merges
                return -1;
            } else {
                // run smaller merge task first
                return mergeTask1.compareTo(mergeTask2);
            }
        }
    });
    private final ExecutorService executorService;
    private final int maxConcurrentMerges;
    private int currentlyExecutingMergesCount;
    private int currentlyActiveIOThrottledMergesCount;

    public ThreadPoolMergeExecutor(ThreadPool threadPool) {
        this.executorService = threadPool.executor(ThreadPool.Names.MERGE);
        this.maxConcurrentMerges = threadPool.info(ThreadPool.Names.MERGE).getMax();
    }

    public double getTargetMBPerSec() {
        return targetMBPerSec;
    }

    public synchronized void registerMergeScheduler(ThreadPoolMergeScheduler threadPoolMergeScheduler) {
        if (registeredMergeSchedulers.add(threadPoolMergeScheduler) == false) {
            throw new IllegalStateException("cannot register the same scheduler multiple times");
        }
    }

    public synchronized void unregisterMergeScheduler(ThreadPoolMergeScheduler threadPoolMergeScheduler) {
        if (registeredMergeSchedulers.remove(threadPoolMergeScheduler) == false) {
            throw new IllegalStateException("cannot unregister if the scheduler has not been registered");
        }
    }

    public synchronized void updateMergeScheduler(
        ThreadPoolMergeScheduler threadPoolMergeScheduler,
        Consumer<ThreadPoolMergeScheduler> updater
    ) {
        boolean removed = registeredMergeSchedulers.remove(threadPoolMergeScheduler);
        if (false == removed) {
            throw new IllegalStateException("Cannot update a merge scheduler that is not registered");
        }
        currentlyExecutingMergesCount -= threadPoolMergeScheduler.getCurrentlyRunningMergeTasks().size();
        currentlyActiveIOThrottledMergesCount -= getIOThrottledMergeTasksCount(threadPoolMergeScheduler);
        updater.accept(threadPoolMergeScheduler);
        boolean added = registeredMergeSchedulers.add(threadPoolMergeScheduler);
        if (false == added) {
            throw new IllegalStateException("Found duplicate registered merge scheduler");
        }
        currentlyExecutingMergesCount += threadPoolMergeScheduler.getCurrentlyRunningMergeTasks().size();
        currentlyActiveIOThrottledMergesCount += getIOThrottledMergeTasksCount(threadPoolMergeScheduler);
        double newTargetMBPerSec = maybeUpdateTargetMBPerSec();
        if (newTargetMBPerSec != targetMBPerSec) {
            targetMBPerSec = newTargetMBPerSec;
            threadPoolMergeScheduler.setIORateLimitForAllMergeTasks(newTargetMBPerSec);
        }
        maybeExecuteNextMerges();
    }

    public synchronized void maybeExecuteNextMerges() {
        while (true) {
            if (currentlyExecutingMergesCount >= maxConcurrentMerges) {
                // all merge threads are busy
                return;
            }
            if (registeredMergeSchedulers.first().peekMergeTaskToExecute() == null) {
                // no merges available to run
                return;
            }
            ThreadPoolMergeScheduler threadPoolMergeScheduler = registeredMergeSchedulers.removeFirst();
            currentlyExecutingMergesCount -= threadPoolMergeScheduler.getCurrentlyRunningMergeTasks().size();
            MergeTask mergeTask = threadPoolMergeScheduler.executeNextMergeTask();
            assert mergeTask != null;
            executorService.execute(mergeTask);
            registeredMergeSchedulers.add(threadPoolMergeScheduler);
            currentlyExecutingMergesCount += threadPoolMergeScheduler.getCurrentlyRunningMergeTasks().size();
        }
    }

    private int getIOThrottledMergeTasksCount(ThreadPoolMergeScheduler mergeScheduler) {
        if (mergeScheduler.shouldIOThrottleMergeTasks() == false) {
            return 0;
        } else {
            int count = 0;
            for (MergeTask runningMergeTask : mergeScheduler.getCurrentlyRunningMergeTasks()) {
                if (runningMergeTask.supportsIOThrottling()) {
                    count++;
                }
            }
            for (MergeTask queuedMergeTask : mergeScheduler.getQueuedMergeTasks()) {
                if (queuedMergeTask.supportsIOThrottling()) {
                    count++;
                }
            }
            return count;
        }
    }

    private double maybeUpdateTargetMBPerSec() {
        if (currentlyActiveIOThrottledMergesCount < maxConcurrentMerges * 2 && targetMBPerSec > MIN_MERGE_MB_PER_SEC) {
            return Math.max(MIN_MERGE_MB_PER_SEC, targetMBPerSec / 1.1);
        } else if (currentlyActiveIOThrottledMergesCount > maxConcurrentMerges * 4 && targetMBPerSec < MAX_MERGE_MB_PER_SEC) {
            return Math.min(MAX_MERGE_MB_PER_SEC, targetMBPerSec * 1.1);
        }
        return targetMBPerSec;
    }
}
