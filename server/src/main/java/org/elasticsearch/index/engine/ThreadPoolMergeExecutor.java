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
                return System.identityHashCode(mergeTask1) - System.identityHashCode(mergeTask2);
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

    public ThreadPoolMergeExecutor(ThreadPool threadPool) {
        this.executorService = threadPool.executor(ThreadPool.Names.MERGE);
        this.maxConcurrentMerges = threadPool.info(ThreadPool.Names.MERGE).getMax();
    }

    public double getTargetMBPerSec() {
        return targetMBPerSec;
    }

    public synchronized void updateMergeScheduler(ThreadPoolMergeScheduler threadPoolMergeScheduler,
                                                  Consumer<ThreadPoolMergeScheduler> updater) {
        registeredMergeSchedulers.remove(threadPoolMergeScheduler);
        currentlyExecutingMergesCount -= threadPoolMergeScheduler.getCurrentlyRunningMergeTasks().size();
        updater.accept(threadPoolMergeScheduler);
        registeredMergeSchedulers.add(threadPoolMergeScheduler);
        currentlyExecutingMergesCount += threadPoolMergeScheduler.getCurrentlyRunningMergeTasks().size();
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
}
