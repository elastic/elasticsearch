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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ThreadPoolMergeQueue {
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
    private final AtomicInteger activeIOThrottledMergeTasksCount = new AtomicInteger();
    private final PriorityBlockingQueue<MergeTask> queuedMergeTasks = new PriorityBlockingQueue<>();
    // the set of all merge tasks currently being executed by merge threads from the pool,
    // in order to be able to update the IO throttle rate of merge tasks also after they have started (while executing)
    private final Set<MergeTask> currentlyRunningMergeTasks = Collections.synchronizedSet(new HashSet<>());
    /**
     * Current IO write throttle rate, for all merge, across all merge schedulers (shards) on the node
     */
    private final AtomicReference<Double> targetMBPerSec = new AtomicReference<>(START_MB_PER_SEC);
    private final ExecutorService executorService;
    private final int maxConcurrentMerges;

    public ThreadPoolMergeQueue(ThreadPool threadPool) {
        this.executorService = threadPool.executor(ThreadPool.Names.MERGE);
        this.maxConcurrentMerges = threadPool.info(ThreadPool.Names.MERGE).getMax();
    }

    void enqueueMergeTask(MergeTask mergeTask) {
        queuedMergeTasks.add(mergeTask);
        if (mergeTask.supportsIOThrottling()) {
            activeIOThrottledMergeTasksCount.incrementAndGet();
            maybeUpdateTargetMBPerSec();
        }
    }

    void executeNextMergeTask() {
        executorService.execute(() -> {
            // one such task always executes a SINGLE merge task; this is important for merge queue statistics
            while (true) {
                MergeTask smallestMergeTask;
                try {
                    // will block if there are backlogged merges until they're enqueued again
                    smallestMergeTask = queuedMergeTasks.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (smallestMergeTask.runNowOrBacklog()) {
                    runMergeTask(smallestMergeTask);
                    // one runnable one merge task
                    return;
                }
            }
        });
    }

    private void runMergeTask(MergeTask mergeTask) {
        assert mergeTask.isRunning() == false;
        boolean added = currentlyRunningMergeTasks.add(mergeTask);
        assert added : "starting merge task [" + mergeTask + "] registered as already running";
        try {
            if (mergeTask.supportsIOThrottling()) {
                mergeTask.setIORateLimit(targetMBPerSec.get());
            }
            mergeTask.run();
        } finally {
            boolean removed = currentlyRunningMergeTasks.remove(mergeTask);
            assert removed : "completed merge task [" + mergeTask + "] not registered as running";
            if (mergeTask.supportsIOThrottling()) {
                activeIOThrottledMergeTasksCount.decrementAndGet();
                maybeUpdateTargetMBPerSec();
            }
        }
    }

    private void maybeUpdateTargetMBPerSec() {
        double currentTargetMBPerSec = targetMBPerSec.get();
        final double newTargetMBPerSec;
        if (activeIOThrottledMergeTasksCount.get() < maxConcurrentMerges * 2 && currentTargetMBPerSec > MIN_MERGE_MB_PER_SEC) {
            newTargetMBPerSec = Math.max(MIN_MERGE_MB_PER_SEC, currentTargetMBPerSec / 1.1);
        } else if (activeIOThrottledMergeTasksCount.get() > maxConcurrentMerges * 4 && currentTargetMBPerSec < MAX_MERGE_MB_PER_SEC) {
            newTargetMBPerSec = Math.min(MAX_MERGE_MB_PER_SEC, currentTargetMBPerSec * 1.1);
        } else {
            newTargetMBPerSec = currentTargetMBPerSec;
        }
        if (newTargetMBPerSec != currentTargetMBPerSec && targetMBPerSec.compareAndSet(currentTargetMBPerSec, newTargetMBPerSec)) {
            currentlyRunningMergeTasks.forEach(mergeTask -> {
                if (mergeTask.supportsIOThrottling()) {
                    mergeTask.setIORateLimit(newTargetMBPerSec);
                }
            });
        }
    }

    // exposed for stats
    double getTargetMBPerSec() {
        return targetMBPerSec.get();
    }
}
