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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final Set<MergeTask> currentlyRunningMergeTasks = new HashSet<>();
    /**
     * Current IO write throttle rate, for all merge, across all merge schedulers (shards) on the node
     */
    private double targetMBPerSec = START_MB_PER_SEC;
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
            // one such task always executes a single merge task; this is important for merge queue statistics
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
        synchronized (this) {
            boolean added = currentlyRunningMergeTasks.add(mergeTask);
            assert added : "failed to run merge task [" + mergeTask + "], it seems to already be running";
        }
        try {
            if (mergeTask.supportsIOThrottling()) {
                mergeTask.setIORateLimit(targetMBPerSec);
            }
            mergeTask.run();
        } finally {
            synchronized (this) {
                boolean removed = currentlyRunningMergeTasks.remove(mergeTask);
                assert removed : "completed merge task [" + mergeTask + "] was not running";
            }
            if (mergeTask.supportsIOThrottling()) {
                activeIOThrottledMergeTasksCount.decrementAndGet();
                maybeUpdateTargetMBPerSec();
            }
        }
    }

    // synchronized so that changes to the target IO rate and updates to the running merges' rates are atomic
    private synchronized void maybeUpdateTargetMBPerSec() {
        double newTargetMBPerSec = targetMBPerSec;
        if (activeIOThrottledMergeTasksCount.get() < maxConcurrentMerges * 2 && targetMBPerSec > MIN_MERGE_MB_PER_SEC) {
            newTargetMBPerSec = Math.max(MIN_MERGE_MB_PER_SEC, targetMBPerSec / 1.1);
        } else if (activeIOThrottledMergeTasksCount.get() > maxConcurrentMerges * 4 && targetMBPerSec < MAX_MERGE_MB_PER_SEC) {
            newTargetMBPerSec = Math.min(MAX_MERGE_MB_PER_SEC, targetMBPerSec * 1.1);
        }
        if (newTargetMBPerSec != targetMBPerSec) {
            targetMBPerSec = newTargetMBPerSec;
            for (MergeTask mergeTask : currentlyRunningMergeTasks) {
                mergeTask.setIORateLimit(targetMBPerSec);
            }
        }
    }

    double getTargetMBPerSec() {
        return targetMBPerSec;
    }
}
