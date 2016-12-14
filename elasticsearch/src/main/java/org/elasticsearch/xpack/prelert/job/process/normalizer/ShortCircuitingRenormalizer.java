/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.normalizer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * Renormalizer that discards outdated quantiles if even newer ones are received while waiting for a prior renormalization to complete.
 */
public class ShortCircuitingRenormalizer implements Renormalizer {

    private static final Logger LOGGER = Loggers.getLogger(ShortCircuitingRenormalizer.class);

    private final String jobId;
    private final ScoresUpdater scoresUpdater;
    private final ExecutorService executorService;
    private final boolean isPerPartitionNormalization;
    private final Deque<Quantiles> quantilesDeque = new ConcurrentLinkedDeque<>();
    /**
     * Each job may only have 1 normalization in progress at any time; the semaphore enforces this
     */
    private final Semaphore semaphore = new Semaphore(1);
    /**
     * <code>null</code> means no normalization is in progress
     */
    private CountDownLatch completionLatch;

    public ShortCircuitingRenormalizer(String jobId, ScoresUpdater scoresUpdater, ExecutorService executorService,
                                       boolean isPerPartitionNormalization)
    {
        this.jobId = jobId;
        this.scoresUpdater = scoresUpdater;
        this.executorService = executorService;
        this.isPerPartitionNormalization = isPerPartitionNormalization;
    }

    public synchronized void renormalize(Quantiles quantiles)
    {
        quantilesDeque.addLast(quantiles);
        completionLatch = new CountDownLatch(1);
        executorService.submit(() -> doRenormalizations());
    }

    public void waitUntilIdle()
    {
        try {
            CountDownLatch latchToAwait = getCompletionLatch();
            while (latchToAwait != null) {
                latchToAwait.await();
                latchToAwait = getCompletionLatch();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private synchronized CountDownLatch getCompletionLatch() {
        return completionLatch;
    }

    private Quantiles getEarliestQuantiles() {
        return quantilesDeque.pollFirst();
    }

    private Quantiles getLatestQuantilesAndClear() {
        // We discard all but the latest quantiles
        Quantiles latestQuantiles = null;
        for (Quantiles quantiles = quantilesDeque.pollFirst(); quantiles != null; quantiles = quantilesDeque.pollFirst()) {
            latestQuantiles = quantiles;
        }
        return latestQuantiles;
    }

    private synchronized boolean tryStartWork() {
        return semaphore.tryAcquire();
    }

    private synchronized boolean tryFinishWork() {
        if (!quantilesDeque.isEmpty()) {
            return false;
        }
        semaphore.release();
        if (completionLatch != null) {
            completionLatch.countDown();
            completionLatch = null;
        }
        return true;
    }

    private synchronized void forceFinishWork() {
        semaphore.release();
        if (completionLatch != null) {
            completionLatch.countDown();
        }
    }

    private void doRenormalizations() {
        // Exit immediately if another normalization is in progress.  This means we don't hog threads.
        if (tryStartWork() == false) {
            return;
        }

        try {
            do {
                // Note that if there is only one set of quantiles in the queue then both these references will point to the same quantiles.
                Quantiles earliestQuantiles = getEarliestQuantiles();
                Quantiles latestQuantiles = getLatestQuantilesAndClear();
                // We could end up with latestQuantiles being null if the thread running this method was
                // preempted before the tryStartWork() call, another thread already running this method
                // did the work and exited, and then this thread got true returned by tryStartWork().
                if (latestQuantiles != null) {
                    // We could end up with earliestQuantiles being null if quantiles were
                    // added between getting the earliest and latest quantiles.
                    if (earliestQuantiles == null) {
                        earliestQuantiles = latestQuantiles;
                    }
                    long earliestBucketTimeMs = earliestQuantiles.getTimestamp().getTime();
                    long latestBucketTimeMs = latestQuantiles.getTimestamp().getTime();
                    // If we're going to skip quantiles, renormalize using the latest quantiles
                    // over the time ranges implied by all quantiles that were provided.
                    long windowExtensionMs = latestBucketTimeMs - earliestBucketTimeMs;
                    if (windowExtensionMs < 0) {
                        LOGGER.warn("[{}] Quantiles not supplied in order - {} after {}",
                                jobId, latestBucketTimeMs, earliestBucketTimeMs);
                    }
                    scoresUpdater.update(latestQuantiles.getQuantileState(), latestBucketTimeMs, windowExtensionMs,
                            isPerPartitionNormalization);
                }
                // Loop if more work has become available while we were working, because the
                // tasks originally submitted to do that work will have exited early.
            } while (tryFinishWork() == false);
        } catch (RuntimeException e) {
            forceFinishWork();
            throw e;
        }
    }
}
