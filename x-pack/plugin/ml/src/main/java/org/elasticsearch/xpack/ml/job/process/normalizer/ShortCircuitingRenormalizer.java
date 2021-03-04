/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;

import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * Renormalizer that discards outdated quantiles if even newer ones are received while waiting for a prior renormalization to complete.
 */
public class ShortCircuitingRenormalizer implements Renormalizer {

    private static final Logger LOGGER = LogManager.getLogger(ShortCircuitingRenormalizer.class);

    private final String jobId;
    private final ScoresUpdater scoresUpdater;
    private final ExecutorService executorService;
    private final Deque<QuantilesWithLatch> quantilesDeque = new ConcurrentLinkedDeque<>();
    private final Deque<CountDownLatch> latchDeque = new ConcurrentLinkedDeque<>();
    /**
     * Each job may only have 1 normalization in progress at any time; the semaphore enforces this
     */
    private final Semaphore semaphore = new Semaphore(1);

    public ShortCircuitingRenormalizer(String jobId, ScoresUpdater scoresUpdater, ExecutorService executorService) {
        this.jobId = jobId;
        this.scoresUpdater = scoresUpdater;
        this.executorService = executorService;
    }

    @Override
    public boolean isEnabled() {
        return scoresUpdater.getNormalizationWindow() > 0;
    }

    @Override
    public void renormalize(Quantiles quantiles) {
        if (isEnabled() == false) {
            return;
        }

        // This will throw NPE if quantiles is null, so do it first
        QuantilesWithLatch quantilesWithLatch = new QuantilesWithLatch(quantiles, new CountDownLatch(1));
        // Needed to ensure work is not added while the tryFinishWork() method is running
        synchronized (quantilesDeque) {
            // Must add to latchDeque before quantilesDeque
            latchDeque.addLast(quantilesWithLatch.getLatch());
            quantilesDeque.addLast(quantilesWithLatch);
            executorService.submit(() -> doRenormalizations());
        }
    }

    @Override
    public void waitUntilIdle() {
        try {
            // We cannot tolerate more than one thread running this loop at any time,
            // but need a different lock to the other synchronized parts of the code
            synchronized (latchDeque) {
                for (CountDownLatch latchToAwait = latchDeque.pollFirst(); latchToAwait != null; latchToAwait = latchDeque.pollFirst()) {
                    latchToAwait.await();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        scoresUpdater.shutdown();
        // We have to wait until idle to avoid a raft of exceptions as other parts of the
        // system are stopped after this method returns.  However, shutting down the
        // scoresUpdater first means it won't do all pending work; it will stop as soon
        // as it can without causing further errors.
        waitUntilIdle();
    }

    private Quantiles getEarliestQuantiles() {
        QuantilesWithLatch earliestQuantilesWithLatch = quantilesDeque.peekFirst();
        return (earliestQuantilesWithLatch != null) ? earliestQuantilesWithLatch.getQuantiles() : null;
    }

    private QuantilesWithLatch getLatestQuantilesWithLatchAndClear() {
        // We discard all but the latest quantiles
        QuantilesWithLatch latestQuantilesWithLatch = null;
        for (QuantilesWithLatch quantilesWithLatch = quantilesDeque.pollFirst(); quantilesWithLatch != null;
             quantilesWithLatch = quantilesDeque.pollFirst()) {
            // Count down the latches associated with any discarded quantiles
            if (latestQuantilesWithLatch != null) {
                latestQuantilesWithLatch.getLatch().countDown();
            }
            latestQuantilesWithLatch = quantilesWithLatch;
        }
        return latestQuantilesWithLatch;
    }

    private boolean tryStartWork() {
        return semaphore.tryAcquire();
    }

    private boolean tryFinishWork() {
        // We cannot tolerate new work being added in between the isEmpty() check and releasing the semaphore
        synchronized (quantilesDeque) {
            if (quantilesDeque.isEmpty() == false) {
                return false;
            }
            semaphore.release();
            return true;
        }
    }

    private void forceFinishWork() {
        // We cannot allow new quantiles to be added while we are failing from a previous renormalization failure.
        synchronized (quantilesDeque) {
            // We discard all but the earliest quantiles, if they exist
            QuantilesWithLatch earliestQuantileWithLatch = null;
            for (QuantilesWithLatch quantilesWithLatch = quantilesDeque.pollFirst(); quantilesWithLatch != null;
                 quantilesWithLatch = quantilesDeque.pollFirst()) {
                if (earliestQuantileWithLatch == null) {
                    earliestQuantileWithLatch = quantilesWithLatch;
                }
                // Count down all the latches as they no longer matter since we failed
                quantilesWithLatch.latch.countDown();
            }
            // Keep the earliest quantile so that the next call to doRenormalizations() will include as much as the failed normalization
            // window as possible.
            // Since this latch is already countedDown, there is no reason to put it in the `latchDeque` again
            if (earliestQuantileWithLatch != null) {
                quantilesDeque.addLast(earliestQuantileWithLatch);
            }
            semaphore.release();
        }
    }

    private void doRenormalizations() {
        // Exit immediately if another normalization is in progress.  This means we don't hog threads.
        if (tryStartWork() == false) {
            return;
        }

        CountDownLatch latch = null;
        try {
            do {
                // Note that if there is only one set of quantiles in the queue then both these references will point to the same quantiles.
                Quantiles earliestQuantiles = getEarliestQuantiles();
                QuantilesWithLatch latestQuantilesWithLatch = getLatestQuantilesWithLatchAndClear();
                // We could end up with latestQuantilesWithLatch being null if the thread running this method
                // was preempted before the tryStartWork() call, another thread already running this method
                // did the work and exited, and then this thread got true returned by tryStartWork().
                if (latestQuantilesWithLatch != null) {
                    Quantiles latestQuantiles = latestQuantilesWithLatch.getQuantiles();
                    latch = latestQuantilesWithLatch.getLatch();
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
                        LOGGER.warn("[{}] Quantiles not supplied in time order - {} after {}",
                                jobId, latestBucketTimeMs, earliestBucketTimeMs);
                        windowExtensionMs = 0;
                    }
                    scoresUpdater.update(latestQuantiles.getQuantileState(), latestBucketTimeMs, windowExtensionMs);
                    latch.countDown();
                    latch = null;
                }
                // Loop if more work has become available while we were working, because the
                // tasks originally submitted to do that work will have exited early.
            } while (tryFinishWork() == false);
        } catch (Exception e) {
            LOGGER.error("[" + jobId + "] Normalization failed", e);
            if (latch != null) {
                latch.countDown();
            }
            forceFinishWork();
        }
    }

    /**
     * Simple grouping of a {@linkplain Quantiles} object with its corresponding {@linkplain CountDownLatch} object.
     */
    private static class QuantilesWithLatch {
        private final Quantiles quantiles;
        private final CountDownLatch latch;

        QuantilesWithLatch(Quantiles quantiles, CountDownLatch latch) {
            this.quantiles = Objects.requireNonNull(quantiles);
            this.latch = Objects.requireNonNull(latch);
        }

        Quantiles getQuantiles() {
            return quantiles;
        }

        CountDownLatch getLatch() {
            return latch;
        }
    }
}
