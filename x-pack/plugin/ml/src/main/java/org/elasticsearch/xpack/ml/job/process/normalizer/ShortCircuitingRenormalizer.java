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

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * Renormalizer that discards outdated quantiles if even newer ones are received while waiting for a prior renormalization to complete.
 */
public class ShortCircuitingRenormalizer implements Renormalizer {

    private static final Logger logger = LogManager.getLogger(ShortCircuitingRenormalizer.class);

    private final String jobId;
    private final ScoresUpdater scoresUpdater;
    private final ExecutorService executorService;
    /**
     * Each job may only have 1 normalization in progress at any time; the semaphore enforces this
     */
    private final Semaphore semaphore = new Semaphore(1);
    private AugmentedQuantiles latestQuantilesHolder; // Access to this must be synchronized

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

        // Needed to ensure work is not added while the tryFinishWork() method is running
        synchronized (this) {
            latestQuantilesHolder = (latestQuantilesHolder == null)
                ? new AugmentedQuantiles(quantiles, null, new CountDownLatch(1))
                : new AugmentedQuantiles(quantiles, latestQuantilesHolder.getEvictedTimestamp(), latestQuantilesHolder.getLatch());
        }
        executorService.submit(this::doRenormalizations);
    }

    @Override
    public void waitUntilIdle() throws InterruptedException {
        CountDownLatch latch;
        do {
            synchronized (this) {
                latch = (latestQuantilesHolder != null) ? latestQuantilesHolder.getLatch() : null;
            }
            if (latch != null) {
                latch.await();
            }
            semaphore.acquire();
            semaphore.release();
        } while (latch != null);
    }

    @Override
    public void shutdown() throws InterruptedException {
        scoresUpdater.shutdown();
        // We have to wait until idle to avoid a raft of exceptions as other parts of the
        // system are stopped after this method returns. However, shutting down the
        // scoresUpdater first means it won't do all pending work; it will stop as soon
        // as it can without causing further errors.
        waitUntilIdle();
    }

    private synchronized AugmentedQuantiles getLatestAugmentedQuantilesAndClear() {
        AugmentedQuantiles latest = latestQuantilesHolder;
        latestQuantilesHolder = null;
        return latest;
    }

    private boolean tryStartWork() {
        return semaphore.tryAcquire();
    }

    private synchronized boolean tryFinishWork() {
        // Synchronized because we cannot tolerate new work being added in between the null check and releasing the semaphore
        if (latestQuantilesHolder != null) {
            return false;
        }
        semaphore.release();
        return true;
    }

    private void doRenormalizations() {
        // Exit immediately if another normalization is in progress. This means we don't hog threads.
        if (tryStartWork() == false) {
            return;
        }

        CountDownLatch latch = null;
        try {
            do {
                AugmentedQuantiles latestAugmentedQuantiles = getLatestAugmentedQuantilesAndClear();
                // We could end up with latestAugmentedQuantiles being null if the thread running this method
                // was preempted before the tryStartWork() call, another thread already running this method
                // did the work and exited, and then this thread got true returned by tryStartWork().
                if (latestAugmentedQuantiles != null) {
                    Quantiles latestQuantiles = latestAugmentedQuantiles.getQuantiles();
                    latch = latestAugmentedQuantiles.getLatch();
                    scoresUpdater.update(
                        latestQuantiles.getQuantileState(),
                        latestQuantiles.getTimestamp().getTime(),
                        latestAugmentedQuantiles.getWindowExtensionMs()
                    );
                    latch.countDown();
                    latch = null;
                }
                // Loop if more work has become available while we were working, because the
                // tasks originally submitted to do that work will have exited early.
            } while (tryFinishWork() == false);
        } catch (Exception e) {
            semaphore.release();
            if (latch != null) {
                latch.countDown();
            }
            logger.error("[" + jobId + "] Normalization failed", e);
        }
    }

    /**
     * Simple grouping of a {@linkplain Quantiles} object with its corresponding {@linkplain CountDownLatch} object.
     */
    private class AugmentedQuantiles {
        private final Quantiles quantiles;
        private final Date earliestEvictedTimestamp;
        private final CountDownLatch latch;

        AugmentedQuantiles(Quantiles quantiles, Date earliestEvictedTimestamp, CountDownLatch latch) {
            this.quantiles = Objects.requireNonNull(quantiles);
            this.earliestEvictedTimestamp = earliestEvictedTimestamp;
            this.latch = Objects.requireNonNull(latch);
        }

        Quantiles getQuantiles() {
            return quantiles;
        }

        Date getEvictedTimestamp() {
            return (earliestEvictedTimestamp != null) ? earliestEvictedTimestamp : quantiles.getTimestamp();
        }

        long getWindowExtensionMs() {
            if (earliestEvictedTimestamp == null) {
                return 0;
            }
            long earliestTimeMs = earliestEvictedTimestamp.getTime();
            long latestTimeMs = quantiles.getTimestamp().getTime();
            // If we're going to skip quantiles, renormalize using the latest quantiles
            // over the time ranges implied by all quantiles that were provided.
            long windowExtensionMs = latestTimeMs - earliestTimeMs;
            if (windowExtensionMs < 0) {
                logger.warn("[{}] Quantiles not supplied in time order - {} after {}", jobId, latestTimeMs, earliestTimeMs);
                return 0;
            }
            return windowExtensionMs;
        }

        CountDownLatch getLatch() {
            return latch;
        }
    }
}
