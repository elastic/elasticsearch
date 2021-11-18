/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.disruption;

import org.elasticsearch.core.TimeValue;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulates irregular long gc intervals.
 */
public class IntermittentLongGCDisruption extends LongGCDisruption {

    volatile boolean disrupting;
    volatile Thread worker;

    final long intervalBetweenDelaysMin;
    final long intervalBetweenDelaysMax;
    final long delayDurationMin;
    final long delayDurationMax;

    public IntermittentLongGCDisruption(
        Random random,
        String disruptedNode,
        long intervalBetweenDelaysMin,
        long intervalBetweenDelaysMax,
        long delayDurationMin,
        long delayDurationMax
    ) {
        super(random, disruptedNode);
        this.intervalBetweenDelaysMin = intervalBetweenDelaysMin;
        this.intervalBetweenDelaysMax = intervalBetweenDelaysMax;
        this.delayDurationMin = delayDurationMin;
        this.delayDurationMax = delayDurationMax;
    }

    static final AtomicInteger thread_ids = new AtomicInteger();

    @Override
    public void startDisrupting() {
        disrupting = true;
        worker = new Thread(new BackgroundWorker(), "long_gc_simulation_" + thread_ids.incrementAndGet());
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void stopDisrupting() {
        if (worker == null) {
            return;
        }
        logger.info("stopping long GCs on [{}]", disruptedNode);
        disrupting = false;
        worker.interrupt();
        try {
            worker.join(2 * (intervalBetweenDelaysMax + delayDurationMax));
        } catch (InterruptedException e) {
            logger.info("background thread failed to stop");
        }
        worker = null;
    }

    private void simulateLongGC(final TimeValue duration) throws InterruptedException {
        logger.info("node [{}] goes into GC for for [{}]", disruptedNode, duration);
        final Set<Thread> nodeThreads = new HashSet<>();
        try {
            while (suspendThreads(nodeThreads))
                ;
            if (nodeThreads.isEmpty() == false) {
                Thread.sleep(duration.millis());
            }
        } finally {
            logger.info("node [{}] resumes from GC", disruptedNode);
            resumeThreads(nodeThreads);
        }
    }

    class BackgroundWorker implements Runnable {

        @Override
        public void run() {
            while (disrupting) {
                try {
                    TimeValue duration = new TimeValue(delayDurationMin + random.nextInt((int) (delayDurationMax - delayDurationMin)));
                    simulateLongGC(duration);

                    duration = new TimeValue(
                        intervalBetweenDelaysMin + random.nextInt((int) (intervalBetweenDelaysMax - intervalBetweenDelaysMin))
                    );
                    if (disrupting) {
                        Thread.sleep(duration.millis());
                    }
                } catch (InterruptedException e) {} catch (Exception e) {
                    logger.error("error in background worker", e);
                }
            }
        }
    }

}
