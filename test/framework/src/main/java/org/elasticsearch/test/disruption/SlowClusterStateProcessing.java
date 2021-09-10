/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.disruption;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlowClusterStateProcessing extends SingleNodeDisruption {

    volatile boolean disrupting;
    volatile Thread worker;

    final long intervalBetweenDelaysMin;
    final long intervalBetweenDelaysMax;
    final long delayDurationMin;
    final long delayDurationMax;


    public SlowClusterStateProcessing(Random random) {
        this(null, random);
    }

    public SlowClusterStateProcessing(String disruptedNode, Random random) {
        this(disruptedNode, random, 100, 200, 300, 20000);
    }

    public SlowClusterStateProcessing(String disruptedNode, Random random, long intervalBetweenDelaysMin,
                                      long intervalBetweenDelaysMax, long delayDurationMin, long delayDurationMax) {
        this(random, intervalBetweenDelaysMin, intervalBetweenDelaysMax, delayDurationMin, delayDurationMax);
        this.disruptedNode = disruptedNode;
    }

    public SlowClusterStateProcessing(Random random,
                                      long intervalBetweenDelaysMin, long intervalBetweenDelaysMax, long delayDurationMin,
                                      long delayDurationMax) {
        super(random);
        this.intervalBetweenDelaysMin = intervalBetweenDelaysMin;
        this.intervalBetweenDelaysMax = intervalBetweenDelaysMax;
        this.delayDurationMin = delayDurationMin;
        this.delayDurationMax = delayDurationMax;
    }


    @Override
    public void startDisrupting() {
        disrupting = true;
        worker = new Thread(new BackgroundWorker());
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void stopDisrupting() {
        if (worker == null) {
            return;
        }
        logger.info("stopping to slow down cluster state processing on [{}]", disruptedNode);
        disrupting = false;
        worker.interrupt();
        try {
            worker.join(2 * (intervalBetweenDelaysMax + delayDurationMax));
        } catch (InterruptedException e) {
            logger.info("background thread failed to stop");
        }
        worker = null;
    }


    private boolean interruptClusterStateProcessing(final TimeValue duration) throws InterruptedException {
        final String disruptionNodeCopy = disruptedNode;
        if (disruptionNodeCopy == null) {
            return false;
        }
        logger.info("delaying cluster state updates on node [{}] for [{}]", disruptionNodeCopy, duration);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ClusterService clusterService = cluster.getInstance(ClusterService.class, disruptionNodeCopy);
        if (clusterService == null) {
            return false;
        }
        final AtomicBoolean stopped = new AtomicBoolean(false);
        clusterService.getClusterApplierService().runOnApplierThread(
            "service_disruption_delay",
            Priority.IMMEDIATE,
            currentState -> {
                try {
                    long count = duration.millis() / 200;
                    // wait while checking for a stopped
                    for (; count > 0 && stopped.get() == false; count--) {
                        Thread.sleep(200);
                    }
                    if (stopped.get() == false) {
                        Thread.sleep(duration.millis() % 200);
                    }
                    countDownLatch.countDown();
                } catch (InterruptedException e) {
                    ExceptionsHelper.reThrowIfNotNull(e);
                }
            },
            e -> countDownLatch.countDown()
        );
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            stopped.set(true);
            // try to wait again, we really want the cluster state thread to be freed up when stopping disruption
            countDownLatch.await();
        }
        return true;
    }

    @Override
    public void removeAndEnsureHealthy(InternalTestCluster cluster) {
        removeFromCluster(cluster);
        ensureNodeCount(cluster);
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueMillis(0);
    }

    class BackgroundWorker implements Runnable {

        @Override
        public void run() {
            while (disrupting && disruptedNode != null) {
                try {
                    TimeValue duration = new TimeValue(delayDurationMin + random.nextInt((int) (delayDurationMax - delayDurationMin)));
                    if (interruptClusterStateProcessing(duration) == false) {
                        continue;
                    }
                    if (intervalBetweenDelaysMax > 0) {
                        duration = new TimeValue(intervalBetweenDelaysMin
                                + random.nextInt((int) (intervalBetweenDelaysMax - intervalBetweenDelaysMin)));
                        if (disrupting && disruptedNode != null) {
                            Thread.sleep(duration.millis());
                        }
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    logger.error("error in background worker", e);
                }
            }
        }
    }

}
