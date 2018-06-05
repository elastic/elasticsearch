/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.test.disruption;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.InternalTestCluster;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlowClusterStateProcessing extends MultiNodesDisruption {

    volatile boolean disrupting;
    volatile Thread worker;

    final long intervalBetweenDelaysMin;
    final long intervalBetweenDelaysMax;
    final long delayDurationMin;
    final long delayDurationMax;

    public SlowClusterStateProcessing(Random random) {
        this(random, (String[])null);
    }

    public SlowClusterStateProcessing(Random random, String ... disruptedNodes) {
        this(disruptedNodes, random, 100, 200, 300, 20000);
    }

    public SlowClusterStateProcessing(String disruptedNode, Random random, long intervalBetweenDelaysMin,
                                      long intervalBetweenDelaysMax, long delayDurationMin, long delayDurationMax) {
        this(disruptedNode != null ? new String[]{disruptedNode} : null,
            random, intervalBetweenDelaysMin, intervalBetweenDelaysMax, delayDurationMin, delayDurationMax);
    }

    public SlowClusterStateProcessing(Random random,
                                      long intervalBetweenDelaysMin, long intervalBetweenDelaysMax, long delayDurationMin,
                                      long delayDurationMax) {
        this((String[])null, random, intervalBetweenDelaysMin, intervalBetweenDelaysMax, delayDurationMin, delayDurationMax);
    }

    public SlowClusterStateProcessing(String[] disruptedNodes, Random random, long intervalBetweenDelaysMin,
                                      long intervalBetweenDelaysMax, long delayDurationMin, long delayDurationMax) {
        super(random, disruptedNodes);
        this.intervalBetweenDelaysMin = intervalBetweenDelaysMin;
        this.intervalBetweenDelaysMax = intervalBetweenDelaysMax;
        this.delayDurationMin = delayDurationMin;
        this.delayDurationMax = delayDurationMax;
    }

    @Override
    public void startDisrupting() {
        disrupting = true;
        worker = new Thread(new BackgroundWorker());
        worker.setName("slow-cluster-worker");
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void stopDisrupting() {
        if (worker == null) {
            return;
        }
        logger.info("stopping to slow down cluster state processing on [{}]", disruptedNodes);
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
        final List<String> disruptionNodesCopy = disruptedNodes;
        if (disruptionNodesCopy == null || disruptionNodesCopy.isEmpty()) {
            return false;
        }
        final String disruptionNode = disruptionNodesCopy.get(random.nextInt(disruptionNodesCopy.size()));
        logger.info("delaying cluster state updates on node [{}] for [{}]", disruptionNode, duration);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ClusterService clusterService = cluster.getInstance(ClusterService.class, disruptionNode);
        if (clusterService == null) {
            return false;
        }
        final AtomicBoolean stopped = new AtomicBoolean(false);
        clusterService.getClusterApplierService().runOnApplierThread("service_disruption_delay",
            currentState -> {
                try {
                    long count = duration.millis() / 200;
                    // wait while checking for a stopped
                    for (; count > 0 && !stopped.get(); count--) {
                        Thread.sleep(200);
                    }
                    if (!stopped.get()) {
                        Thread.sleep(duration.millis() % 200);
                    }
                    countDownLatch.countDown();
                } catch (InterruptedException e) {
                    ExceptionsHelper.reThrowIfNotNull(e);
                }
            }, (source, e) -> countDownLatch.countDown(),
            Priority.IMMEDIATE);
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
            while (disrupting && disruptedNodes != null) {
                try {
                    TimeValue duration = new TimeValue(delayDurationMin + random.nextInt((int) (delayDurationMax - delayDurationMin)));
                    if (!interruptClusterStateProcessing(duration)) {
                        continue;
                    }
                    if (intervalBetweenDelaysMax > 0) {
                        duration = new TimeValue(intervalBetweenDelaysMin + random.nextInt((int) (intervalBetweenDelaysMax - intervalBetweenDelaysMin)));
                        if (disrupting && disruptedNodes != null) {
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
