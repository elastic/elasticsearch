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

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateNonMasterUpdateTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
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
        clusterService.submitStateUpdateTask("service_disruption_delay", Priority.IMMEDIATE, new ClusterStateNonMasterUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                long count = duration.millis() / 200;
                // wait while checking for a stopped
                for (; count > 0 && !stopped.get(); count--) {
                    Thread.sleep(200);
                }
                if (!stopped.get()) {
                    Thread.sleep(duration.millis() % 200);
                }
                countDownLatch.countDown();
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                countDownLatch.countDown();
            }
        });
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
                    if (!interruptClusterStateProcessing(duration)) {
                        continue;
                    }
                    if (intervalBetweenDelaysMax > 0) {
                        duration = new TimeValue(intervalBetweenDelaysMin + random.nextInt((int) (intervalBetweenDelaysMax - intervalBetweenDelaysMin)));
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
