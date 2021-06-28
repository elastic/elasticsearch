/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.disruption;

import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class BlockClusterStateProcessing extends SingleNodeDisruption {

    private final AtomicReference<CountDownLatch> disruptionLatch = new AtomicReference<>();

    public BlockClusterStateProcessing(String disruptedNode, Random random) {
        super(random);
        this.disruptedNode = disruptedNode;
    }


    @Override
    public void startDisrupting() {
        final String disruptionNodeCopy = disruptedNode;
        if (disruptionNodeCopy == null) {
            return;
        }
        ClusterService clusterService = cluster.getInstance(ClusterService.class, disruptionNodeCopy);
        if (clusterService == null) {
            return;
        }
        logger.info("delaying cluster state updates on node [{}]", disruptionNodeCopy);
        boolean success = disruptionLatch.compareAndSet(null, new CountDownLatch(1));
        assert success : "startDisrupting called without waiting on stopDisrupting to complete";
        final CountDownLatch started = new CountDownLatch(1);
        clusterService.getClusterApplierService().runOnApplierThread("service_disruption_block",
            currentState -> {
                started.countDown();
                CountDownLatch latch = disruptionLatch.get();
                if (latch != null) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        Throwables.rethrow(e);
                    }
                }
            }, (source, e) -> logger.error("unexpected error during disruption", e),
            Priority.IMMEDIATE);
        try {
            started.await();
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void stopDisrupting() {
        CountDownLatch latch = disruptionLatch.get();
        if (latch != null) {
            latch.countDown();
        }

    }

    @Override
    public void removeAndEnsureHealthy(InternalTestCluster cluster) {
        removeFromCluster(cluster);
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return TimeValue.timeValueMinutes(0);
    }
}
