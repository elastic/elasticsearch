/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.disruption;

import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
        assertTrue(disruptionLatch.compareAndSet(null, new CountDownLatch(1)));
        final CountDownLatch started = new CountDownLatch(1);
        clusterService.getClusterApplierService().runOnApplierThread("service_disruption_block", Priority.IMMEDIATE, currentState -> {
            started.countDown();
            CountDownLatch latch = disruptionLatch.get();
            assertNotNull(latch);
            try {
                logger.info("waiting for removal of cluster state update disruption on node [{}]", disruptionNodeCopy);
                latch.await();
                logger.info("removing cluster state update disruption on node [{}]", disruptionNodeCopy);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("interrupted during disruption", e);
                Throwables.rethrow(e);
            }
        }, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error during disruption", e);
                assert false : e;
            }
        });
        try {
            started.await();
            logger.info("cluster state updates on node [{}] are now being delayed", disruptionNodeCopy);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("interrupted waiting for disruption to start", e);
            assert false : e;
        }
    }

    @Override
    public void stopDisrupting() {
        CountDownLatch latch = disruptionLatch.get();
        assertNotNull(latch);
        latch.countDown();
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
