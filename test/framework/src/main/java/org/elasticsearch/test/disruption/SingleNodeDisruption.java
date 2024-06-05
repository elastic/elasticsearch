/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.disruption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Random;

import static org.junit.Assert.assertFalse;

public abstract class SingleNodeDisruption implements ServiceDisruptionScheme {

    protected final Logger logger = LogManager.getLogger(getClass());

    protected volatile String disruptedNode;
    protected volatile InternalTestCluster cluster;
    protected final Random random;

    public SingleNodeDisruption(Random random) {
        this.random = new Random(random.nextLong());
    }

    @Override
    public void applyToCluster(InternalTestCluster testCluster) {
        this.cluster = testCluster;
        if (disruptedNode == null) {
            String[] nodes = testCluster.getNodeNames();
            disruptedNode = nodes[random.nextInt(nodes.length)];
        }
    }

    @Override
    public void removeFromCluster(InternalTestCluster testCluster) {
        if (disruptedNode != null) {
            removeFromNode(disruptedNode, testCluster);
        }
    }

    @Override
    public synchronized void applyToNode(String node, InternalTestCluster testCluster) {

    }

    @Override
    public synchronized void removeFromNode(String node, InternalTestCluster testCluster) {
        if (disruptedNode == null) {
            return;
        }
        if (node.equals(disruptedNode) == false) {
            return;
        }
        stopDisrupting();
        disruptedNode = null;
    }

    @Override
    public synchronized void testClusterClosed() {
        disruptedNode = null;
    }

    protected void ensureNodeCount(InternalTestCluster testCluster) {
        assertFalse(
            "cluster failed to form after disruption was healed",
            testCluster.client()
                .admin()
                .cluster()
                .prepareHealth()
                .setWaitForNodes(String.valueOf(testCluster.size()))
                .setWaitForNoRelocatingShards(true)
                .get()
                .isTimedOut()
        );
    }
}
