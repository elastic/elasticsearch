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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertFalse;

public abstract class MultiNodesDisruption implements ServiceDisruptionScheme {

    protected final Logger logger = Loggers.getLogger(getClass());

    protected volatile List<String> disruptedNodes;
    protected volatile InternalTestCluster cluster;
    protected final Random random;

    public MultiNodesDisruption(Random random, String ... disruptedNodes) {
        this(random);
        this.disruptedNodes = disruptedNodes != null ? new CopyOnWriteArrayList<>(Arrays.asList(disruptedNodes)) : null;
    }

    public MultiNodesDisruption(Random random) {
        this.random = new Random(random.nextLong());
    }

    @Override
    public void applyToCluster(InternalTestCluster cluster) {
        this.cluster = cluster;
        if (disruptedNodes == null) {
            disruptedNodes = new CopyOnWriteArrayList<>(Arrays.asList(cluster.getNodeNames()));
        }
    }

    @Override
    public void removeFromCluster(InternalTestCluster cluster) {
        List<String> nodes = this.disruptedNodes;
        if (nodes != null) {
            for (String node : nodes) {
                removeFromNode(node, cluster);
            }
        }
    }

    @Override
    public synchronized void applyToNode(String node, InternalTestCluster cluster) {

    }

    @Override
    public synchronized void removeFromNode(String node, InternalTestCluster cluster) {
        List<String> nodes = this.disruptedNodes;
        if (nodes == null) {
            return;
        }
        if (!nodes.remove(node)) {
            return;
        }
        if (nodes.isEmpty()) {
            stopDisrupting();
            disruptedNodes = null;
        }
    }

    @Override
    public synchronized void testClusterClosed() {
        disruptedNodes = null;
    }

    protected void ensureNodeCount(InternalTestCluster cluster) {
        assertFalse("cluster failed to form after disruption was healed", cluster.client().admin().cluster().prepareHealth()
                .setWaitForNodes(String.valueOf(cluster.size()))
                .setWaitForNoRelocatingShards(true)
                .get().isTimedOut());
    }
}
