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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Random;

import static org.junit.Assert.assertFalse;

public abstract class SingleNodeDisruption implements ServiceDisruptionScheme {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    protected volatile String disruptedNode;
    protected volatile InternalTestCluster cluster;
    protected final Random random;


    public SingleNodeDisruption(String disruptedNode, Random random) {
        this(random);
        this.disruptedNode = disruptedNode;
    }

    public SingleNodeDisruption(Random random) {
        this.random = new Random(random.nextLong());
    }

    @Override
    public void applyToCluster(InternalTestCluster cluster) {
        this.cluster = cluster;
        if (disruptedNode == null) {
            String[] nodes = cluster.getNodeNames();
            disruptedNode = nodes[random.nextInt(nodes.length)];
        }
    }

    @Override
    public void removeFromCluster(InternalTestCluster cluster) {
        if (disruptedNode != null) {
            removeFromNode(disruptedNode, cluster);
        }
    }

    @Override
    public synchronized void applyToNode(String node, InternalTestCluster cluster) {

    }

    @Override
    public synchronized void removeFromNode(String node, InternalTestCluster cluster) {
        if (disruptedNode == null) {
            return;
        }
        if (!node.equals(disruptedNode)) {
            return;
        }
        stopDisrupting();
        disruptedNode = null;
    }

    @Override
    public synchronized void testClusterClosed() {
        disruptedNode = null;
    }

    protected void ensureNodeCount(InternalTestCluster cluster) {
        assertFalse("cluster failed to form after disruption was healed", cluster.client().admin().cluster().prepareHealth()
                .setWaitForNodes("" + cluster.size())
                .setWaitForRelocatingShards(0)
                .get().isTimedOut());
    }
}
