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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public abstract class NetworkPartition implements ServiceDisruptionScheme {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    final Set<String> nodesSideOne;
    final Set<String> nodesSideTwo;
    volatile boolean autoExpand;
    protected final Random random;
    protected volatile InternalTestCluster cluster;
    protected volatile boolean activeDisruption = false;


    public NetworkPartition(Random random) {
        this.random = new Random(random.nextLong());
        nodesSideOne = new HashSet<>();
        nodesSideTwo = new HashSet<>();
        autoExpand = true;
    }

    public NetworkPartition(String node1, String node2, Random random) {
        this(random);
        nodesSideOne.add(node1);
        nodesSideTwo.add(node2);
        autoExpand = false;
    }

    public NetworkPartition(Set<String> nodesSideOne, Set<String> nodesSideTwo, Random random) {
        this(random);
        this.nodesSideOne.addAll(nodesSideOne);
        this.nodesSideTwo.addAll(nodesSideTwo);
        autoExpand = false;
    }


    public List<String> getNodesSideOne() {
        return ImmutableList.copyOf(nodesSideOne);
    }

    public List<String> getNodesSideTwo() {
        return ImmutableList.copyOf(nodesSideTwo);
    }

    public List<String> getMajoritySide() {
        if (nodesSideOne.size() >= nodesSideTwo.size()) {
            return getNodesSideOne();
        } else {
            return getNodesSideTwo();
        }
    }

    public List<String> getMinoritySide() {
        if (nodesSideOne.size() >= nodesSideTwo.size()) {
            return getNodesSideTwo();
        } else {
            return getNodesSideOne();
        }
    }

    @Override
    public void applyToCluster(InternalTestCluster cluster) {
        this.cluster = cluster;
        if (autoExpand) {
            for (String node : cluster.getNodeNames()) {
                applyToNode(node, cluster);
            }
        }
    }

    @Override
    public void removeFromCluster(InternalTestCluster cluster) {
        stopDisrupting();
    }

    @Override
    public synchronized void applyToNode(String node, InternalTestCluster cluster) {
        if (!autoExpand || nodesSideOne.contains(node) || nodesSideTwo.contains(node)) {
            return;
        }
        if (nodesSideOne.isEmpty()) {
            nodesSideOne.add(node);
        } else if (nodesSideTwo.isEmpty()) {
            nodesSideTwo.add(node);
        } else if (random.nextBoolean()) {
            nodesSideOne.add(node);
        } else {
            nodesSideTwo.add(node);
        }
    }

    @Override
    public synchronized void removeFromNode(String node, InternalTestCluster cluster) {
        MockTransportService transportService = (MockTransportService) cluster.getInstance(TransportService.class, node);
        DiscoveryNode discoveryNode = discoveryNode(node);
        Set<String> otherSideNodes;
        if (nodesSideOne.contains(node)) {
            otherSideNodes = nodesSideTwo;
        } else if (nodesSideTwo.contains(node)) {
            otherSideNodes = nodesSideOne;
        } else {
            return;
        }
        for (String node2 : otherSideNodes) {
            MockTransportService transportService2 = (MockTransportService) cluster.getInstance(TransportService.class, node2);
            DiscoveryNode discoveryNode2 = discoveryNode(node2);
            removeDisruption(discoveryNode, transportService, discoveryNode2, transportService2);
        }
    }

    @Override
    public synchronized void testClusterClosed() {

    }

    protected abstract String getPartitionDescription();


    protected DiscoveryNode discoveryNode(String node) {
        return cluster.getInstance(Discovery.class, node).localNode();
    }

    @Override
    public synchronized void startDisrupting() {
        if (nodesSideOne.size() == 0 || nodesSideTwo.size() == 0) {
            return;
        }
        logger.info("nodes {} will be partitioned from {}. partition type [{}]", nodesSideOne, nodesSideTwo, getPartitionDescription());
        activeDisruption = true;
        for (String node1 : nodesSideOne) {
            MockTransportService transportService1 = (MockTransportService) cluster.getInstance(TransportService.class, node1);
            DiscoveryNode discoveryNode1 = discoveryNode(node1);
            for (String node2 : nodesSideTwo) {
                DiscoveryNode discoveryNode2 = discoveryNode(node2);
                MockTransportService transportService2 = (MockTransportService) cluster.getInstance(TransportService.class, node2);
                applyDisruption(discoveryNode1, transportService1, discoveryNode2, transportService2);
            }
        }
    }


    @Override
    public synchronized void stopDisrupting() {
        if (nodesSideOne.size() == 0 || nodesSideTwo.size() == 0 || !activeDisruption) {
            return;
        }
        logger.info("restoring partition between nodes {} & nodes {}", nodesSideOne, nodesSideTwo);
        for (String node1 : nodesSideOne) {
            MockTransportService transportService1 = (MockTransportService) cluster.getInstance(TransportService.class, node1);
            DiscoveryNode discoveryNode1 = discoveryNode(node1);
            for (String node2 : nodesSideTwo) {
                DiscoveryNode discoveryNode2 = discoveryNode(node2);
                MockTransportService transportService2 = (MockTransportService) cluster.getInstance(TransportService.class, node2);
                removeDisruption(discoveryNode1, transportService1, discoveryNode2, transportService2);
            }
        }
        activeDisruption = false;
    }

    abstract void applyDisruption(DiscoveryNode node1, MockTransportService transportService1,
                                  DiscoveryNode node2, MockTransportService transportService2);


    protected void removeDisruption(DiscoveryNode node1, MockTransportService transportService1,
                                    DiscoveryNode node2, MockTransportService transportService2) {
        transportService1.clearRule(node2);
        transportService2.clearRule(node1);
    }

}
