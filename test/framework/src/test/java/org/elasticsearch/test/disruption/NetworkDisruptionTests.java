/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.disruption;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption.Bridge;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;

import java.util.HashSet;
import java.util.Set;

public class NetworkDisruptionTests extends ESTestCase {

    public void testTwoPartitions() {
        Set<String> partition1 = generateRandomStringSet(1, 10);
        Set<String> partition2 = generateRandomStringSet(1, 10);
        TwoPartitions topology = new TwoPartitions(partition1, partition2);
        checkTwoPartitions(topology, partition1, partition2);
    }

    public void testRandomTwoPartitions() {
        TwoPartitions topology = TwoPartitions.random(random(), generateRandomStringSet(2, 20));
        Set<String> partition1 = topology.getNodesSideOne();
        Set<String> partition2 = topology.getNodesSideTwo();
        checkTwoPartitions(topology, partition1, partition2);
    }

    private void checkTwoPartitions(TwoPartitions topology, Set<String> partition1, Set<String> partition2) {
        for (int i = 0; i < 10; i++) {
            assertTrue(topology.disrupt(randomFrom(partition1), randomFrom(partition2)));
            assertTrue(topology.disrupt(randomFrom(partition2), randomFrom(partition1)));
            assertFalse(topology.disrupt(randomFrom(partition1), randomFrom(partition1)));
            assertFalse(topology.disrupt(randomFrom(partition2), randomFrom(partition2)));
            assertFalse(topology.disrupt(randomAlphaOfLength(10), randomFrom(partition1)));
            assertFalse(topology.disrupt(randomAlphaOfLength(10), randomFrom(partition2)));
            assertFalse(topology.disrupt(randomFrom(partition1), randomAlphaOfLength(10)));
            assertFalse(topology.disrupt(randomFrom(partition2), randomAlphaOfLength(10)));
        }
        assertTrue(topology.getMajoritySide().size() >= topology.getMinoritySide().size());
    }

    public void testIsolateAll() {
        Set<String> nodes = generateRandomStringSet(1, 10);
        NetworkDisruption.DisruptedLinks topology = new NetworkDisruption.IsolateAllNodes(nodes);
        for (int i = 0; i < 10; i++) {
            final String node1 = randomFrom(nodes);
            final String node2 = randomFrom(nodes);
            if (node1.equals(node2)) {
                continue;
            }
            assertTrue(topology.nodes().contains(node1));
            assertTrue(topology.nodes().contains(node2));
            assertTrue(topology.disrupt(node1, node2));
        }
    }

    public void testBridge() {
        Set<String> partition1 = generateRandomStringSet(1, 10);
        Set<String> partition2 = generateRandomStringSet(1, 10);
        String bridgeNode = randomAlphaOfLength(10);
        Bridge topology = new Bridge(bridgeNode, partition1, partition2);
        checkBridge(topology, bridgeNode, partition1, partition2);
    }

    public void testRandomBridge() {
        Bridge topology = Bridge.random(random(), generateRandomStringSet(3, 20));
        String bridgeNode = topology.getBridgeNode();
        Set<String> partition1 = topology.getNodesSideOne();
        Set<String> partition2 = topology.getNodesSideTwo();
        checkBridge(topology, bridgeNode, partition1, partition2);
    }

    private void checkBridge(Bridge topology, String bridgeNode, Set<String> partition1, Set<String> partition2) {
        for (int i = 0; i < 10; i++) {
            assertTrue(topology.disrupt(randomFrom(partition1), randomFrom(partition2)));
            assertTrue(topology.disrupt(randomFrom(partition2), randomFrom(partition1)));
            assertFalse(topology.disrupt(randomFrom(partition1), randomFrom(partition1)));
            assertFalse(topology.disrupt(randomFrom(partition1), bridgeNode));
            assertFalse(topology.disrupt(bridgeNode, randomFrom(partition1)));
            assertFalse(topology.disrupt(randomFrom(partition2), randomFrom(partition2)));
            assertFalse(topology.disrupt(randomFrom(partition2), bridgeNode));
            assertFalse(topology.disrupt(bridgeNode, randomFrom(partition2)));
            assertFalse(topology.disrupt(randomAlphaOfLength(10), randomFrom(partition1)));
            assertFalse(topology.disrupt(randomAlphaOfLength(10), randomFrom(partition2)));
            assertFalse(topology.disrupt(randomAlphaOfLength(10), bridgeNode));
            assertFalse(topology.disrupt(randomFrom(partition1), randomAlphaOfLength(10)));
            assertFalse(topology.disrupt(randomFrom(partition2), randomAlphaOfLength(10)));
            assertFalse(topology.disrupt(bridgeNode, randomAlphaOfLength(10)));
        }
    }

    private Set<String> generateRandomStringSet(int minSize, int maxSize) {
        assert maxSize >= minSize;
        Set<String> result = new HashSet<>();
        for (int i = 0; i < minSize + randomInt(maxSize - minSize); i++) {
            result.add(randomAlphaOfLength(10));
        }
        return result;
    }
}
