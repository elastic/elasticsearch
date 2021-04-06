/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ResponseCollectorServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private ResponseCollectorService collector;
    private ThreadPool threadpool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadpool = new TestThreadPool("response_collector_tests");
        clusterService = new ClusterService(Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadpool);
        collector = new ResponseCollectorService(clusterService);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdownNow();
    }

    public void testNodeStats() throws Exception {
        collector.addNodeStatistics("node1", 1, 100, 10);
        Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertThat(nodeStats.get("node1").queueSize, equalTo(1));
        assertThat(nodeStats.get("node1").responseTime, equalTo(100.0));
        assertThat(nodeStats.get("node1").serviceTime, equalTo(10.0));
    }

    /*
     * Test that concurrently adding values and removing nodes does not cause exceptions
     */
    public void testConcurrentAddingAndRemoving() throws Exception {
        String[] nodes = new String[] {"a", "b", "c", "d"};

        final CountDownLatch latch = new CountDownLatch(5);

        Runnable f = () -> {
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail("should not be interrupted");
            }
            for (int i = 0; i < randomIntBetween(100, 200); i++) {
                if (randomBoolean()) {
                    collector.removeNode(randomFrom(nodes));
                }
                collector.addNodeStatistics(randomFrom(nodes), randomIntBetween(1,100), randomIntBetween(1,100), randomIntBetween(1,100));
            }
        };

        Thread t1 = new Thread(f);
        Thread t2 = new Thread(f);
        Thread t3 = new Thread(f);
        Thread t4 = new Thread(f);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        latch.countDown();
        t1.join();
        t2.join();
        t3.join();
        t4.join();

        final Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        logger.info("--> got stats: {}", nodeStats);
        for (String nodeId : nodes) {
            if (nodeStats.containsKey(nodeId)) {
                assertThat(nodeStats.get(nodeId).queueSize, greaterThan(0));
                assertThat(nodeStats.get(nodeId).responseTime, greaterThan(0.0));
                assertThat(nodeStats.get(nodeId).serviceTime, greaterThan(0.0));
            }
        }
    }

    public void testNodeRemoval() throws Exception {
        collector.addNodeStatistics("node1", randomIntBetween(1,100), randomIntBetween(1,100), randomIntBetween(1,100));
        collector.addNodeStatistics("node2", randomIntBetween(1,100), randomIntBetween(1,100), randomIntBetween(1,100));

        ClusterState previousState = ClusterState.builder(new ClusterName("cluster")).nodes(DiscoveryNodes.builder()
                .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node1"))
                .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9201), "node2")))
                .build();
        ClusterState newState = ClusterState.builder(previousState).nodes(DiscoveryNodes.builder(previousState.nodes())
                .remove("node2")).build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, previousState);

        collector.clusterChanged(event);
        final Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertFalse(nodeStats.containsKey("node2"));
    }
}
