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
package org.elasticsearch.discovery;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class BlockingClusterStatePublishResponseHandlerTests extends ESTestCase {

    private static class PublishResponder extends AbstractRunnable {

        final boolean fail;
        final DiscoveryNode node;
        final CyclicBarrier barrier;
        final Logger logger;
        final BlockingClusterStatePublishResponseHandler handler;

        PublishResponder(boolean fail, DiscoveryNode node, CyclicBarrier barrier, Logger logger,
                         BlockingClusterStatePublishResponseHandler handler) {
            this.fail = fail;

            this.node = node;
            this.barrier = barrier;
            this.logger = logger;
            this.handler = handler;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("unexpected error", e);
        }

        @Override
        protected void doRun() throws Exception {
            barrier.await();
            if (fail) {
                handler.onFailure(node, new Exception("bla"));
            } else {
                handler.onResponse(node);
            }
        }
    }

    public void testConcurrentAccess() throws InterruptedException {
        int nodeCount = scaledRandomIntBetween(10, 20);
        DiscoveryNode[] allNodes = new DiscoveryNode[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            DiscoveryNode node = new DiscoveryNode("node_" + i, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
            allNodes[i] = node;
        }

        BlockingClusterStatePublishResponseHandler handler =
            new BlockingClusterStatePublishResponseHandler(new HashSet<>(Arrays.asList(allNodes)));

        int firstRound = randomIntBetween(5, nodeCount - 1);
        Thread[] threads = new Thread[firstRound];
        CyclicBarrier barrier = new CyclicBarrier(firstRound);
        Set<DiscoveryNode> expectedFailures = new HashSet<>();
        Set<DiscoveryNode> completedNodes = new HashSet<>();
        for (int i = 0; i < threads.length; i++) {
            final DiscoveryNode node = allNodes[i];
            completedNodes.add(node);
            final boolean fail = randomBoolean();
            if (fail) {
                expectedFailures.add(node);
            }
            threads[i] = new Thread(new PublishResponder(fail, node, barrier, logger, handler));
            threads[i].start();
        }
        // wait on the threads to finish
        for (Thread t : threads) {
            t.join();
        }
        // verify that the publisher times out
        assertFalse("expected handler wait to timeout as not all nodes responded", handler.awaitAllNodes(new TimeValue(10)));
        Set<DiscoveryNode> pendingNodes = new HashSet<>(Arrays.asList(handler.pendingNodes()));
        assertThat(completedNodes, not(contains(pendingNodes.toArray(new DiscoveryNode[0]))));
        assertThat(completedNodes.size() + pendingNodes.size(), equalTo(allNodes.length));
        int secondRound = allNodes.length - firstRound;
        threads = new Thread[secondRound];
        barrier = new CyclicBarrier(secondRound);

        for (int i = 0; i < threads.length; i++) {
            final DiscoveryNode node = allNodes[firstRound + i];
            final boolean fail = randomBoolean();
            if (fail) {
                expectedFailures.add(node);
            }
            threads[i] = new Thread(new PublishResponder(fail, node, barrier, logger, handler));
            threads[i].start();
        }
        // wait on the threads to finish
        for (Thread t : threads) {
            t.join();
        }
        assertTrue("expected handler not to timeout as all nodes responded", handler.awaitAllNodes(new TimeValue(10)));
        assertThat(handler.pendingNodes(), arrayWithSize(0));
        assertThat(handler.getFailedNodes(), equalTo(expectedFailures));
    }
}
