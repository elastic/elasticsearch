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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.*;

public class BlockingClusterStatePublishResponseHandlerTests extends ElasticsearchTestCase {

    static private class PublishResponder extends AbstractRunnable {

        final boolean fail;
        final DiscoveryNode node;
        final CyclicBarrier barrier;
        final ESLogger logger;
        final BlockingClusterStatePublishResponseHandler handler;

        public PublishResponder(boolean fail, DiscoveryNode node, CyclicBarrier barrier, ESLogger logger, BlockingClusterStatePublishResponseHandler handler) {
            this.fail = fail;

            this.node = node;
            this.barrier = barrier;
            this.logger = logger;
            this.handler = handler;
        }

        @Override
        public void onFailure(Throwable t) {
            logger.error("unexpected error", t);
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
            DiscoveryNode node = new DiscoveryNode("node_" + i, DummyTransportAddress.INSTANCE, Version.CURRENT);
            allNodes[i] = node;
        }

        BlockingClusterStatePublishResponseHandler handler = new BlockingClusterStatePublishResponseHandler(new HashSet<>(Arrays.asList(allNodes)));

        int firstRound = randomIntBetween(5, nodeCount - 1);
        Thread[] threads = new Thread[firstRound];
        CyclicBarrier barrier = new CyclicBarrier(firstRound);
        Set<DiscoveryNode> completedNodes = new HashSet<>();
        for (int i = 0; i < threads.length; i++) {
            completedNodes.add(allNodes[i]);
            threads[i] = new Thread(new PublishResponder(randomBoolean(), allNodes[i], barrier, logger, handler));
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
            threads[i] = new Thread(new PublishResponder(randomBoolean(), allNodes[firstRound + i], barrier, logger, handler));
            threads[i].start();
        }
        // wait on the threads to finish
        for (Thread t : threads) {
            t.join();
        }
        assertTrue("expected handler not to timeout as all nodes responded", handler.awaitAllNodes(new TimeValue(10)));
        assertThat(handler.pendingNodes(), arrayWithSize(0));

    }
}
