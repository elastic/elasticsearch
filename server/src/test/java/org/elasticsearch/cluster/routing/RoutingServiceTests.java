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
package org.elasticsearch.cluster.routing;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;

public class RoutingServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void createTestHarness() {
        threadPool = new TestThreadPool("test");
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void shutdownThreadPool() {
        clusterService.stop();
        threadPool.shutdown();
    }

    public void testRejectionUnlessStarted() {
        final RoutingService routingService = new RoutingService(clusterService, (s, r) -> s);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();

        if (randomBoolean()) {
            routingService.start();
            routingService.stop();
        } else if (randomBoolean()) {
            routingService.close();
        }

        routingService.reroute("test", future);
        assertTrue(future.isDone());
        assertThat(expectThrows(IllegalStateException.class, future::actionGet).getMessage(),
            startsWith("rejecting delayed reroute [test] in state ["));
    }

    public void testReroutesWhenRequested() throws InterruptedException {
        final AtomicLong rerouteCount = new AtomicLong();
        final RoutingService routingService = new RoutingService(clusterService, (s, r) -> {
            rerouteCount.incrementAndGet();
            return s;
        });

        routingService.start();

        long rerouteCountBeforeReroute = 0L;
        final int iterations = between(1, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(iterations);
        for (int i = 0; i < iterations; i++) {
            rerouteCountBeforeReroute = Math.max(rerouteCountBeforeReroute, rerouteCount.get());
            routingService.reroute("iteration " + i, ActionListener.wrap(countDownLatch::countDown));
        }
        countDownLatch.await(10, TimeUnit.SECONDS);
        assertThat(rerouteCountBeforeReroute, lessThan(rerouteCount.get()));
    }

    public void testBatchesReroutesTogether() throws BrokenBarrierException, InterruptedException {
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        clusterService.submitStateUpdateTask("block master service", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                cyclicBarrier.await(); // notify test that we are blocked
                cyclicBarrier.await(); // wait to be unblocked by test
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError(source, e);
            }
        });

        cyclicBarrier.await(); // wait for master thread to be blocked

        final AtomicBoolean rerouteExecuted = new AtomicBoolean();
        final RoutingService routingService = new RoutingService(clusterService, (s, r) -> {
            assertTrue(rerouteExecuted.compareAndSet(false, true)); // only called once
            return s;
        });

        routingService.start();

        final int iterations = between(1, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(iterations);
        for (int i = 0; i < iterations; i++) {
            routingService.reroute("iteration " + i, ActionListener.wrap(countDownLatch::countDown));
        }

        cyclicBarrier.await(); // allow master thread to continue;
        countDownLatch.await(); // wait for reroute to complete
        assertTrue(rerouteExecuted.get()); // see above for assertion that it's only called once
    }

    public void testNotifiesOnFailure() throws InterruptedException {

        final RoutingService routingService = new RoutingService(clusterService, (s, r) -> {
            if (rarely()) {
                throw new ElasticsearchException("simulated");
            }
            return randomBoolean() ? s : ClusterState.builder(s).build();
        });
        routingService.start();

        final int iterations = between(1, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(iterations);
        for (int i = 0; i < iterations; i++) {
            routingService.reroute("iteration " + i, ActionListener.wrap(countDownLatch::countDown));
            if (rarely()) {
                clusterService.getMasterService().setClusterStatePublisher(
                    randomBoolean()
                        ? ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService())
                        : (event, publishListener, ackListener)
                        -> publishListener.onFailure(new FailedToCommitClusterStateException("simulated")));
            }

            if (rarely()) {
                clusterService.getClusterApplierService().onNewClusterState("simulated", () -> {
                    ClusterState state = clusterService.state();
                    return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes())
                        .masterNodeId(randomBoolean() ? null : state.nodes().getLocalNodeId())).build();
                }, (source, e) -> { });
            }
        }

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS)); // i.e. it doesn't leak any listeners
    }
}
