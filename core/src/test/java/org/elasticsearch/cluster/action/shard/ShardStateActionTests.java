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

package org.elasticsearch.cluster.action.shard;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithStartedPrimary;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ShardStateActionTests extends ESTestCase {
    private static ThreadPool THREAD_POOL;

    private TestShardStateAction shardStateAction;
    private CapturingTransport transport;
    private TransportService transportService;
    private TestClusterService clusterService;

    private static class TestShardStateAction extends ShardStateAction {
        public TestShardStateAction(Settings settings, ClusterService clusterService, TransportService transportService, AllocationService allocationService, RoutingService routingService) {
            super(settings, clusterService, transportService, allocationService, routingService);
        }

        private Runnable onBeforeWaitForNewMasterAndRetry;

        public void setOnBeforeWaitForNewMasterAndRetry(Runnable onBeforeWaitForNewMasterAndRetry) {
            this.onBeforeWaitForNewMasterAndRetry = onBeforeWaitForNewMasterAndRetry;
        }

        private Runnable onAfterWaitForNewMasterAndRetry;

        public void setOnAfterWaitForNewMasterAndRetry(Runnable onAfterWaitForNewMasterAndRetry) {
            this.onAfterWaitForNewMasterAndRetry = onAfterWaitForNewMasterAndRetry;
        }

        @Override
        protected void waitForNewMasterAndRetry(String actionName, ClusterStateObserver observer, ShardRoutingEntry shardRoutingEntry, Listener listener) {
            onBeforeWaitForNewMasterAndRetry.run();
            super.waitForNewMasterAndRetry(actionName, observer, shardRoutingEntry, listener);
            onAfterWaitForNewMasterAndRetry.run();
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new ThreadPool("ShardStateActionTest");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.transport = new CapturingTransport();
        clusterService = new TestClusterService(THREAD_POOL);
        transportService = new TransportService(transport, THREAD_POOL);
        transportService.start();
        shardStateAction = new TestShardStateAction(Settings.EMPTY, clusterService, transportService, null, null);
        shardStateAction.setOnBeforeWaitForNewMasterAndRetry(() -> {});
        shardStateAction.setOnAfterWaitForNewMasterAndRetry(() -> {});
    }

    @Override
    @After
    public void tearDown() throws Exception {
        transportService.stop();
        super.tearDown();
    }

    @AfterClass
    public static void stopThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        THREAD_POOL = null;
    }

    public void testSuccess() throws InterruptedException {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        AtomicBoolean success = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(1);

        ShardRouting shardRouting = getRandomShardRouting(index);
        shardStateAction.shardFailed(shardRouting, indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onSuccess() {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                success.set(false);
                latch.countDown();
                assert false;
            }
        });

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);
        // the request is a shard failed request
        assertThat(capturedRequests[0].request, is(instanceOf(ShardStateAction.ShardRoutingEntry.class)));
        ShardStateAction.ShardRoutingEntry shardRoutingEntry = (ShardStateAction.ShardRoutingEntry)capturedRequests[0].request;
        // for the right shard
        assertEquals(shardRouting, shardRoutingEntry.getShardRouting());
        // sent to the master
        assertEquals(clusterService.state().nodes().masterNode().getId(), capturedRequests[0].node.getId());

        transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);

        latch.await();
        assertTrue(success.get());
    }

    public void testNoMaster() throws InterruptedException {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        DiscoveryNodes.Builder noMasterBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
        noMasterBuilder.masterNodeId(null);
        clusterService.setState(ClusterState.builder(clusterService.state()).nodes(noMasterBuilder));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger retries = new AtomicInteger();
        AtomicBoolean success = new AtomicBoolean();

        setUpMasterRetryVerification(1, retries, latch, requestId -> {});

        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onSuccess() {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                success.set(false);
                latch.countDown();
                assert false;
            }
        });

        latch.await();

        assertThat(retries.get(), equalTo(1));
        assertTrue(success.get());
    }

    public void testMasterChannelException() throws InterruptedException {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger retries = new AtomicInteger();
        AtomicBoolean success = new AtomicBoolean();
        AtomicReference<Throwable> throwable = new AtomicReference<>();

        LongConsumer retryLoop = requestId -> {
            if (randomBoolean()) {
                transport.handleRemoteError(
                    requestId,
                    randomFrom(new NotMasterException("simulated"), new Discovery.FailedToCommitClusterStateException("simulated")));
            } else {
                if (randomBoolean()) {
                    transport.handleLocalError(requestId, new NodeNotConnectedException(null, "simulated"));
                } else {
                    transport.handleError(requestId, new NodeDisconnectedException(null, ShardStateAction.SHARD_FAILED_ACTION_NAME));
                }
            }
        };

        final int numberOfRetries = randomIntBetween(1, 256);
        setUpMasterRetryVerification(numberOfRetries, retries, latch, retryLoop);

        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onSuccess() {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                success.set(false);
                throwable.set(t);
                latch.countDown();
                assert false;
            }
        });

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        assertFalse(success.get());
        assertThat(retries.get(), equalTo(0));
        retryLoop.accept(capturedRequests[0].requestId);

        latch.await();
        assertNull(throwable.get());
        assertThat(retries.get(), equalTo(numberOfRetries));
        assertTrue(success.get());
    }

    public void testUnhandledFailure() {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        AtomicBoolean failure = new AtomicBoolean();

        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onSuccess() {
                failure.set(false);
                assert false;
            }

            @Override
            public void onFailure(Throwable t) {
                failure.set(true);
            }
        });

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        assertFalse(failure.get());
        transport.handleRemoteError(capturedRequests[0].requestId, new TransportException("simulated"));

        assertTrue(failure.get());
    }

    public void testShardNotFound() throws InterruptedException {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        AtomicBoolean success = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(1);

        ShardRouting failedShard = getRandomShardRouting(index);
        RoutingTable routingTable = RoutingTable.builder(clusterService.state().getRoutingTable()).remove(index).build();
        clusterService.setState(ClusterState.builder(clusterService.state()).routingTable(routingTable));
        shardStateAction.shardFailed(failedShard, indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onSuccess() {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                success.set(false);
                latch.countDown();
                assert false;
            }
        });

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);

        latch.await();
        assertTrue(success.get());
    }

    private ShardRouting getRandomShardRouting(String index) {
        IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(index);
        ShardsIterator shardsIterator = indexRoutingTable.randomAllActiveShardsIt();
        ShardRouting shardRouting = shardsIterator.nextOrNull();
        assert shardRouting != null;
        return shardRouting;
    }

    private void setUpMasterRetryVerification(int numberOfRetries, AtomicInteger retries, CountDownLatch latch, LongConsumer retryLoop) {
        shardStateAction.setOnBeforeWaitForNewMasterAndRetry(() -> {
            DiscoveryNodes.Builder masterBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
            masterBuilder.masterNodeId(clusterService.state().nodes().masterNodes().iterator().next().value.id());
            clusterService.setState(ClusterState.builder(clusterService.state()).nodes(masterBuilder));
        });

        shardStateAction.setOnAfterWaitForNewMasterAndRetry(() -> verifyRetry(numberOfRetries, retries, latch, retryLoop));
    }

    private void verifyRetry(int numberOfRetries, AtomicInteger retries, CountDownLatch latch, LongConsumer retryLoop) {
        // assert a retry request was sent
        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        if (capturedRequests.length == 1) {
            retries.incrementAndGet();
            if (retries.get() == numberOfRetries) {
                // finish the request
                transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);
            } else {
                retryLoop.accept(capturedRequests[0].requestId);
            }
        } else {
            // there failed to be a retry request
            // release the driver thread to fail the test
            latch.countDown();
        }
    }

    private Throwable getSimulatedFailure() {
        return new CorruptIndexException("simulated", (String) null);
    }
}
