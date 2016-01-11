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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithStartedPrimary;
import static org.hamcrest.CoreMatchers.equalTo;

public class ShardStateActionTests extends ESTestCase {
    private static ThreadPool THREAD_POOL;

    private AtomicBoolean timeout;
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
        protected void waitForNewMasterAndRetry(ClusterStateObserver observer, ShardRoutingEntry shardRoutingEntry, Listener listener) {
            onBeforeWaitForNewMasterAndRetry.run();
            super.waitForNewMasterAndRetry(observer, shardRoutingEntry, listener);
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
        this.timeout = new AtomicBoolean();
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

    public void testNoMaster() throws InterruptedException {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        DiscoveryNodes.Builder noMasterBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
        noMasterBuilder.masterNodeId(null);
        clusterService.setState(ClusterState.builder(clusterService.state()).nodes(noMasterBuilder));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean noMaster = new AtomicBoolean();
        AtomicBoolean retried = new AtomicBoolean();
        AtomicBoolean success = new AtomicBoolean();

        setUpMasterRetryVerification(noMaster, retried, latch);

        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onSuccess() {
                success.set(true);
                latch.countDown();
            }
        });

        latch.await();

        assertTrue(noMaster.get());
        assertTrue(retried.get());
        assertTrue(success.get());
    }

    public void testMasterChannelException() throws InterruptedException {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean noMaster = new AtomicBoolean();
        AtomicBoolean retried = new AtomicBoolean();
        AtomicBoolean success = new AtomicBoolean();
        AtomicReference<Exception> exception = new AtomicReference<>();

        setUpMasterRetryVerification(noMaster, retried, latch);

        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onSuccess() {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onShardFailedFailure(Exception e) {
                success.set(false);
                exception.set(e);
                latch.countDown();
            }
        });

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.capturedRequests();
        transport.clear();
        assertThat(capturedRequests.length, equalTo(1));
        assertFalse(success.get());
        List<Exception> possibleExceptions = new ArrayList<>();
        possibleExceptions.add(new NotMasterException("simulated"));
        possibleExceptions.add(new NodeDisconnectedException(clusterService.state().nodes().masterNode(), ShardStateAction.SHARD_FAILED_ACTION_NAME));
        possibleExceptions.add(new Discovery.FailedToCommitClusterStateException("simulated"));
        transport.handleResponse(capturedRequests[0].requestId, randomFrom(possibleExceptions));

        latch.await();
        assertNull(exception.get());
        assertTrue(success.get());
    }

    public void testUnhandledFailure() {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        AtomicBoolean failure = new AtomicBoolean();

        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onShardFailedFailure(Exception e) {
                failure.set(true);
            }
        });

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        assertFalse(failure.get());
        transport.handleResponse(capturedRequests[0].requestId, new TransportException("simulated"));

        assertTrue(failure.get());
    }

    private ShardRouting getRandomShardRouting(String index) {
        IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(index);
        ShardsIterator shardsIterator = indexRoutingTable.randomAllActiveShardsIt();
        ShardRouting shardRouting = shardsIterator.nextOrNull();
        assert shardRouting != null;
        return shardRouting;
    }

    private void setUpMasterRetryVerification(AtomicBoolean noMaster, AtomicBoolean retried, CountDownLatch latch) {
        shardStateAction.setOnBeforeWaitForNewMasterAndRetry(() -> {
            DiscoveryNodes.Builder masterBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
            masterBuilder.masterNodeId(clusterService.state().nodes().masterNodes().iterator().next().value.id());
            clusterService.setState(ClusterState.builder(clusterService.state()).nodes(masterBuilder));
        });

        shardStateAction.setOnAfterWaitForNewMasterAndRetry(() -> verifyRetry(noMaster, retried, latch));
    }

    private void verifyRetry(AtomicBoolean invoked, AtomicBoolean retried, CountDownLatch latch) {
        invoked.set(true);

        // assert a retry request was sent
        final CapturingTransport.CapturedRequest[] capturedRequests = transport.capturedRequests();
        transport.clear();
        retried.set(capturedRequests.length == 1);
        if (retried.get()) {
            // finish the request
            transport.handleResponse(capturedRequests[0].requestId, TransportResponse.Empty.INSTANCE);
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
