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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithStartedPrimary;
import static org.hamcrest.CoreMatchers.equalTo;

public class ShardStateActionTests extends ESTestCase {
    private static ThreadPool THREAD_POOL;

    private ShardStateAction shardStateAction;
    private CapturingTransport transport;
    private TransportService transportService;
    private TestClusterService clusterService;

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
        shardStateAction = new ShardStateAction(Settings.EMPTY, clusterService, transportService, null, null);
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

    public void testNoMaster() {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(clusterService.state().nodes());
        builder.masterNodeId(null);
        clusterService.setState(ClusterState.builder(clusterService.state()).nodes(builder));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        AtomicBoolean noMaster = new AtomicBoolean();
        assert !noMaster.get();

        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onShardFailedNoMaster() {
                noMaster.set(true);
            }

            @Override
            public void onShardFailedFailure(DiscoveryNode master, TransportException e) {

            }
        });

        assertTrue(noMaster.get());
    }

    public void testFailure() {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        AtomicBoolean failure = new AtomicBoolean();
        assert !failure.get();

        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), new ShardStateAction.Listener() {
            @Override
            public void onShardFailedNoMaster() {

            }

            @Override
            public void onShardFailedFailure(DiscoveryNode master, TransportException e) {
                failure.set(true);
            }
        });

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.capturedRequests();
        transport.clear();
        assertThat(capturedRequests.length, equalTo(1));
        assert !failure.get();
        transport.handleResponse(capturedRequests[0].requestId, new TransportException("simulated"));

        assertTrue(failure.get());
    }

    public void testTimeout() throws InterruptedException {
        final String index = "test";

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        String indexUUID = clusterService.state().metaData().index(index).getIndexUUID();

        AtomicBoolean progress = new AtomicBoolean();
        AtomicBoolean timedOut = new AtomicBoolean();

        TimeValue timeout = new TimeValue(1, TimeUnit.MILLISECONDS);
        CountDownLatch latch = new CountDownLatch(1);
        shardStateAction.shardFailed(getRandomShardRouting(index), indexUUID, "test", getSimulatedFailure(), timeout, new ShardStateAction.Listener() {
            @Override
            public void onShardFailedFailure(DiscoveryNode master, TransportException e) {
                if (e instanceof ReceiveTimeoutTransportException) {
                    assertFalse(progress.get());
                    timedOut.set(true);
                }
                latch.countDown();
            }
        });

        latch.await();
        progress.set(true);
        assertTrue(timedOut.get());

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.capturedRequests();
        transport.clear();
        assertThat(capturedRequests.length, equalTo(1));
    }

    private ShardRouting getRandomShardRouting(String index) {
        IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(index);
        ShardsIterator shardsIterator = indexRoutingTable.randomAllActiveShardsIt();
        ShardRouting shardRouting = shardsIterator.nextOrNull();
        assert shardRouting != null;
        return shardRouting;
    }

    private Throwable getSimulatedFailure() {
        return new CorruptIndexException("simulated", (String) null);
    }
}
