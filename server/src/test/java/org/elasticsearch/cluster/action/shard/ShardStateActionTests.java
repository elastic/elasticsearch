/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.action.shard;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.ShardLongFieldRangeWireTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.test.TransportVersionUtils.randomCompatibleVersion;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ShardStateActionTests extends ESTestCase {
    private static ThreadPool THREAD_POOL;

    private TestShardStateAction shardStateAction;
    private CapturingTransport transport;
    private TransportService transportService;
    private ClusterService clusterService;

    private static class TestShardStateAction extends ShardStateAction {
        TestShardStateAction(
            ClusterService clusterService,
            TransportService transportService,
            AllocationService allocationService,
            RerouteService rerouteService
        ) {
            super(clusterService, transportService, allocationService, rerouteService, THREAD_POOL);
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
        protected void waitForNewMasterAndRetry(
            String actionName,
            ClusterStateObserver observer,
            TransportRequest request,
            ActionListener<Void> listener
        ) {
            onBeforeWaitForNewMasterAndRetry.run();
            super.waitForNewMasterAndRetry(actionName, observer, request, listener);
            onAfterWaitForNewMasterAndRetry.run();
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool("ShardStateActionTest");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            THREAD_POOL,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new TestShardStateAction(clusterService, transportService, null, null);
        shardStateAction.setOnBeforeWaitForNewMasterAndRetry(() -> {});
        shardStateAction.setOnAfterWaitForNewMasterAndRetry(() -> {});
    }

    @Override
    @After
    public void tearDown() throws Exception {
        clusterService.close();
        transportService.close();
        super.tearDown();
        assertThat(shardStateAction.remoteShardRequestsInFlight(), equalTo(0));
    }

    @AfterClass
    public static void stopThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        THREAD_POOL = null;
    }

    public void testSuccess() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        final TestListener listener = new TestListener();
        ShardRouting shardRouting = getRandomShardRouting(index);
        shardStateAction.localShardFailed(shardRouting, "test", getSimulatedFailure(), listener);

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);
        // the request is a shard failed request
        assertThat(capturedRequests[0].request(), is(instanceOf(ShardStateAction.FailedShardEntry.class)));
        ShardStateAction.FailedShardEntry shardEntry = (ShardStateAction.FailedShardEntry) capturedRequests[0].request();
        // for the right shard
        assertEquals(shardEntry.getShardId(), shardRouting.shardId());
        assertEquals(shardEntry.getAllocationId(), shardRouting.allocationId().getId());
        // sent to the master
        assertEquals(clusterService.state().nodes().getMasterNode().getId(), capturedRequests[0].node().getId());

        transport.handleResponse(capturedRequests[0].requestId(), TransportResponse.Empty.INSTANCE);

        listener.await();
        assertNull(listener.failure.get());
    }

    public void testNoMaster() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        DiscoveryNodes.Builder noMasterBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
        noMasterBuilder.masterNodeId(null);
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(noMasterBuilder));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger retries = new AtomicInteger();
        AtomicBoolean success = new AtomicBoolean();

        setUpMasterRetryVerification(1, retries, latch, requestId -> {});

        ShardRouting failedShard = getRandomShardRouting(index);
        shardStateAction.localShardFailed(failedShard, "test", getSimulatedFailure(), new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
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

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger retries = new AtomicInteger();
        AtomicBoolean success = new AtomicBoolean();
        AtomicReference<Throwable> throwable = new AtomicReference<>();

        LongConsumer retryLoop = requestId -> {
            if (randomBoolean()) {
                transport.handleRemoteError(
                    requestId,
                    randomFrom(new NotMasterException("simulated"), new FailedToCommitClusterStateException("simulated"))
                );
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

        ShardRouting failedShard = getRandomShardRouting(index);
        shardStateAction.localShardFailed(failedShard, "test", getSimulatedFailure(), new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                success.set(false);
                throwable.set(e);
                latch.countDown();
                assert false;
            }
        });

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        assertFalse(success.get());
        assertThat(retries.get(), equalTo(0));
        retryLoop.accept(capturedRequests[0].requestId());

        latch.await();
        assertNull(throwable.get());
        assertThat(retries.get(), equalTo(numberOfRetries));
        assertTrue(success.get());
    }

    public void testUnhandledFailure() {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        final TestListener listener = new TestListener();
        ShardRouting failedShard = getRandomShardRouting(index);
        shardStateAction.localShardFailed(failedShard, "test", getSimulatedFailure(), listener);

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        transport.handleRemoteError(capturedRequests[0].requestId(), new TransportException("simulated"));
        assertNotNull(listener.failure.get());
    }

    public void testShardNotFound() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        final TestListener listener = new TestListener();
        ShardRouting failedShard = getRandomShardRouting(index);
        RoutingTable routingTable = RoutingTable.builder(clusterService.state().getRoutingTable()).remove(index).build();
        setState(clusterService, ClusterState.builder(clusterService.state()).routingTable(routingTable));
        shardStateAction.localShardFailed(failedShard, "test", getSimulatedFailure(), listener);

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        transport.handleResponse(capturedRequests[0].requestId(), TransportResponse.Empty.INSTANCE);

        listener.await();
        assertNull(listener.failure.get());
    }

    public void testNoLongerPrimaryShardException() throws InterruptedException {
        final String index = "test";

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        ShardRouting failedShard = getRandomShardRouting(index);

        final TestListener listener = new TestListener();
        long primaryTerm = clusterService.state().metadata().index(index).primaryTerm(failedShard.id());
        assertThat(primaryTerm, greaterThanOrEqualTo(1L));
        shardStateAction.remoteShardFailed(
            failedShard.shardId(),
            failedShard.allocationId().getId(),
            primaryTerm + 1,
            randomBoolean(),
            "test",
            getSimulatedFailure(),
            listener
        );

        ShardStateAction.NoLongerPrimaryShardException catastrophicError = new ShardStateAction.NoLongerPrimaryShardException(
            failedShard.shardId(),
            "dummy failure"
        );
        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        transport.handleRemoteError(capturedRequests[0].requestId(), catastrophicError);

        listener.await();

        final Exception failure = listener.failure.get();
        assertNotNull(failure);
        assertThat(failure, instanceOf(ShardStateAction.NoLongerPrimaryShardException.class));
        assertThat(failure.getMessage(), equalTo(catastrophicError.getMessage()));
    }

    public void testCacheRemoteShardFailed() throws Exception {
        final String index = "test";
        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));
        ShardRouting failedShard = getRandomShardRouting(index);
        boolean markAsStale = randomBoolean();
        int numListeners = between(1, 100);
        CountDownLatch latch = new CountDownLatch(numListeners);
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        for (int i = 0; i < numListeners; i++) {
            shardStateAction.remoteShardFailed(
                failedShard.shardId(),
                failedShard.allocationId().getId(),
                primaryTerm,
                markAsStale,
                "test",
                getSimulatedFailure(),
                new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void aVoid) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        latch.countDown();
                    }
                }
            );
        }
        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests, arrayWithSize(1));
        transport.handleResponse(capturedRequests[0].requestId(), TransportResponse.Empty.INSTANCE);
        latch.await();
        assertThat(transport.capturedRequests(), arrayWithSize(0));
    }

    public void testDeduplicateRemoteShardStarted() throws InterruptedException {
        final String index = "test";
        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));
        ShardRouting startedShard = getRandomShardRouting(index);
        int numListeners = between(1, 100);
        CountDownLatch latch = new CountDownLatch(numListeners);
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        int expectedRequests = 1;
        for (int i = 0; i < numListeners; i++) {
            if (rarely() && i > 0) {
                expectedRequests++;
                shardStateAction.clearRemoteShardRequestDeduplicator();
            }
            shardStateAction.shardStarted(startedShard, primaryTerm, "started", ShardLongFieldRange.EMPTY, new ActionListener<>() {
                @Override
                public void onResponse(Void aVoid) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            });
        }
        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests, arrayWithSize(expectedRequests));
        for (int i = 0; i < expectedRequests; i++) {
            transport.handleResponse(capturedRequests[i].requestId(), TransportResponse.Empty.INSTANCE);
        }
        latch.await();
        assertThat(transport.capturedRequests(), arrayWithSize(0));
    }

    public void testRemoteShardFailedConcurrently() throws Exception {
        final String index = "test";
        final AtomicBoolean shutdown = new AtomicBoolean(false);
        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));
        ShardRouting[] failedShards = new ShardRouting[between(1, 5)];
        for (int i = 0; i < failedShards.length; i++) {
            failedShards[i] = getRandomShardRouting(index);
        }
        Thread[] clientThreads = new Thread[between(1, 6)];
        int iterationsPerThread = scaledRandomIntBetween(50, 500);
        Phaser barrier = new Phaser(clientThreads.length + 2); // one for master thread, one for the main thread
        Thread masterThread = new Thread(() -> {
            barrier.arriveAndAwaitAdvance();
            while (shutdown.get() == false) {
                for (CapturingTransport.CapturedRequest request : transport.getCapturedRequestsAndClear()) {
                    if (randomBoolean()) {
                        transport.handleResponse(request.requestId(), TransportResponse.Empty.INSTANCE);
                    } else {
                        transport.handleRemoteError(request.requestId(), randomFrom(getSimulatedFailure()));
                    }
                }
            }
        });
        masterThread.start();

        AtomicInteger notifiedResponses = new AtomicInteger();
        for (int t = 0; t < clientThreads.length; t++) {
            clientThreads[t] = new Thread(() -> {
                barrier.arriveAndAwaitAdvance();
                for (int i = 0; i < iterationsPerThread; i++) {
                    ShardRouting failedShard = randomFrom(failedShards);
                    shardStateAction.remoteShardFailed(
                        failedShard.shardId(),
                        failedShard.allocationId().getId(),
                        randomLongBetween(1, Long.MAX_VALUE),
                        randomBoolean(),
                        "test",
                        getSimulatedFailure(),
                        new ActionListener<Void>() {
                            @Override
                            public void onResponse(Void aVoid) {
                                notifiedResponses.incrementAndGet();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                notifiedResponses.incrementAndGet();
                            }
                        }
                    );
                }
            });
            clientThreads[t].start();
        }
        barrier.arriveAndAwaitAdvance();
        for (Thread t : clientThreads) {
            t.join();
        }
        assertBusy(() -> assertThat(notifiedResponses.get(), equalTo(clientThreads.length * iterationsPerThread)));
        shutdown.set(true);
        masterThread.join();
    }

    public void testShardStarted() throws InterruptedException {
        final String index = "test";
        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(index, true, randomInt(5)));

        final ShardRouting shardRouting = getRandomShardRouting(index);
        final long primaryTerm = clusterService.state().metadata().index(shardRouting.index()).primaryTerm(shardRouting.id());
        final TestListener listener = new TestListener();
        shardStateAction.shardStarted(shardRouting, primaryTerm, "testShardStarted", ShardLongFieldRange.UNKNOWN, listener);

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests[0].request(), instanceOf(ShardStateAction.StartedShardEntry.class));

        ShardStateAction.StartedShardEntry entry = (ShardStateAction.StartedShardEntry) capturedRequests[0].request();
        assertThat(entry.shardId, equalTo(shardRouting.shardId()));
        assertThat(entry.allocationId, equalTo(shardRouting.allocationId().getId()));
        assertThat(entry.primaryTerm, equalTo(primaryTerm));
        assertThat(entry.timestampRange, sameInstance(ShardLongFieldRange.UNKNOWN));

        transport.handleResponse(capturedRequests[0].requestId(), TransportResponse.Empty.INSTANCE);
        listener.await();
        assertNull(listener.failure.get());
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
            masterBuilder.masterNodeId(clusterService.state().nodes().getMasterNodes().values().iterator().next().getId());
            setState(clusterService, ClusterState.builder(clusterService.state()).nodes(masterBuilder));
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
                transport.handleResponse(capturedRequests[0].requestId(), TransportResponse.Empty.INSTANCE);
            } else {
                retryLoop.accept(capturedRequests[0].requestId());
            }
        } else {
            // there failed to be a retry request
            // release the driver thread to fail the test
            latch.countDown();
        }
    }

    private Exception getSimulatedFailure() {
        return new CorruptIndexException("simulated", (String) null);
    }

    public void testFailedShardEntrySerialization() throws Exception {
        final ShardId shardId = new ShardId(randomRealisticUnicodeOfLengthBetween(10, 100), UUID.randomUUID().toString(), between(0, 1000));
        final String allocationId = randomRealisticUnicodeOfCodepointLengthBetween(10, 100);
        final long primaryTerm = randomIntBetween(0, 100);
        final String message = randomRealisticUnicodeOfCodepointLengthBetween(10, 100);
        final Exception failure = randomBoolean() ? null : getSimulatedFailure();
        final boolean markAsStale = randomBoolean();

        final TransportVersion version = randomFrom(randomCompatibleVersion(random()));
        final FailedShardEntry failedShardEntry = new FailedShardEntry(shardId, allocationId, primaryTerm, message, failure, markAsStale);
        try (StreamInput in = serialize(failedShardEntry, version).streamInput()) {
            in.setTransportVersion(version);
            final FailedShardEntry deserialized = new FailedShardEntry(in);
            assertThat(deserialized.getShardId(), equalTo(shardId));
            assertThat(deserialized.getAllocationId(), equalTo(allocationId));
            assertThat(deserialized.primaryTerm, equalTo(primaryTerm));
            assertThat(deserialized.message, equalTo(message));
            if (failure != null) {
                assertThat(deserialized.failure, notNullValue());
                assertThat(deserialized.failure.getClass(), equalTo(failure.getClass()));
                assertThat(deserialized.failure.getMessage(), equalTo(failure.getMessage()));
            } else {
                assertThat(deserialized.failure, nullValue());
            }
            assertThat(deserialized.markAsStale, equalTo(markAsStale));
            assertEquals(failedShardEntry, deserialized);
        }
    }

    public void testStartedShardEntrySerialization() throws Exception {
        final ShardId shardId = new ShardId(randomRealisticUnicodeOfLengthBetween(10, 100), UUID.randomUUID().toString(), between(0, 1000));
        final String allocationId = randomRealisticUnicodeOfCodepointLengthBetween(10, 100);
        final long primaryTerm = randomIntBetween(0, 100);
        final String message = randomRealisticUnicodeOfCodepointLengthBetween(10, 100);

        final TransportVersion version = randomFrom(randomCompatibleVersion(random()));
        final ShardLongFieldRange timestampRange = ShardLongFieldRangeWireTests.randomRange();
        final StartedShardEntry startedShardEntry = new StartedShardEntry(shardId, allocationId, primaryTerm, message, timestampRange);
        try (StreamInput in = serialize(startedShardEntry, version).streamInput()) {
            in.setTransportVersion(version);
            final StartedShardEntry deserialized = new StartedShardEntry(in);
            assertThat(deserialized.shardId, equalTo(shardId));
            assertThat(deserialized.allocationId, equalTo(allocationId));
            assertThat(deserialized.primaryTerm, equalTo(primaryTerm));
            assertThat(deserialized.message, equalTo(message));
            assertThat(deserialized.timestampRange, equalTo(timestampRange));
        }
    }

    BytesReference serialize(Writeable writeable, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            writeable.writeTo(out);
            return out.bytes();
        }
    }

    private static class TestListener implements ActionListener<Void> {

        private final SetOnce<Exception> failure = new SetOnce<>();
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onResponse(Void aVoid) {
            try {
                failure.set(null);
            } finally {
                latch.countDown();
            }
        }

        @Override
        public void onFailure(final Exception e) {
            try {
                failure.set(e);
            } finally {
                latch.countDown();
            }
        }

        void await() throws InterruptedException {
            latch.await();
        }
    }
}
