/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SplitSourceServiceTests extends ESTestCase {

    AtomicLong nowInMillis = new AtomicLong();

    /// [SplitSourceService#waitForHandoffSuccessOrFailure] must not look up the index itself. The observer it starts releases the
    /// permits, so a lookup that throws on a deleted index strands them.
    public void testHandoffReleasesPermitsWhenIndexIsGone() throws Exception {
        final var permitsClosed = new AtomicInteger();
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(
                new DeterministicTaskQueue().getThreadPool(),
                clusterSettings()
            )
        ) {
            final var splitSourceService = new SplitSourceService(null, clusterService, null, null, null, null, null, Settings.EMPTY);
            final var goneIndex = new Index("gone", "gone-uuid");
            final var handoff = new PlainActionFuture<ActionResponse>();

            splitSourceService.waitForHandoffSuccessOrFailure(
                new ShardId(goneIndex, 1),
                new ShardId(goneIndex, 0),
                1L,
                1L,
                new AtomicBoolean(true),
                permitsClosed::incrementAndGet,
                handoff
            );

            expectThrows(IndexNotFoundException.class, handoff::actionGet);
        }
        assertEquals(1, permitsClosed.get());
    }

    // test that a RefCountingAcquirer will only acquire the resource once if multiple acquirers arrive while the resource is held
    public void testRefCountedAcquirerAcquiresAndReleasesOnce() throws Exception {
        final var numAcquirers = randomIntBetween(1, 10);
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger released = new AtomicInteger();

        var acquirersArrived = new CountDownLatch(numAcquirers);
        // waits for all acquirers to enter RefCountedAcquirer.acquire, then returns the RefCountedAcquirer
        // a releasable that counts the number of times it has ever been fired
        Consumer<ActionListener<Releasable>> acquirer = listener -> new Thread(() -> {
            acquired.incrementAndGet();
            logger.info("acquiring {}", acquired.get());
            try {
                acquirersArrived.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            listener.onResponse(() -> {
                released.incrementAndGet();
                logger.info("releasing {}", released.get());
            });
        }).start();
        var refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(
            acquirer,
            nowInMillis::incrementAndGet,
            duration -> assertEquals(1, duration)
        );

        var threads = new Thread[numAcquirers];
        // creates numAcquirers threads that will enter acquire and then complete
        var acquirersAcquired = new CountDownLatch(numAcquirers);
        for (int i = 0; i < numAcquirers; i++) {
            final int sleepMillis = randomIntBetween(1, 50);
            threads[i] = new Thread(() -> {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ignored) {}
                refCountedAcquirer.acquire(runAndRelease(acquirersAcquired::countDown));
                acquirersArrived.countDown();
            });
            threads[i].start();
        }

        for (int i = 0; i < numAcquirers; i++) {
            threads[i].join();
        }
        acquirersAcquired.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);

        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(1));
            assertThat(released.get(), equalTo(1));
        });
    }

    // test that a RefCountingAcquirer will acquire the provided resource again after it has released it
    public void testRefCountedAcquirerCanReacquire() throws Exception {
        // counts the number of times it's been acquired and released
        final var acquired = new AtomicInteger();
        final var released = new AtomicInteger();
        var refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(listener -> {
            acquired.incrementAndGet();
            listener.onResponse(released::incrementAndGet);
        }, nowInMillis::incrementAndGet, duration -> assertEquals(1, duration));

        var acquiredLatch = new CountDownLatch(1);
        refCountedAcquirer.acquire(runAndRelease(acquiredLatch::countDown));
        acquiredLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(1));
            assertThat(released.get(), equalTo(1));
        });

        acquiredLatch = new CountDownLatch(1);
        refCountedAcquirer.acquire(runAndRelease(acquiredLatch::countDown));
        acquiredLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> {
            assertThat(acquired.get(), equalTo(2));
            assertThat(released.get(), equalTo(2));
        });
    }

    // test that a RefCountingAcquirer releases its resource if it acquires it
    public void testRefCountedAcquirer() throws Exception {
        AtomicInteger acquireCount = new AtomicInteger();
        AtomicInteger releaseCount = new AtomicInteger();

        Consumer<ActionListener<Releasable>> acquirer = (listener) -> new Thread(() -> {
            try {
                Thread.sleep(randomIntBetween(1, 100));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            acquireCount.incrementAndGet();
            listener.onResponse(releaseCount::incrementAndGet);
        }).start();

        SplitSourceService.RefCountedAcquirer refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(
            acquirer,
            nowInMillis::incrementAndGet,
            duration -> assertEquals(1, duration)
        );

        int numThreads = randomIntBetween(1, 10);
        Thread[] threads = new Thread[numThreads];
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final var tid = i;
            final int sleepMillis = randomIntBetween(1, 50);
            threads[i] = new Thread(() -> {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                logger.info("acquiring {}", tid);
                refCountedAcquirer.acquire(runAndRelease(latch::countDown));
            });
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        latch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> assertThat(releaseCount.get(), equalTo(acquireCount.get())));
    }

    // verify that if resource acquisition fails, the refcount is still decremented properly
    // we do this by verifying that we attempt to acquire again, which we only do when the refcount is at 0
    public void testReleaseWhenAcquireFails() {
        // increments when acquisition is attempted
        AtomicInteger acquired = new AtomicInteger();
        // will only increment if acquire is called with resource successfully held
        AtomicInteger withResource = new AtomicInteger();
        SplitSourceService.RefCountedAcquirer acquirer = new SplitSourceService.RefCountedAcquirer(listener -> {
            acquired.incrementAndGet();
            throw new IllegalStateException("oops");
        }, nowInMillis::incrementAndGet, duration -> fail("blocked duration recorded"));

        acquirer.acquire(runAndRelease(withResource::incrementAndGet));
        acquirer.acquire(runAndRelease(withResource::incrementAndGet));
        // attempted, then dropped ref, then attempted again
        assertEquals(acquired.get(), 2);
        // never actually obtained the resource
        assertEquals(withResource.get(), 0);
    }

    public void testSetupTargetShardFailsWhenSplitIsNoLongerInProgress() {
        var indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, true).build())
            .build();

        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings());

        // The null args are not reached before the request is rejected.
        var splitSourceService = new SplitSourceService(
            null,
            clusterService,
            null,
            mock(StatelessCommitService.class),
            null,
            null,
            null,
            Settings.EMPTY
        );

        var exception = expectThrows(
            StaleSplitRequestException.class,
            () -> splitSourceService.setupTargetShard(null, new ShardId(indexMetadata.getIndex(), 0), 1L, 1L, ActionListener.noop())
        );
        assertThat(exception.getMessage(), containsString("No split is in progress"));
    }

    public void testRefCountedAcquirerRecordsDurationFromAcquireStart() {
        final var clock = new AtomicLong(1000);
        final var recordedDuration = new AtomicLong(-1);
        final long waitMillis = randomLongBetween(1, 1000);
        final long holdMillis = randomLongBetween(1, 1000);

        var refCountedAcquirer = new SplitSourceService.RefCountedAcquirer(listener -> {
            clock.addAndGet(waitMillis);
            listener.onResponse(() -> {});
        }, clock::get, recordedDuration::set);

        var done = new PlainActionFuture<Void>();
        refCountedAcquirer.acquire(ActionListener.wrap(releasable -> {
            // Still waiting to release
            assertThat(recordedDuration.get(), equalTo(-1L));
            clock.addAndGet(holdMillis);
            releasable.close();
            done.onResponse(null);
        }, done::onFailure));
        done.actionGet(SAFE_AWAIT_TIMEOUT);

        assertThat(recordedDuration.get(), equalTo(waitMillis + holdMillis));
    }

    private ActionListener<Releasable> runAndRelease(Runnable runnable) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    runnable.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("acquiring failed", e);
            }
        };
    }

    private void assertHandoffThrottled(
        int numShards,
        double maxConcurrentHandoffPercentage,
        IntConsumer assertMaxConcurrentHandoffs,
        boolean testCancel
    ) {
        try (
            var threadPool = new TestThreadPool(getTestName());
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings())
        ) {
            var projectId = randomProjectIdOrDefault();
            int maxConcurrentHandoffs = Math.max(1, (int) (numShards * maxConcurrentHandoffPercentage / 100.0));
            assertMaxConcurrentHandoffs.accept(maxConcurrentHandoffs);

            var initialIndexMetadata = IndexMetadata.builder("test")
                .settings(indexSettings(IndexVersion.current(), numShards, 0))
                .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(numShards, 2))
                .build();
            var index = initialIndexMetadata.getIndex();
            var reshardIndexService = mock(ReshardIndexService.class);
            when(reshardIndexService.getReshardMetrics()).thenReturn(ReshardMetrics.NOOP);
            var service = new SplitSourceService(null, clusterService, null, null, null, reshardIndexService, null, Settings.EMPTY);

            Consumer<IndexReshardingMetadata> publish = metadata -> publishReshardingMetadata(
                clusterService,
                projectId,
                initialIndexMetadata,
                metadata,
                maxConcurrentHandoffPercentage,
                TimeValue.ZERO
            );

            var reshardingMetadata = initialIndexMetadata.getReshardingMetadata();
            publish.accept(reshardingMetadata);

            for (int i = 0; i < numShards; i++) {
                var targetShardId = new ShardId(index, numShards + i);
                var awaitFuture = new PlainActionFuture<Void>();
                var task = new CancellableTask(1, "test", "test", "split", TaskId.EMPTY_TASK_ID, Map.of());
                service.awaitHandoffSlot(task, awaitFuture, targetShardId);

                if (i < maxConcurrentHandoffs) {
                    awaitFuture.actionGet(SAFE_AWAIT_TIMEOUT);
                } else {
                    assertFalse("listener should be blocked while the HANDOFF slots are full", awaitFuture.isDone());
                    if (testCancel) {
                        TaskCancelHelper.cancel(task, "test");
                        expectThrows(TaskCancelledException.class, () -> awaitFuture.actionGet(SAFE_AWAIT_TIMEOUT));
                    }
                    // Transition to SPLIT, free HANDOFF slot
                    reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                        new ShardId(index, numShards + i - maxConcurrentHandoffs),
                        IndexReshardingState.Split.TargetShardState.SPLIT
                    );
                    publish.accept(reshardingMetadata);
                    if (testCancel == false) {
                        awaitFuture.actionGet(SAFE_AWAIT_TIMEOUT);
                    }
                }

                reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                    targetShardId,
                    IndexReshardingState.Split.TargetShardState.HANDOFF
                );
                publish.accept(reshardingMetadata);
            }
        }
    }

    public void testThrottleHandoffOneShard() {
        assertHandoffThrottled(randomIntBetween(2, 8), 12.5, maxConcurrentHandoffs -> assertThat(maxConcurrentHandoffs, equalTo(1)), false);
    }

    public void testThrottleHandoffMultipleShards() {
        int numShards = randomIntBetween(5, 20);
        int maxConcurrentHandoffPercentage = randomIntBetween((int) Math.ceil(200.0 / numShards), 99);
        assertHandoffThrottled(
            numShards,
            maxConcurrentHandoffPercentage,
            maxConcurrentHandoffs -> assertThat(maxConcurrentHandoffs, greaterThan(1)),
            false
        );
    }

    public void testHandoffThrottleDisabled() {
        int numShards = randomIntBetween(2, 10);
        assertHandoffThrottled(numShards, 100, maxConcurrentHandoffs -> assertThat(maxConcurrentHandoffs, equalTo(numShards)), false);
    }

    public void testHandoffThrottleFailsWhenSplitIsCancelledWhileWaiting() {
        int numShards = randomIntBetween(2, 8);
        assertHandoffThrottled(numShards, 12.5, maxConcurrentHandoffs -> assertThat(maxConcurrentHandoffs, lessThan(numShards)), true);
    }

    public void testHandoffThrottleHoldsWhileAboveLimit() {
        var threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ExecutorService generic() {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }
        };
        try (threadPool; ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings())) {
            var projectId = randomProjectIdOrDefault();
            var numShards = randomIntBetween(3, 20);
            double maxConcurrentHandoffPercentage = randomIntBetween(1, 30);
            int maxConcurrentHandoffs = Math.max(1, (int) (numShards * maxConcurrentHandoffPercentage / 100.0));
            assert maxConcurrentHandoffs < numShards - 1;
            // initial shards in handoff > maxConcurrentHandoffs
            int initialShardsInHandoff = randomIntBetween(maxConcurrentHandoffs + 1, numShards - 1);

            var initialIndexMetadata = IndexMetadata.builder("test")
                .settings(indexSettings(IndexVersion.current(), numShards, 0))
                .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(numShards, 2))
                .build();
            var index = initialIndexMetadata.getIndex();
            var reshardIndexService = mock(ReshardIndexService.class);
            when(reshardIndexService.getReshardMetrics()).thenReturn(ReshardMetrics.NOOP);
            var service = new SplitSourceService(null, clusterService, null, null, null, reshardIndexService, null, Settings.EMPTY);

            var reshardingMetadata = initialIndexMetadata.getReshardingMetadata();
            for (int i = 0; i < initialShardsInHandoff; i++) {
                reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                    new ShardId(index, numShards + i),
                    IndexReshardingState.Split.TargetShardState.HANDOFF
                );
            }
            publishReshardingMetadata(
                clusterService,
                projectId,
                initialIndexMetadata,
                reshardingMetadata,
                maxConcurrentHandoffPercentage,
                TimeValue.ZERO
            );

            var handoffFuture = new PlainActionFuture<Void>();
            var task = new CancellableTask(1, "test", "test", "split", TaskId.EMPTY_TASK_ID, Map.of());
            service.awaitHandoffSlot(task, handoffFuture, new ShardId(index, numShards + initialShardsInHandoff));

            // move target shards to SPLIT
            for (int i = 0; i < initialShardsInHandoff - maxConcurrentHandoffs + 1; i++) {
                assertFalse("listener should be blocked while HANDOFF is at or above the limit", handoffFuture.isDone());
                reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                    new ShardId(index, numShards + i),
                    IndexReshardingState.Split.TargetShardState.SPLIT
                );
                publishReshardingMetadata(
                    clusterService,
                    projectId,
                    initialIndexMetadata,
                    reshardingMetadata,
                    maxConcurrentHandoffPercentage,
                    TimeValue.ZERO
                );
            }
            handoffFuture.actionGet(SAFE_AWAIT_TIMEOUT);
        }
    }

    public void testHandoffThrottleChecksAgainAfterDelay() {
        var threadPool = new CapturingThreadPool(getTestName());
        try (threadPool; ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings())) {
            var setup = new TwoTargetsSetup(clusterService, TimeValue.timeValueSeconds(1));
            var waiting = setup.waitForSlotOfThirdTarget();

            // The slot frees, which only starts the delay
            setup.moveToSplit(setup.first);
            assertFalse(waiting.isDone());
            assertThat(threadPool.delayed, hasSize(1));

            // Another shard takes the slot during the delay, shard should wait again
            setup.moveToHandoff(setup.second);
            threadPool.delayed.remove().run();
            assertFalse(waiting.isDone());
            assertThat(threadPool.delayed, hasSize(0));

            // The slot frees again
            setup.moveToSplit(setup.second);
            assertThat(threadPool.delayed, hasSize(1));
            threadPool.delayed.remove().run();
            waiting.actionGet(SAFE_AWAIT_TIMEOUT);

            threadPool.delays.forEach(delay -> assertThat(delay.millis(), lessThanOrEqualTo(TimeValue.timeValueSeconds(1).millis())));
        }
    }

    public void testHandoffThrottleIsCancelledDuringDelay() {
        var threadPool = new CapturingThreadPool(getTestName());
        try (threadPool; ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings())) {
            var setup = new TwoTargetsSetup(clusterService, TimeValue.timeValueSeconds(1));
            var waiting = setup.waitForSlotOfThirdTarget();

            setup.moveToSplit(setup.first);
            assertThat(threadPool.delayed, hasSize(1));

            TaskCancelHelper.cancel(setup.task, "test");
            expectThrows(TaskCancelledException.class, () -> waiting.actionGet(SAFE_AWAIT_TIMEOUT));

            threadPool.delayed.remove().run();
            assertThat(threadPool.delayed, hasSize(0));
        }
    }

    public void testHandoffThrottleProceedsAnywayOnTimeout() {
        var threadPool = new CapturingThreadPool(getTestName());
        try (threadPool; ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings())) {
            var setup = new TwoTargetsSetup(clusterService, TimeValue.timeValueSeconds(1));
            var waiting = setup.waitForSlotOfThirdTarget();
            assertThat(threadPool.timeouts, hasSize(1));

            threadPool.timeouts.remove().run();
            waiting.actionGet(SAFE_AWAIT_TIMEOUT);

            // Free the slot
            setup.moveToSplit(setup.first);
            threadPool.delayed.remove().run();
            assertThat(threadPool.delayed, hasSize(0));
        }
    }

    private static class CapturingThreadPool extends TestThreadPool {
        final Queue<Runnable> delayed = new ConcurrentLinkedQueue<>();
        final List<TimeValue> delays = new CopyOnWriteArrayList<>();
        final Queue<Runnable> timeouts = new ConcurrentLinkedQueue<>();

        CapturingThreadPool(String name) {
            super(name);
        }

        @Override
        public ExecutorService generic() {
            return EsExecutors.DIRECT_EXECUTOR_SERVICE;
        }

        @Override
        public void scheduleUnlessShuttingDown(TimeValue delay, Executor executor, Runnable command) {
            if (command.getClass().getName().startsWith(SplitSourceService.class.getName()) == false) {
                super.scheduleUnlessShuttingDown(delay, executor, command);
            } else if (delay.equals(SplitSourceService.HANDOFF_SLOT_TIMEOUT)) {
                timeouts.add(command);
            } else {
                delays.add(delay);
                delayed.add(command);
            }
        }
    }

    private class TwoTargetsSetup {
        final ClusterService clusterService;
        final ProjectId projectId = randomProjectIdOrDefault();
        final double percentage = SplitSourceService.RESHARD_SPLIT_MAX_CONCURRENT_HANDOFF_PERCENTAGE.getDefault(Settings.EMPTY);
        final IndexMetadata initialIndexMetadata;
        final ShardId first;
        final ShardId second;
        final ShardId third;
        final SplitSourceService service;
        final CancellableTask task = new CancellableTask(1, "test", "test", "split", TaskId.EMPTY_TASK_ID, Map.of());
        final TimeValue maxJitter;
        IndexReshardingMetadata reshardingMetadata;

        TwoTargetsSetup(ClusterService clusterService, TimeValue maxJitter) {
            this.clusterService = clusterService;
            // 12.5% of 8 = 1, only allow 1 target shard in HANDOFF at once
            int numShards = randomIntBetween(3, 8);
            initialIndexMetadata = IndexMetadata.builder("test")
                .settings(indexSettings(IndexVersion.current(), numShards, 0))
                .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(numShards, 2))
                .build();
            first = new ShardId(initialIndexMetadata.getIndex(), numShards);
            second = new ShardId(initialIndexMetadata.getIndex(), numShards + 1);
            third = new ShardId(initialIndexMetadata.getIndex(), numShards + 2);
            var reshardIndexService = mock(ReshardIndexService.class);
            when(reshardIndexService.getReshardMetrics()).thenReturn(ReshardMetrics.NOOP);
            service = new SplitSourceService(null, clusterService, null, null, null, reshardIndexService, null, Settings.EMPTY);
            this.maxJitter = maxJitter;
            reshardingMetadata = initialIndexMetadata.getReshardingMetadata();
            moveToHandoff(first);
        }

        PlainActionFuture<Void> waitForSlotOfThirdTarget() {
            var waiting = new PlainActionFuture<Void>();
            service.awaitHandoffSlot(task, waiting, third);
            assertFalse("listener should be blocked while the HANDOFF slot is taken", waiting.isDone());
            return waiting;
        }

        void moveToHandoff(ShardId target) {
            move(target, IndexReshardingState.Split.TargetShardState.HANDOFF);
        }

        void moveToSplit(ShardId target) {
            move(target, IndexReshardingState.Split.TargetShardState.SPLIT);
        }

        private void move(ShardId target, IndexReshardingState.Split.TargetShardState state) {
            reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(target, state);
            publishReshardingMetadata(clusterService, projectId, initialIndexMetadata, reshardingMetadata, percentage, maxJitter);
        }
    }

    private static void publishReshardingMetadata(
        ClusterService clusterService,
        ProjectId projectId,
        IndexMetadata baseIndexMetadata,
        IndexReshardingMetadata reshardingMetadata,
        double maxConcurrentHandoffPercentage,
        TimeValue maxJitter
    ) {
        var persistentSettings = Settings.builder()
            .put(SplitSourceService.RESHARD_SPLIT_MAX_CONCURRENT_HANDOFF_PERCENTAGE.getKey(), maxConcurrentHandoffPercentage)
            .put(SplitSourceService.HANDOFF_THROTTLE_MAX_JITTER.getKey(), maxJitter)
            .build();
        var indexMetadata = IndexMetadata.builder(baseIndexMetadata).reshardingMetadata(reshardingMetadata).build();
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(
                    Metadata.builder()
                        .persistentSettings(persistentSettings)
                        .put(ProjectMetadata.builder(projectId).put(indexMetadata, true).build())
                )
                .build()
        );
    }

    private static ClusterSettings clusterSettings() {
        var registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.add(SplitSourceService.RESHARD_SPLIT_MAX_CONCURRENT_HANDOFF_PERCENTAGE);
        registered.add(SplitSourceService.HANDOFF_THROTTLE_MAX_JITTER);
        return new ClusterSettings(Settings.EMPTY, registered);
    }
}
