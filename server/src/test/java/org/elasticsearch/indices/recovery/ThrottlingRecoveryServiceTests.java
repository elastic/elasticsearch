/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.AbstractProjectResolver;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThrottlingRecoveryServiceTests extends ESTestCase {
    private static TestThreadPool threadPool;
    private RecoveryStats stats = new RecoveryStats();

    @BeforeClass
    public static void init() throws Exception {
        threadPool = new TestThreadPool(ThrottlingRecoveryServiceTests.class.getSimpleName());
    }

    @AfterClass
    public static void close() throws Exception {
        terminate(threadPool);
    }

    @After
    public void verifyStats() {
        // recovery stats counters should always be back to 0 at the end of tests
        assertTrue(stats.noCurrentRecoveries());
        stats = new RecoveryStats();
    }

    /// Regression test for a context-leaking bug: [issue-152039](https://github.com/elastic/elasticsearch/issues/152039)
    public void testQueuedRecoveryWithProjectIds() throws Exception {
        final var multiProjectResolver = new AbstractProjectResolver(() -> threadPool.getThreadContext()) {
            @Override
            protected ProjectId getFallbackProjectId() {
                return ProjectId.DEFAULT;
            }

            @Override
            protected boolean allowAccessToAllProjects(ThreadContext threadContext) {
                return true;
            }
        };
        final var projectId1 = randomUniqueProjectId();
        final var projectId2 = randomUniqueProjectId();
        final var projectId3 = randomUniqueProjectId();

        final var service = new ThrottlingRecoveryService(
            threadPool,
            multiProjectResolver,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );

        final var firstRecoveryRunning = new CountDownLatch(1);
        final var firstRecoveryProceed = new CountDownLatch(1);
        final var thirdRecoveryDone = new CountDownLatch(1);

        service.enqueue(projectId1, RecoveryListener.NOOP, newRecoveryState(), UUIDs.randomBase64UUID(), stats, listener -> {
            assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId1.id()));
            firstRecoveryRunning.countDown();
            safeAwait(firstRecoveryProceed);
            listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        });

        safeAwait(firstRecoveryRunning);

        // Test the failure path
        final var secondListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(RecoveryState state, ShardLongFieldRange t, ShardLongFieldRange e) {
                fail("unexpected success");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId2.id()));
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };

        service.enqueue(projectId2, secondListener, newRecoveryState(), UUIDs.randomBase64UUID(), stats, ignored -> {
            assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId2.id()));
            throw new RuntimeException("test simulated failure");
        });

        // Test the success path
        final var thirdListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(RecoveryState state, ShardLongFieldRange t, ShardLongFieldRange e) {
                assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId3.id()));
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };
        service.enqueue(projectId3, thirdListener, newRecoveryState(), UUIDs.randomBase64UUID(), stats, listener -> {
            assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId3.id()));
            listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            thirdRecoveryDone.countDown();
        });

        assertThat(service.currentQueueSize(), equalTo(2));
        firstRecoveryProceed.countDown();
        safeAwait(thirdRecoveryDone);
    }

    public void testSynchronousTaskRunsOnProvidedThreadPoolAndNotifiesUserListener() {
        // Use real threads instead of DeterministicTaskQueue to verify actual threading behavior below
        final var recoveryType = randomFrom(RecoverySource.Type.values());
        final var service = new ThrottlingRecoveryService(
            threadPool,
            DefaultProjectResolver.INSTANCE,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );
        final var callerThread = Thread.currentThread();
        final var executionThread = new AtomicReference<Thread>();
        final var consumerReturned = new CountDownLatch(1);
        final var expectedStats = new RecoveryStats();

        final var listener = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        expectedStats.targetRecoveryQueued(recoveryType);
        service.enqueue(
            ProjectId.DEFAULT,
            listener,
            newRecoveryState(recoveryType, new ShardId(randomIndexName(), IndexMetadata.INDEX_UUID_NA_VALUE, 1)),
            UUIDs.randomBase64UUID(),
            stats,
            schedulingListener -> {
                executionThread.set(Thread.currentThread());

                expectedStats.targetRecoveryDequeuedAndStarted(recoveryType);
                assertThat(stats, equalTo(expectedStats));

                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                assertTrue("user listener should have been notified of completion", listener.wasNotified());
                consumerReturned.countDown();
            }
        );
        safeAwait(consumerReturned);
        expectedStats.targetRecoveryCompleted(recoveryType);
        assertThat(stats, equalTo(expectedStats));
        assertThat("recovery executed on enqueueing thread instead of generic pool", executionThread.get(), not(equalTo(callerThread)));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    /// Asynchronous task: consumer returns before the scheduling listener receives a terminal callback.
    public void testAsynchronousTaskListenerNotificationAfterConsumerReturns() {
        // Use real threads instead of DeterministicTaskQueue to be able to use safeAwait below
        final var service = new ThrottlingRecoveryService(
            threadPool,
            DefaultProjectResolver.INSTANCE,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );
        final var consumerReturned = new CountDownLatch(1);
        final var recoveryDone = new CountDownLatch(1);
        final var userListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                assertThat("terminal callback should follow consumer return", consumerReturned.getCount(), equalTo(0L));
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };
        service.enqueue(ProjectId.DEFAULT, userListener, newRecoveryState(), UUIDs.randomBase64UUID(), stats, schedulingListener -> {
            threadPool.generic().execute(() -> {
                safeAwait(consumerReturned);
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                recoveryDone.countDown();
            });
            consumerReturned.countDown();
        });
        safeAwait(recoveryDone);
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testMaxConcurrencyBoundWithAsynchronousTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final int maxConcurrentRecoveries = between(2, 5);
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(maxConcurrentRecoveries),
            RecoverySchedulingListener.NOOP
        );
        final var running = new AtomicInteger();
        final var completed = new AtomicInteger();
        final var peakConcurrent = new AtomicInteger();
        final int totalEnqueuedTasks = maxConcurrentRecoveries * 3;

        RecoveryListener trackingListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                running.decrementAndGet();
                completed.incrementAndGet();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                fail(e);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };

        long initialTime = taskQueue.getCurrentTimeMillis();
        for (int i = 0; i < totalEnqueuedTasks; i++) {
            final var ordinal = i;
            service.enqueue(
                ProjectId.DEFAULT,
                trackingListener,
                newRecoveryState(),
                UUIDs.randomBase64UUID(),
                stats,
                schedulingListener -> {
                    int current = running.incrementAndGet();
                    peakConcurrent.accumulateAndGet(current, Integer::max);
                    // Schedule completion for the future
                    final var minDelay = ((ordinal / maxConcurrentRecoveries) + 1) * 100 + 1;
                    final var maxDelay = minDelay + 99;
                    taskQueue.scheduleAt(
                        initialTime + randomIntBetween(minDelay, maxDelay),
                        () -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
                    );
                }
            );
        }
        taskQueue.runAllRunnableTasks();
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));
        assertThat(completed.get(), equalTo(0));

        // Complete first batch
        taskQueue.runTasksUpToTimeInOrder(initialTime + 200);
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));
        assertThat(completed.get(), equalTo(maxConcurrentRecoveries));

        // Complete second batch
        taskQueue.runTasksUpToTimeInOrder(initialTime + 300);
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));
        assertThat(completed.get(), equalTo(maxConcurrentRecoveries * 2));

        // Complete all
        taskQueue.runAllTasks();
        assertThat(running.get(), equalTo(0));
        assertThat(service.currentQueueSize(), equalTo(0));
        assertThat(completed.get(), equalTo(totalEnqueuedTasks));
        assertThat(peakConcurrent.get(), equalTo(maxConcurrentRecoveries));
    }

    public void testIncreasingMaxConcurrentRecoveriesStartsPendingTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(2);
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            clusterService,
            RecoverySchedulingListener.NOOP
        );
        final var started = new AtomicInteger();

        for (int i = 0; i < 10; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED),
                newRecoveryState(),
                UUIDs.randomBase64UUID(),
                stats,
                schedulingListener -> {
                    started.incrementAndGet();
                    taskQueue.scheduleAt(
                        taskQueue.getCurrentTimeMillis() + 100, // Delay completion until we explicitly trigger time jump
                        () -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
                    );
                }
            );
        }

        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(2));

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 4).build());
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(4));
        taskQueue.runAllTasks();
        assertThat(started.get(), equalTo(10));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testDecreasingMaxConcurrentRecoveriesDefersQueueWithoutCancellingRunningTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(3);
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            clusterService,
            RecoverySchedulingListener.NOOP
        );
        final var started = new AtomicInteger();
        final var done = new AtomicInteger();

        // Delay completion until we explicitly trigger time jump
        final long initialTime = taskQueue.getCurrentTimeMillis();
        for (int i = 0; i < 6; i++) {
            final var ordinal = i;
            service.enqueue(ProjectId.DEFAULT, new RecoveryListener() {
                @Override
                public void onRecoveryDone(
                    RecoveryState state,
                    ShardLongFieldRange timestampMillisFieldRange,
                    ShardLongFieldRange eventIngestedMillisFieldRange
                ) {
                    done.incrementAndGet();
                }

                @Override
                public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                    fail("unexpected recovery failure");
                }

                @Override
                public void onRecoveryAborted() {
                    fail("unexpected recovery abortion");
                }
            }, newRecoveryState(), UUIDs.randomBase64UUID(), stats, schedulingListener -> {
                started.incrementAndGet();
                taskQueue.scheduleAt(
                    initialTime + 100 + ordinal,
                    () -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
                );
            });
        }

        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(3));
        assertThat(done.get(), equalTo(0));

        // Lower limit to 1
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build());

        // Complete one task, should not start more (still have 2 running)
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(3));
        assertThat(done.get(), equalTo(1));

        // Complete second, should not start more (still have 1 running)
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(3));
        assertThat(done.get(), equalTo(2));

        // Complete third, now we're at 0 running, so should start the 4th
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(4));
        assertThat(done.get(), equalTo(3));

        // Complete remaining
        taskQueue.runAllTasks();
        assertThat(started.get(), equalTo(6));
        assertThat(done.get(), equalTo(6));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testFifoWhenThrottledToOneConcurrentWithSynchronousCompletion() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );
        final int total = between(10, 20);
        final var completionOrder = new CopyOnWriteArrayList<Integer>();

        for (int i = 0; i < total; i++) {
            final int sequence = i;
            RecoveryListener userListener = new RecoveryListener() {
                @Override
                public void onRecoveryDone(
                    RecoveryState state,
                    ShardLongFieldRange timestampMillisFieldRange,
                    ShardLongFieldRange eventIngestedMillisFieldRange
                ) {
                    completionOrder.add(sequence);
                }

                @Override
                public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                    fail(e);
                }

                @Override
                public void onRecoveryAborted() {
                    fail("recovery aborted");
                }
            };
            service.enqueue(
                ProjectId.DEFAULT,
                userListener,
                newRecoveryState(),
                UUIDs.randomBase64UUID(),
                stats,
                schedulingListener -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            );
        }

        taskQueue.runAllRunnableTasks();
        assertThat(completionOrder.size(), equalTo(total));
        for (int i = 0; i < total; i++) {
            assertThat(completionOrder.get(i), equalTo(i));
        }
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testFailureTriggersNextQueuedRecovery() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );

        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.FAILED);
        service.enqueue(ProjectId.DEFAULT, listener1, newRecoveryState(), UUIDs.randomBase64UUID(), stats, ignored -> {
            throw new RuntimeException("test recovery task injected failure");
        });

        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(ProjectId.DEFAULT, listener2, newRecoveryState(), UUIDs.randomBase64UUID(), stats, schedulingListener -> {
            assertTrue("first task should have completed before second one started", listener1.wasNotified());
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        });

        taskQueue.runAllRunnableTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener1, listener2);
    }

    public void testRecoveryAbortedTriggersNextQueuedRecovery() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );

        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.ABORTED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener1,
            newRecoveryState(),
            UUIDs.randomBase64UUID(),
            stats,
            RecoveryListener::onRecoveryAborted
        );
        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(ProjectId.DEFAULT, listener2, newRecoveryState(), UUIDs.randomBase64UUID(), stats, schedulingListener -> {
            assertTrue("first task should have completed before second one started", listener1.wasNotified());
            schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        });

        taskQueue.runAllRunnableTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener1, listener2);
    }

    public void testCloseAbortsQueuedButNotDispatchedRecoveries() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );

        final var runningTaskDispatched = new AtomicBoolean();
        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(ProjectId.DEFAULT, listener1, newRecoveryState(), UUIDs.randomBase64UUID(), stats, listener -> {
            runningTaskDispatched.set(true);
            taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 1000,
                () -> listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            );
        });
        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.ABORTED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener2,
            newRecoveryState(),
            UUIDs.randomBase64UUID(),
            stats,
            ignored -> fail("queued task should not be dispatched after close")
        );

        taskQueue.runAllRunnableTasks();
        assertTrue("first task should have been dispatched", runningTaskDispatched.get());
        assertFalse(listener1.wasNotified());
        assertThat("second task should still be queued", service.currentQueueSize(), equalTo(1));
        assertFalse(listener2.wasNotified());

        service.close();
        assertThat(service.currentQueueSize(), equalTo(0));
        taskQueue.runAllTasks();
        ensureListenersWereNotified();
    }

    public void testEnqueueAfterCloseImmediatelyAborts() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );
        service.close();

        final var listener = new TestCaptureResultListener(ExpectedRecoveryOutcome.ABORTED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener,
            newRecoveryState(),
            UUIDs.randomBase64UUID(),
            stats,
            ignored -> fail("should not be dispatched after close")
        );
        ensureListenersWereNotified(listener);
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testRecordedCancellationAppliedAtEnqueueTime() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(10),
            RecoverySchedulingListener.NOOP
        );
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        assertTrue(service.cancelRecoveries(Map.of(allocationId, shardId)).isEmpty());

        final var listener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            listener,
            newRecoveryState(shardId),
            allocationId,
            stats,
            ignored -> fail("task should have been cancelled")
        );
        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener);
    }

    public void testCancellationAppliedWhenTaskInPendingQueue() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(1),
            RecoverySchedulingListener.NOOP
        );
        final var shardId1 = new ShardId("index1", UUIDs.randomBase64UUID(), 0);
        final var allocationId1 = UUIDs.randomBase64UUID();
        final var shardId2 = new ShardId("index2", UUIDs.randomBase64UUID(), 0);
        final var allocationId2 = UUIDs.randomBase64UUID();

        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_STARTED);
        service.enqueue(ProjectId.DEFAULT, listener1, newRecoveryState(shardId1), allocationId1, new RecoveryStats(), listener -> {
            // simulates cancellation of started recovery
            taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 100,
                () -> listener.onRecoveryFailure(new RecoveryCancelledException(shardId1, null, null), true)
            );
        });

        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            listener2,
            newRecoveryState(shardId2),
            allocationId2,
            stats,
            ignored -> fail("task should have been cancelled")
        );
        assertThat(service.cancelRecoveries(Map.of(allocationId1, shardId1, allocationId2, shardId2)), equalTo(Set.of(allocationId2)));
        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener1, listener2);
    }

    public void testStaleRecordedEntryRemovedOnClusterStateChangeWithLocalNodeNull() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(10),
            RecoverySchedulingListener.NOOP
        );
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        assertTrue(service.cancelRecoveries(Map.of(allocationId, shardId)).isEmpty());

        final var event = mock(ClusterChangedEvent.class);
        final var state = mock(ClusterState.class);
        final var routingNodes = mock(RoutingNodes.class);
        when(event.state()).thenReturn(state);
        when(state.getRoutingNodes()).thenReturn(routingNodes);
        when(routingNodes.node(anyString())).thenReturn(null);
        service.clusterChanged(event);

        final var recoveryState = newRecoveryState(shardId);
        final var listener = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener,
            recoveryState,
            allocationId,
            stats,
            l -> l.onRecoveryDone(recoveryState, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
        );
        taskQueue.runAllRunnableTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener);
    }

    public void testStaleRecordedEntryRemovedOnClusterStateChangeWithShardRelocated() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(10);
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            clusterService,
            RecoverySchedulingListener.NOOP
        );
        final var staleShardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var staleAllocationId = UUIDs.randomBase64UUID();
        final var retainedShardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var retainedShardRouting = TestShardRouting.newShardRouting(
            retainedShardId,
            clusterService.localNode().getId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        final var retainedAllocationId = retainedShardRouting.allocationId().getId();

        assertTrue(service.cancelRecoveries(Map.of(staleAllocationId, staleShardId, retainedAllocationId, retainedShardId)).isEmpty());

        final var event = mock(ClusterChangedEvent.class);
        final var state = mock(ClusterState.class);
        final var routingNodes = mock(RoutingNodes.class);
        final var routingNode = mock(RoutingNode.class);
        when(event.state()).thenReturn(state);
        when(state.getRoutingNodes()).thenReturn(routingNodes);
        when(routingNodes.node(clusterService.localNode().getId())).thenReturn(routingNode);
        // staleShardId relocated away from this node, retainedShardId is still here
        when(routingNode.getByShardId(staleShardId)).thenReturn(null);
        when(routingNode.getByShardId(retainedShardId)).thenReturn(retainedShardRouting);
        service.clusterChanged(event);

        final var staleRecoveryState = newRecoveryState(staleShardId);
        final var staleListener = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(
            ProjectId.DEFAULT,
            staleListener,
            staleRecoveryState,
            staleAllocationId,
            stats,
            l -> l.onRecoveryDone(staleRecoveryState, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
        );

        final var retainedListener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            retainedListener,
            newRecoveryState(retainedShardId),
            retainedAllocationId,
            stats,
            ignored -> fail("task should have been cancelled")
        );

        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(staleListener, retainedListener);
    }

    public void testPendingRecoveryDiscardedWhenLocalNodeRemoved() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(1);
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            clusterService,
            RecoverySchedulingListener.NOOP
        );

        final var blockerShardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var blockerListener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_STARTED);
        service.enqueue(ProjectId.DEFAULT, blockerListener, newRecoveryState(blockerShardId), UUIDs.randomBase64UUID(), stats, listener -> {
            // occupies the sole concurrency slot
            taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 100,
                () -> listener.onRecoveryFailure(new RecoveryCancelledException(blockerShardId, null, null), true)
            );
        });

        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        final var listener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            listener,
            newRecoveryState(shardId),
            allocationId,
            stats,
            ignored -> fail("task should have been cancelled")
        );
        assertThat(service.currentQueueSize(), equalTo(1));

        // Simulate this node leaving the cluster's data nodes entirely (e.g. it's shutting down).
        final var event = mock(ClusterChangedEvent.class);
        final var state = mock(ClusterState.class);
        final var routingNodes = mock(RoutingNodes.class);
        when(event.state()).thenReturn(state);
        when(state.getRoutingNodes()).thenReturn(routingNodes);
        when(routingNodes.node(clusterService.localNode().getId())).thenReturn(null);
        service.clusterChanged(event);

        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(blockerListener, listener);
    }

    public void testPendingRecoveryDiscardedWhenAllocationIdChangesWhileQueued() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(1);
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            clusterService,
            RecoverySchedulingListener.NOOP
        );

        final var blockerShardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var blockerListener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_STARTED);
        service.enqueue(ProjectId.DEFAULT, blockerListener, newRecoveryState(blockerShardId), UUIDs.randomBase64UUID(), stats, listener -> {
            // occupies the sole concurrency slot
            taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 100,
                () -> listener.onRecoveryFailure(new RecoveryCancelledException(blockerShardId, null, null), true)
            );
        });

        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var oldAllocationId = UUIDs.randomBase64UUID();
        final var newShardRouting = TestShardRouting.newShardRouting(
            shardId,
            clusterService.localNode().getId(),
            true,
            ShardRoutingState.INITIALIZING
        );

        final var listener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            listener,
            newRecoveryState(shardId),
            oldAllocationId,
            stats,
            ignored -> fail("task should have been cancelled")
        );
        assertThat(service.currentQueueSize(), equalTo(1));

        final var event = mock(ClusterChangedEvent.class);
        final var state = mock(ClusterState.class);
        final var routingNodes = mock(RoutingNodes.class);
        final var routingNode = mock(RoutingNode.class);
        when(event.state()).thenReturn(state);
        when(state.getRoutingNodes()).thenReturn(routingNodes);
        when(routingNodes.node(clusterService.localNode().getId())).thenReturn(routingNode);
        when(routingNode.getByShardId(shardId)).thenReturn(newShardRouting);
        service.clusterChanged(event);

        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(blockerListener, listener);
    }

    public void testCancelRecoveryReturnsEmptyWhenNoLongerInQueue() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(10),
            RecoverySchedulingListener.NOOP
        );
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();
        final var recoveryState = newRecoveryState(shardId);

        service.enqueue(
            ProjectId.DEFAULT,
            new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED),
            recoveryState,
            allocationId,
            stats,
            l -> taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 100,
                () -> l.onRecoveryDone(recoveryState, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            )
        );

        taskQueue.runAllRunnableTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        assertTrue(
            "should return empty set, task is no longer in pending queue",
            service.cancelRecoveries(Map.of(allocationId, shardId)).isEmpty()
        );
        taskQueue.runAllTasks();
    }

    /// Stress one [ThrottlingRecoveryService] by enqueueing many tasks with randomized completion times,
    /// alternating bursty submits and completion periods, and randomly changing the max concurrent limit.
    /// Verify that all tasks finish and that concurrent execution never exceeds the limit applied.
    public void testStressConcurrentEnqueueMaintainsBoundsAndCompleteness() {
        final var taskQueue = new DeterministicTaskQueue();
        taskQueue.setExecutionDelayVariabilityMillis(100);

        final var maxConcurrency = new AtomicInteger(between(1, 20));
        final var clusterService = newClusterService(maxConcurrency.get());
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            clusterService,
            RecoverySchedulingListener.NOOP
        );

        final var recoveryState = newRecoveryState();
        final var running = new AtomicInteger();
        final var completed = new AtomicInteger();
        final var totalTaskCount = new AtomicInteger();

        RecoveryListener trackingListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                completed.incrementAndGet();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                completed.incrementAndGet();
            }

            @Override
            public void onRecoveryAborted() {
                completed.incrementAndGet();
            }
        };

        for (int iteration = 0; iteration < 20; iteration++) {
            maxConcurrency.set(randomBoolean() ? between(1, 50) : Integer.MAX_VALUE);
            clusterService.getClusterSettings()
                .applySettings(
                    Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrency.get()).build()
                );

            final var incomingTasks = randomIntBetween(50, 100);
            totalTaskCount.addAndGet(incomingTasks);
            for (int i = 0; i < incomingTasks; i++) {
                if (iteration > 15 && rarely()) {
                    // idempotent
                    service.close();
                }
                taskQueue.scheduleNow(
                    () -> service.enqueue(
                        ProjectId.DEFAULT,
                        trackingListener,
                        recoveryState,
                        UUIDs.randomBase64UUID(),
                        stats,
                        schedulingListener -> {
                            assertThat(running.incrementAndGet(), lessThanOrEqualTo(maxConcurrency.get()));
                            final var currentTime = taskQueue.getCurrentTimeMillis();
                            taskQueue.scheduleAt(currentTime + randomIntBetween(0, 100), () -> {
                                running.decrementAndGet();
                                // Randomly choose completion type
                                if (randomBoolean()) {
                                    schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                                } else {
                                    if (randomBoolean()) {
                                        schedulingListener.onRecoveryAborted();
                                    } else {
                                        schedulingListener.onRecoveryFailure(
                                            new RecoveryFailedException(
                                                recoveryState,
                                                null,
                                                new RuntimeException("test recovery task injected failure")
                                            ),
                                            false
                                        );
                                    }
                                }
                            });
                        }
                    )
                );
                taskQueue.runAllRunnableTasks();
                while (randomBoolean() && taskQueue.hasDeferredTasks()) {
                    if (service.isClosed()) {
                        assertThat(service.currentQueueSize(), equalTo(0));
                    }
                    taskQueue.advanceTime();
                    taskQueue.runAllRunnableTasks();
                }
                if (service.isClosed()) {
                    assertThat(service.currentQueueSize(), equalTo(0));
                }
            }
            // Execute all enqueued and scheduled tasks
            taskQueue.runAllTasks();
            assertThat(completed.get(), equalTo(totalTaskCount.get()));
            assertThat(running.get(), equalTo(0));
            assertThat(service.currentQueueSize(), equalTo(0));
        }
    }

    /// Stress one [ThrottlingRecoveryService] from many producer threads using real threads: alternating
    /// bursty submits (high contention on the throttle) and idle periods. Verify that all tasks finish and
    /// that concurrent recovery executions count never exceeded the peak value of `maxConcurrentRecoveries`.
    ///
    /// Unlike [#testStressConcurrentEnqueueMaintainsBoundsAndCompleteness], this test uses real threads to
    /// catch missing happens-before relationships that a deterministic scheduler cannot expose.
    public void testStressConcurrentEnqueueWithRealThreads() throws Exception {
        final int initialMaxConcurrentRecoveries = between(1, 20);
        final var clusterService = newClusterService(initialMaxConcurrentRecoveries);
        final var peakLimit = new AtomicInteger(initialMaxConcurrentRecoveries);
        final var throttlingRecoveryService = new ThrottlingRecoveryService(
            threadPool,
            DefaultProjectResolver.INSTANCE,
            clusterService,
            RecoverySchedulingListener.NOOP
        );

        final var currentMaxConcurrentRecoveries = new AtomicInteger(peakLimit.get());
        final var runningOrPending = new AtomicInteger();
        final var running = new AtomicInteger();
        final var peakRunning = new AtomicInteger();
        final var tasksEnqueued = new AtomicInteger();
        final var tasksCompleted = new AtomicInteger();
        final var allFinished = new CountDownLatch(1);
        final var refCounted = AbstractRefCounted.of(allFinished::countDown);
        final int maxTaskCount = 1000;
        final var recoveryState = newRecoveryState();

        final var trackingListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                runningOrPending.decrementAndGet();
                tasksCompleted.incrementAndGet();
                refCounted.decRef();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                runningOrPending.decrementAndGet();
                tasksCompleted.incrementAndGet();
                refCounted.decRef();
            }

            @Override
            public void onRecoveryAborted() {
                runningOrPending.decrementAndGet();
                tasksCompleted.incrementAndGet();
                refCounted.decRef();
            }
        };

        final int producerThreads = between(1, 6);
        runInParallel(producerThreads, index -> {
            while (tasksEnqueued.get() < maxTaskCount) {
                if (index == 0) {
                    if (rarely()) {
                        int nextLimit = between(1, 20);
                        clusterService.getClusterSettings()
                            .applySettings(
                                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), nextLimit).build()
                            );
                        peakLimit.accumulateAndGet(nextLimit, Integer::max);
                        currentMaxConcurrentRecoveries.set(nextLimit);
                    }
                    if ((tasksEnqueued.get() * 1.0 / maxTaskCount) > 0.8 && rarely()) {
                        throttlingRecoveryService.close();
                    }
                }

                int localRunningOrPending = runningOrPending.get();
                int localLimit = currentMaxConcurrentRecoveries.get();
                if (randomDouble() > localRunningOrPending * 1.0 / localLimit) {
                    // Likelihood to generate load is proportional to the number of free slots.
                    // If all slots are free (localRunningOrPending == 0), likelihood is 100%.
                    // Rarely burst with enough tasks to fill the queue.
                    boolean burst = rarely();
                    int incomingTasks = burst ? localLimit : 1;
                    for (int i = 0; i < incomingTasks && tasksEnqueued.get() < maxTaskCount; i++) {
                        refCounted.incRef();
                        runningOrPending.incrementAndGet();
                        tasksEnqueued.incrementAndGet();
                        throttlingRecoveryService.enqueue(
                            ProjectId.DEFAULT,
                            trackingListener,
                            recoveryState,
                            UUIDs.randomBase64UUID(),
                            stats,
                            schedulingListener -> {
                                peakRunning.accumulateAndGet(running.incrementAndGet(), Integer::max);
                                runStressInboundRecoveryTask(recoveryState, schedulingListener, running);
                            }
                        );
                        Thread.yield();
                    }
                }
                Thread.yield();
            }
        });

        // refCounted starts with 1 ref, decremented here
        refCounted.decRef();
        safeAwait(allFinished, TimeValue.timeValueSeconds(30));
        // stats are updated after onRecoveryDone is called
        assertBusy(() -> assertThat(stats, equalTo(new RecoveryStats())));
        assertThat(tasksCompleted.get(), equalTo(tasksEnqueued.get()));
        assertThat(peakRunning.get(), lessThanOrEqualTo(peakLimit.get()));
    }

    private static void runStressInboundRecoveryTask(
        RecoveryState recoveryState,
        RecoveryListener schedulingListener,
        AtomicInteger running
    ) {
        threadPool.generic().execute(() -> {
            Thread.yield();
            running.decrementAndGet();
            if (randomBoolean()) {
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            } else {
                if (randomBoolean()) {
                    schedulingListener.onRecoveryAborted();
                } else {
                    schedulingListener.onRecoveryFailure(
                        new RecoveryFailedException(recoveryState, null, new RuntimeException("test recovery task injected failure")),
                        randomBoolean()
                    );
                }
            }
        });
    }

    private enum ExpectedRecoveryOutcome {
        COMPLETED,
        CANCELLED_IN_QUEUE,
        CANCELLED_STARTED,
        FAILED,
        ABORTED
    }

    private void ensureListenersWereNotified(TestCaptureResultListener... listeners) {
        assertTrue("all listeners should have been notified", Arrays.stream(listeners).allMatch(TestCaptureResultListener::wasNotified));
    }

    private static class TestCaptureResultListener extends SubscribableListener<Void> implements RecoveryListener {
        private final ExpectedRecoveryOutcome expectedOutcome;
        private volatile boolean notified;

        TestCaptureResultListener(ExpectedRecoveryOutcome expectedOutcome) {
            this.expectedOutcome = expectedOutcome;
        }

        @Override
        public void onRecoveryDone(
            RecoveryState state,
            ShardLongFieldRange timestampMillisFieldRange,
            ShardLongFieldRange eventIngestedMillisFieldRange
        ) {
            assert super.isDone() == false;
            switch (expectedOutcome) {
                case COMPLETED -> super.onResponse(null);
                case ABORTED, CANCELLED_IN_QUEUE, CANCELLED_STARTED, FAILED -> fail(
                    "unexpected recovery success, expected outcome: " + expectedOutcome
                );
            }
        }

        @Override
        public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
            assert super.isDone() == false;
            switch (expectedOutcome) {
                case FAILED -> super.onResponse(null);
                case CANCELLED_IN_QUEUE, CANCELLED_STARTED -> {
                    assert expectedOutcome == ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE || sendShardFailure
                        : "should notify the master solely when cancelling started recoveries";
                    if (e instanceof RecoveryCancelledException == false) {
                        throw new AssertionError("unexpected failure type", e);
                    }
                    super.onResponse(null);
                }
                case ABORTED, COMPLETED -> fail(
                    new AssertionError("unexpected recovery cancellation, expected outcome: " + expectedOutcome, e)
                );
            }
        }

        @Override
        public void onRecoveryAborted() {
            assert super.isDone() == false;
            switch (expectedOutcome) {
                case ABORTED -> super.onResponse(null);
                case COMPLETED, CANCELLED_IN_QUEUE, CANCELLED_STARTED, FAILED -> fail(
                    "unexpected recovery abortion, expected outcome: " + expectedOutcome
                );
            }
        }

        public boolean wasNotified() {
            return super.isDone();
        }
    }

    private static ClusterService newClusterService(int maxConcurrentRecoveries) {
        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrentRecoveries)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create("local-node"));
        return clusterService;
    }

    private static RecoveryState newRecoveryState() {
        return newRecoveryState(
            randomFrom(RecoverySource.Type.values()),
            new ShardId(randomIndexName(), IndexMetadata.INDEX_UUID_NA_VALUE, 1)
        );
    }

    private static RecoveryState newRecoveryState(ShardId shardId) {
        return newRecoveryState(randomFrom(RecoverySource.Type.values()), shardId);
    }

    private static RecoveryState newRecoveryState(RecoverySource.Type type, ShardId shardId) {
        final var routing = TestShardRouting.newShardRouting(
            shardId,
            "node",
            type.equals(RecoverySource.Type.PEER) == false,
            ShardRoutingState.INITIALIZING,
            switch (type) {
                case EMPTY_STORE -> RecoverySource.EmptyStoreRecoverySource.INSTANCE;
                case EXISTING_STORE -> RecoverySource.ExistingStoreRecoverySource.INSTANCE;
                case PEER -> RecoverySource.PeerRecoverySource.INSTANCE;
                case SNAPSHOT -> new RecoverySource.SnapshotRecoverySource(
                    randomUUID(),
                    new Snapshot(ProjectId.DEFAULT, randomRepoName(), new SnapshotId(randomSnapshotName(), randomUUID())),
                    IndexVersion.current(),
                    new IndexId(shardId.getIndexName(), randomUUID())
                );
                case LOCAL_SHARDS -> RecoverySource.LocalShardsRecoverySource.INSTANCE;
                case RESHARD_SPLIT -> new RecoverySource.ReshardSplitRecoverySource(
                    new ShardId(shardId.getIndexName(), IndexMetadata.INDEX_UUID_NA_VALUE, 0)
                );
            }
        );
        return new RecoveryState(routing, DiscoveryNodeUtils.create("source"), DiscoveryNodeUtils.create("target"));
    }
}
