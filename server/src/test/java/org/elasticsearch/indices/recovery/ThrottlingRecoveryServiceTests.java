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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.AbstractProjectResolver;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.Lifecycle;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.indices.recovery.RecoveryGateMonitor.ENABLE_RECOVERY_GATES_SETTING;
import static org.elasticsearch.indices.recovery.RecoveryListener.FailureStrategy.FAIL_SEND;
import static org.elasticsearch.indices.recovery.RecoveryListener.FailureStrategy.FAIL_SILENT;
import static org.elasticsearch.indices.recovery.ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING;
import static org.elasticsearch.indices.recovery.ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThrottlingRecoveryServiceTests extends ESTestCase {
    private static TestThreadPool threadPool;
    private static DiscoveryNode localNode;
    private static DiscoveryNode sourceNode;
    private static DiscoveryNode targetNode;
    private RecoveryStats stats = new RecoveryStats();

    @BeforeClass
    public static void init() throws Exception {
        threadPool = new TestThreadPool(ThrottlingRecoveryServiceTests.class.getSimpleName());
        localNode = DiscoveryNodeUtils.create("local-node");
        sourceNode = DiscoveryNodeUtils.create("source");
        targetNode = DiscoveryNodeUtils.create("target");
    }

    @AfterClass
    public static void close() throws Exception {
        terminate(threadPool);
    }

    @After
    public void verifyNoOutstandingRecoveriesInStats() {
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

        final var service = newStartedService(threadPool, multiProjectResolver, newClusterService(1));

        final var firstRecoveryRunning = new CountDownLatch(1);
        final var firstRecoveryProceed = new CountDownLatch(1);
        final var thirdRecoveryDone = new CountDownLatch(1);

        service.enqueue(
            projectId1,
            RecoveryListener.NOOP,
            newRecoveryState(ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY), // top priority
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            listener -> {
                assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId1.id()));
                firstRecoveryRunning.countDown();
                safeAwait(firstRecoveryProceed);
                listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            }
        );

        safeAwait(firstRecoveryRunning);

        // Test the failure path
        final var secondListener = new RecoveryListener() {
            @Override
            public void onRecoveryDone(RecoveryState state, ShardLongFieldRange t, ShardLongFieldRange e) {
                fail("unexpected success");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId2.id()));
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery aborted");
            }
        };

        service.enqueue(
            projectId2,
            secondListener,
            newRecoveryState(ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED), // second priority, so should happen second
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            ignored -> {
                assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId2.id()));
                throw new RuntimeException("test simulated failure");
            }
        );

        // Test the success path
        final var thirdListener = onRecoveryDoneListener(
            () -> assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId3.id()))
        );
        service.enqueue(
            projectId3,
            thirdListener,
            newRecoveryState(ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED), // third priority, so should happen third
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            listener -> {
                assertThat(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId3.id()));
                listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                thirdRecoveryDone.countDown();
            }
        );

        assertThat(service.currentQueueSize(), equalTo(2));
        firstRecoveryProceed.countDown();
        safeAwait(thirdRecoveryDone);
    }

    public void testSynchronousTaskRunsOnProvidedThreadPoolAndNotifiesUserListener() {
        // Use real threads instead of DeterministicTaskQueue to verify actual threading behavior below
        final var recoveryType = randomFrom(RecoverySource.Type.values());
        final var service = newStartedService(threadPool, DefaultProjectResolver.INSTANCE, newClusterService(1));
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
            newIndexMetadata(),
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
        final var service = newStartedService(threadPool, DefaultProjectResolver.INSTANCE, newClusterService(1));
        final var consumerReturned = new CountDownLatch(1);
        final var recoveryDone = new CountDownLatch(1);
        final var userListener = onRecoveryDoneListener(
            () -> assertThat("terminal callback should follow consumer return", consumerReturned.getCount(), equalTo(0L))
        );
        service.enqueue(
            ProjectId.DEFAULT,
            userListener,
            newRecoveryState(),
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            schedulingListener -> {
                threadPool.generic().execute(() -> {
                    safeAwait(consumerReturned);
                    schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                    recoveryDone.countDown();
                });
                consumerReturned.countDown();
            }
        );
        safeAwait(recoveryDone);
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testMaxConcurrencyBoundWithAsynchronousTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final int maxConcurrentRecoveries = between(2, 5);
        Settings.Builder settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrentRecoveries);
        if (randomBoolean()) {
            // Set INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING to a value equal to or greater than
            // INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, which has no effect:
            settings.put(
                INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING.getKey(),
                randomIntBetween(maxConcurrentRecoveries, Integer.MAX_VALUE)
            );
        }
        final var service = newStartedService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(settings.build())
        );
        final var running = new AtomicInteger();
        final var completed = new AtomicInteger();
        final var peakConcurrent = new AtomicInteger();
        final int totalEnqueuedTasks = maxConcurrentRecoveries * 3;

        RecoveryListener trackingListener = onRecoveryDoneListener(() -> {
            running.decrementAndGet();
            completed.incrementAndGet();
        });

        long initialTime = taskQueue.getCurrentTimeMillis();
        AtomicInteger ordinal = new AtomicInteger(0);
        for (int i = 0; i < totalEnqueuedTasks; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                trackingListener,
                newRecoveryState(),
                newIndexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                schedulingListener -> {
                    int current = running.incrementAndGet();
                    peakConcurrent.accumulateAndGet(current, Integer::max);
                    // Schedule completion for the future, with a scheduled delay matching the execution order of these callbacks:
                    final var minDelay = ((ordinal.getAndIncrement() / maxConcurrentRecoveries) + 1) * 100 + 1;
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

    public void testMaxConcurrentRelocationsSetting() {
        final var taskQueue = new DeterministicTaskQueue();
        final int maxConcurrentRecoveries = between(5, 10);
        // Ensure that there are at least two slots for any recovery and at least two slots for recoveries from unassigned only:
        final int maxConcurrentRelocationRecoveries = between(2, maxConcurrentRecoveries - 2);
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrentRecoveries)
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING.getKey(), maxConcurrentRelocationRecoveries)
            .build();
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(settings));

        Map<String, RecoveryListener> runningRecoveries = new HashMap<>();
        // Enqueue maxConcurrentRelocationRecoveries relocations, and assert that they all start:
        for (int i = 0; i < maxConcurrentRelocationRecoveries; i++) {
            String id = "relocation-" + i;
            service.enqueue(
                ProjectId.DEFAULT,
                noopRecoveryListener(),
                newRelocationRecoveryState(),
                newIndexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                listener -> runningRecoveries.put(id, listener)
            );
        }
        taskQueue.runAllRunnableTasks();
        assertThat(
            runningRecoveries.keySet(),
            equalTo(IntStream.range(0, maxConcurrentRelocationRecoveries).mapToObj(i -> "relocation-" + i).collect(toSet()))
        );

        // Enqueue another relocation, and assert that it does not start (we have reached the limit for relocations):
        service.enqueue(
            ProjectId.DEFAULT,
            noopRecoveryListener(),
            newRelocationRecoveryState(),
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            listener -> runningRecoveries.put("blocked-relocation", listener)
        );
        taskQueue.runAllRunnableTasks();
        assertThat(runningRecoveries.keySet(), not(hasItem("blocked-relocation")));

        // Enqueue recoveries from unassigned up to the limit, and assert that they all start:
        for (int i = 0; i < maxConcurrentRecoveries - maxConcurrentRelocationRecoveries; i++) {
            String id = "unassigned-" + i;
            service.enqueue(
                ProjectId.DEFAULT,
                noopRecoveryListener(),
                newUnassignedRecoveryState(),
                newIndexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                listener -> runningRecoveries.put(id, listener)
            );
        }
        taskQueue.runAllRunnableTasks();
        assertThat(
            runningRecoveries.keySet(),
            equalTo(
                Stream.concat(
                    IntStream.range(0, maxConcurrentRelocationRecoveries).mapToObj(i -> "relocation-" + i),
                    IntStream.range(0, maxConcurrentRecoveries - maxConcurrentRelocationRecoveries).mapToObj(i -> "unassigned-" + i)
                ).collect(toSet())
            )
        );

        // Enqueue another recovery from unassigned, and assert that it does not start (we have reached the overall limit):
        service.enqueue(
            ProjectId.DEFAULT,
            noopRecoveryListener(),
            newUnassignedRecoveryState(),
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            listener -> runningRecoveries.put("blocked-unassigned", listener)
        );
        taskQueue.runAllRunnableTasks();
        assertThat(runningRecoveries.keySet(), not(hasItem("blocked-unassigned")));

        // Complete one of the unassigned recoveries, and assert that the blocked one starts:
        runningRecoveries.remove("unassigned-" + randomIntBetween(0, maxConcurrentRecoveries - maxConcurrentRelocationRecoveries - 1))
            .onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        taskQueue.runAllRunnableTasks();
        assertThat(runningRecoveries.keySet(), hasItem("blocked-unassigned"));

        // Complete another one of the unassigned recoveries, and assert that the blocked relocation does not start (we are still using all
        // the relocation slots):
        runningRecoveries.remove("blocked-unassigned").onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        taskQueue.runAllRunnableTasks();
        assertThat(runningRecoveries.keySet(), not(hasItem("blocked-relocation")));

        // Complete one of the relocations, and assert that the blocked one starts:
        runningRecoveries.remove("relocation-" + randomIntBetween(0, maxConcurrentRelocationRecoveries - 1))
            .onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
        taskQueue.runAllRunnableTasks();
        assertThat(runningRecoveries.keySet(), hasItem("blocked-relocation"));

        // The queue is now empty, just complete all the remaining recoveries to clean up:
        runningRecoveries.values().forEach(listener -> listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY));
    }

    public void testIncreasingMaxConcurrentRecoveriesStartsPendingTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(2);
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, clusterService);
        final var started = new AtomicInteger();

        for (int i = 0; i < 10; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED),
                newRecoveryState(),
                newIndexMetadata(),
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

    public void testIncreasingMaxConcurrentRelocationRecoveriesStartsPendingTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING.getKey(), 2)
            .build();
        final var clusterService = newClusterService(settings);
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, clusterService);
        final var started = new AtomicInteger();

        for (int i = 0; i < 10; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED),
                newRelocationRecoveryState(),
                newIndexMetadata(),
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
            .applySettings(Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING.getKey(), 4).build());
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(4));
        taskQueue.runAllTasks();
        assertThat(started.get(), equalTo(10));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testDecreasingMaxConcurrentRecoveriesDefersQueueWithoutCancellingRunningTasks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(3);
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, clusterService);
        final var started = new AtomicInteger();
        final var done = new AtomicInteger();

        // Delay completion until we explicitly trigger time jump
        final long initialTime = taskQueue.getCurrentTimeMillis();
        AtomicInteger ordinal = new AtomicInteger(0);
        for (int i = 0; i < 6; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                onRecoveryDoneListener(done::incrementAndGet),
                newRecoveryState(),
                newIndexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                schedulingListener -> {
                    started.incrementAndGet();
                    taskQueue.scheduleAt(
                        initialTime + 100 + ordinal.getAndIncrement(),
                        () -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
                    );
                }
            );
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

    public void testExecutionInPriorityOrderWhenThrottledToOneConcurrentWithSynchronousCompletion() {
        // Use a deterministic task queue, so no async actions are executed until runAllRunnableTasks() is called, after the recoveries are
        // all enqueued, and then do one recovery at a time, so that we expect all recoveries to complete in priority order:
        final var taskQueue = new DeterministicTaskQueue();
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(1));

        record TestRecovery(RecoveryState recoveryState, IndexMetadata indexMetadata) {
            @Override
            public String toString() {
                // Give Index rather than IndexMetadata since the latter doesn't have a useful toString():
                return Strings.format("{recoveryState=%s, index=%s}", recoveryState, indexMetadata.getIndex());
            }
        }

        // Construct a list of TestRecovery instances in the order we expect them to be executed:
        int highIndexPriority = randomIntBetween(10, 100);
        int lowIndexPriority = randomIntBetween(5, highIndexPriority - 1);
        long newCreationDate = randomMillisUpToYear9999();
        long oldCreationDate = randomLongBetween(0, newCreationDate - 1);
        // The first level of ordering is by RecoveryPriority:
        List<TestRecovery> orderedRecoveries = Stream.of(ShardRouting.RecoveryPriority.values())
            // Exclude UNKNOWN as we shouldn't ever see that in the cluster state:
            .filter(pri -> pri != ShardRouting.RecoveryPriority.UNKNOWN)
            .flatMap(
                pri -> Stream.of(
                    // Within a RecoveryPriority, ordering is according to PriorityComparator:
                    indexMetadataBuilder("index-0001-system").system(true).priority(lowIndexPriority).creationDate(oldCreationDate).build(),
                    indexMetadataBuilder("index-0001-high-priority").priority(highIndexPriority).creationDate(oldCreationDate).build(),
                    indexMetadataBuilder("index-0001-new-creation-date").priority(lowIndexPriority).creationDate(newCreationDate).build(),
                    indexMetadataBuilder("index-0002-new-by-name").priority(lowIndexPriority).creationDate(oldCreationDate).build(),
                    indexMetadataBuilder("index-0001-last").priority(lowIndexPriority).creationDate(oldCreationDate).build()
                ).map(idx -> new TestRecovery(newRecoveryState(pri), idx))
            )
            .toList();

        // Make a shuffled copy of the ordered list of TestRecovery instances:
        List<TestRecovery> shuffledRecoveries = new ArrayList<>(orderedRecoveries);
        Collections.shuffle(shuffledRecoveries, random());

        // Enqueue the shuffled TestRecovery instances and collect the order in which they are completed:
        List<TestRecovery> completedRecoveries = new CopyOnWriteArrayList<>();
        for (TestRecovery record : shuffledRecoveries) {
            RecoveryListener userListener = onRecoveryDoneListener(() -> completedRecoveries.add(record));
            service.enqueue(
                ProjectId.DEFAULT,
                userListener,
                record.recoveryState(),
                record.indexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                schedulingListener -> schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            );
        }

        taskQueue.runAllRunnableTasks();
        // Assert that the completion order matches the expected order (using List equality, which respects ordering):
        assertThat(completedRecoveries, equalTo(orderedRecoveries));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testFailureTriggersNextQueuedRecovery() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(1));

        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.FAILED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener1,
            newRecoveryState(ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY), // high priority
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            ignored -> { throw new RuntimeException("test recovery task injected failure"); }
        );

        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener2,
            newRecoveryState(ShardRouting.RecoveryPriority.RELOCATE_REBALANCING), // low priority, so previous recovery should happen first
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            schedulingListener -> {
                assertTrue("first task should have completed before second one started", listener1.wasNotified());
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            }
        );

        taskQueue.runAllRunnableTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener1, listener2);
    }

    public void testRecoveryAbortedTriggersNextQueuedRecovery() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(1));

        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.ABORTED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener1,
            newRecoveryState(ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY), // high priority
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            RecoveryListener::onRecoveryAborted
        );
        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener2,
            newRecoveryState(ShardRouting.RecoveryPriority.RELOCATE_REBALANCING), // low priority, so previous recovery should happen first
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            schedulingListener -> {
                assertTrue("first task should have completed before second one started", listener1.wasNotified());
                schedulingListener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
            }
        );

        taskQueue.runAllRunnableTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener1, listener2);
    }

    public void testCloseAbortsQueuedButNotDispatchedRecoveries() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(1));

        final var runningTaskDispatched = new AtomicBoolean();
        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener1,
            newRecoveryState(ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY), // high priority
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            listener -> {
                runningTaskDispatched.set(true);
                taskQueue.scheduleAt(
                    taskQueue.getCurrentTimeMillis() + 1000,
                    () -> listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
                );
            }
        );
        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.ABORTED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener2,
            newRecoveryState(ShardRouting.RecoveryPriority.RELOCATE_REBALANCING), // low priority, so previous recovery should happen first
            newIndexMetadata(),
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
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(1));
        service.close();

        final var listener = new TestCaptureResultListener(ExpectedRecoveryOutcome.ABORTED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener,
            newRecoveryState(),
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            ignored -> fail("should not be dispatched after close")
        );
        ensureListenersWereNotified(listener);
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testRecordedCancellationAppliedAtEnqueueTime() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(10));
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        assertTrue(service.cancelRecoveries(Map.of(allocationId, shardId)).isEmpty());

        final var listener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            listener,
            newRecoveryState(shardId),
            newIndexMetadata(),
            allocationId,
            stats,
            ignored -> fail("task should have been cancelled")
        );
        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener);
    }

    /// A recorded cancellation must persist across multiple [ThrottlingRecoveryService#enqueue] attempts for the same
    /// allocation ID, until pruned by [ThrottlingRecoveryService#clusterChanged].
    public void testRecordedCancellationPersistsForSubsequentEnqueueAttempts() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(10));
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        assertTrue(service.cancelRecoveries(Map.of(allocationId, shardId)).isEmpty());

        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            listener1,
            newRecoveryState(shardId, ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY), // high priority
            newIndexMetadata(),
            allocationId,
            stats,
            ignored -> fail("first enqueue attempt should have been rejected due to recorded cancellation")
        );

        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            listener2,
            newRecoveryState(shardId, ShardRouting.RecoveryPriority.RELOCATE_REBALANCING), // low priority, so previous should happen first
            newIndexMetadata(),
            allocationId,
            stats,
            ignored -> fail("second enqueue attempt should also have been rejected")
        );

        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener1, listener2);
    }

    public void testCancellationAppliedWhenTaskInPendingQueue() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(1));
        final var shardId1 = new ShardId("index1", UUIDs.randomBase64UUID(), 0);
        final var allocationId1 = UUIDs.randomBase64UUID();
        final var shardId2 = new ShardId("index2", UUIDs.randomBase64UUID(), 0);
        final var allocationId2 = UUIDs.randomBase64UUID();

        final var listener1 = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_STARTED);
        service.enqueue(
            ProjectId.DEFAULT,
            listener1,
            newRecoveryState(shardId1),
            newIndexMetadata(),
            allocationId1,
            new RecoveryStats(),
            listener -> {
                // simulates cancellation of started recovery
                taskQueue.scheduleAt(
                    taskQueue.getCurrentTimeMillis() + 100,
                    () -> listener.onRecoveryFailure(new RecoveryCancelledException(shardId1, null, null), FAIL_SEND)
                );
            }
        );
        taskQueue.runAllRunnableTasks();

        final var listener2 = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            listener2,
            newRecoveryState(shardId2),
            newIndexMetadata(),
            allocationId2,
            stats,
            ignored -> fail("task should have been cancelled")
        );
        assertThat(service.cancelRecoveries(Map.of(allocationId1, shardId1, allocationId2, shardId2)), equalTo(Set.of(allocationId2)));
        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(listener1, listener2);
    }

    public void testStaleRecordedEntryRemovedOnClusterStateChangeWithShardRelocated() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(10);
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, clusterService);
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

        final var staleRecoveryState = newRecoveryState(
            staleShardId,
            ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY // high priority
        );
        final var staleListener = new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED);
        service.enqueue(
            ProjectId.DEFAULT,
            staleListener,
            staleRecoveryState,
            newIndexMetadata(),
            staleAllocationId,
            stats,
            l -> l.onRecoveryDone(staleRecoveryState, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
        );

        final var retainedListener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE);
        service.enqueue(
            ProjectId.DEFAULT,
            retainedListener,
            newRecoveryState(retainedShardId, ShardRouting.RecoveryPriority.RELOCATE_REBALANCING), // low priority
            newIndexMetadata(),
            retainedAllocationId,
            stats,
            ignored -> fail("task should have been cancelled")
        );

        taskQueue.runAllTasks();
        assertThat(service.currentQueueSize(), equalTo(0));
        ensureListenersWereNotified(staleListener, retainedListener);
    }

    public void testPendingRecoveryDiscardedWhenAllocationIdChangesWhileQueued() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterService = newClusterService(1);
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, clusterService);

        final var blockerShardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var blockerListener = new TestCaptureResultListener(ExpectedRecoveryOutcome.CANCELLED_STARTED);
        service.enqueue(
            ProjectId.DEFAULT,
            blockerListener,
            newRecoveryState(blockerShardId),
            newIndexMetadata(),
            UUIDs.randomBase64UUID(),
            stats,
            listener -> {
                // occupies the sole concurrency slot
                taskQueue.scheduleAt(
                    taskQueue.getCurrentTimeMillis() + 100,
                    () -> listener.onRecoveryFailure(new RecoveryCancelledException(blockerShardId, null, null), FAIL_SEND)
                );
            }
        );
        taskQueue.runAllRunnableTasks();

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
            newIndexMetadata(),
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
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, newClusterService(10));
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();
        final var recoveryState = newRecoveryState(shardId);

        service.enqueue(
            ProjectId.DEFAULT,
            new TestCaptureResultListener(ExpectedRecoveryOutcome.COMPLETED),
            recoveryState,
            newIndexMetadata(),
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
    /// alternating bursty submits and completion periods, and randomly changing the max concurrent limits
    /// (both the overall limit and the relocation-specific limit).
    /// Verify that all tasks finish and that concurrent execution never exceeds the limit applied.
    public void testStressConcurrentEnqueueMaintainsBoundsAndCompleteness() {
        final var taskQueue = new DeterministicTaskQueue();
        taskQueue.setExecutionDelayVariabilityMillis(100);

        final var maxConcurrentRecoveries = new AtomicInteger(between(1, 20));
        final var maxConcurrentRelocations = new AtomicInteger(between(1, 20));
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrentRecoveries.get())
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING.getKey(), maxConcurrentRelocations.get())
            .build();
        final var clusterService = newClusterService(settings);
        final var service = newStartedService(taskQueue.getThreadPool(), DefaultProjectResolver.INSTANCE, clusterService);

        final var runningRecoveries = new AtomicInteger();
        final var runningRelocations = new AtomicInteger();
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
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                completed.incrementAndGet();
            }

            @Override
            public void onRecoveryAborted() {
                completed.incrementAndGet();
            }
        };

        for (int iteration = 0; iteration < 20; iteration++) {
            maxConcurrentRecoveries.set(randomBoolean() ? between(1, 50) : Integer.MAX_VALUE);
            maxConcurrentRelocations.set(randomBoolean() ? between(1, 50) : Integer.MAX_VALUE);
            clusterService.getClusterSettings()
                .applySettings(
                    Settings.builder()
                        .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrentRecoveries.get())
                        .put(INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING.getKey(), maxConcurrentRelocations.get())
                        .build()
                );

            final var incomingTasks = randomIntBetween(50, 100);
            totalTaskCount.addAndGet(incomingTasks);
            for (int i = 0; i < incomingTasks; i++) {
                if (iteration > 15 && rarely()) {
                    // idempotent
                    service.close();
                }
                boolean isRelocation = randomBoolean();
                RecoveryState recoveryState = isRelocation ? newRelocationRecoveryState() : newUnassignedRecoveryState();
                taskQueue.scheduleNow(
                    () -> service.enqueue(
                        ProjectId.DEFAULT,
                        trackingListener,
                        recoveryState,
                        newIndexMetadata(),
                        UUIDs.randomBase64UUID(),
                        stats,
                        schedulingListener -> {
                            assertThat(runningRecoveries.incrementAndGet(), lessThanOrEqualTo(maxConcurrentRecoveries.get()));
                            if (isRelocation) {
                                assertThat(runningRelocations.incrementAndGet(), lessThanOrEqualTo(maxConcurrentRelocations.get()));
                            }

                            final var currentTime = taskQueue.getCurrentTimeMillis();
                            taskQueue.scheduleAt(currentTime + randomIntBetween(0, 100), () -> {
                                runningRecoveries.decrementAndGet();
                                if (isRelocation) {
                                    runningRelocations.decrementAndGet();
                                }
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
                                            FAIL_SILENT
                                        );
                                    }
                                }
                            });
                        }
                    )
                );
                taskQueue.runAllRunnableTasks();
                while (randomBoolean() && taskQueue.hasDeferredTasks()) {
                    if (service.lifecycleState() != Lifecycle.State.STARTED) {
                        assertThat(service.currentQueueSize(), equalTo(0));
                    }
                    taskQueue.advanceTime();
                    taskQueue.runAllRunnableTasks();
                }
                if (service.lifecycleState() != Lifecycle.State.STARTED) {
                    assertThat(service.currentQueueSize(), equalTo(0));
                }
            }
            // Execute all enqueued and scheduled tasks
            taskQueue.runAllTasks();
            assertThat(completed.get(), equalTo(totalTaskCount.get()));
            assertThat(runningRecoveries.get(), equalTo(0));
            assertThat(runningRelocations.get(), equalTo(0));
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
        final var throttlingRecoveryService = newStartedService(threadPool, DefaultProjectResolver.INSTANCE, clusterService);

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
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
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
                            newIndexMetadata(),
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
                        randomBoolean() ? FAIL_SEND : FAIL_SILENT
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
        public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
            assert super.isDone() == false;
            switch (expectedOutcome) {
                case FAILED -> super.onResponse(null);
                case CANCELLED_IN_QUEUE, CANCELLED_STARTED -> {
                    assert expectedOutcome == ExpectedRecoveryOutcome.CANCELLED_IN_QUEUE || failureStrategy.notifyMaster()
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

    public void testGateBlocksAllRecoveriesUntilItAllows() {
        final var taskQueue = new DeterministicTaskQueue();
        // A blocking gate holds every recovery back until it flips to run.
        final var gateDecision = new AtomicReference<>(RecoveryGate.Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30)));
        final RecoveryGate gate = gateDecision::get;
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(Integer.MAX_VALUE), // plenty of slots, so only the gate can hold recoveries back
            RecoverySchedulingListener.NOOP,
            new RecoveryGateMonitor(() -> List.of(gate), taskQueue.getThreadPool(), clusterSettingsWithGatesEnabled())
        );
        service.start();

        final var started = new AtomicInteger();
        final int count = between(2, 5);
        for (int i = 0; i < count; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                RecoveryListener.NOOP,
                newRecoveryState(),
                newIndexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                listener -> {
                    started.incrementAndGet();
                    listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                }
            );
        }

        taskQueue.runAllRunnableTasks();
        assertThat("gate should hold every recovery back", started.get(), equalTo(0));
        assertThat(service.currentQueueSize(), equalTo(count));

        // Conditions improve: the periodic recheck notices the gate now allows recoveries and wakes the scheduler.
        gateDecision.set(RecoveryGate.Decision.RUN);
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(count));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testEmptyGateDispatchesImmediately() {
        final var taskQueue = new DeterministicTaskQueue();
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(Integer.MAX_VALUE),
            RecoverySchedulingListener.NOOP,
            monitorWithNoGates(taskQueue.getThreadPool())
        );
        service.start();
        final var started = new AtomicInteger();
        final int count = between(1, 100);
        for (int i = 0; i < count; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                RecoveryListener.NOOP,
                newRecoveryState(),
                newIndexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                listener -> {
                    started.incrementAndGet();
                    listener.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                }
            );
        }
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(count));
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    public void testGateBlockedTimeIsReported() {
        final var taskQueue = new DeterministicTaskQueue();

        final var blockedGate = new AtomicReference<String>();
        final var blockedCount = new AtomicInteger();
        final var unblockedCount = new AtomicInteger();
        final var reportedBlockedMillis = new AtomicLong(-1);
        final RecoverySchedulingListener listener = new RecoverySchedulingListener() {
            @Override
            public void onRecoveriesBlocked(String gateName) {
                blockedGate.set(gateName);
                blockedCount.incrementAndGet();
            }

            @Override
            public void onRecoveriesUnblocked(long blockedTimeMillis) {
                unblockedCount.incrementAndGet();
                reportedBlockedMillis.set(blockedTimeMillis);
            }
        };
        final String gateName = randomIdentifier();
        final var gateDecision = new AtomicReference<>(RecoveryGate.Decision.block(gateName, randomAlphaOfLengthBetween(5, 30)));
        final RecoveryGate gate = gateDecision::get;
        final var recoveryGateMonitor = new RecoveryGateMonitor(
            () -> List.of(gate),
            taskQueue.getThreadPool(),
            clusterSettingsWithGatesEnabled()
        );
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(Integer.MAX_VALUE), // plenty of slots, so only the gate can hold recoveries back
            listener,
            recoveryGateMonitor
        );
        service.start();

        final long blockedSince = taskQueue.getCurrentTimeMillis();
        final var started = new AtomicInteger();
        final int count = between(1, 100);
        for (int i = 0; i < count; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                RecoveryListener.NOOP,
                newRecoveryState(),
                newIndexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                l -> {
                    started.incrementAndGet();
                    l.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                }
            );
        }
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(0));
        // The block is reported exactly once (on the first dispatch attempt), naming the responsible gate.
        assertThat(blockedCount.get(), equalTo(1));
        assertThat(blockedGate.get(), equalTo(gateName));
        assertThat(unblockedCount.get(), equalTo(0));

        // Stay blocked across a few periodic rechecks: nothing new is reported.
        for (int i = between(0, 3); i > 0; i--) {
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }
        assertThat(blockedCount.get(), equalTo(1));
        assertThat(unblockedCount.get(), equalTo(0));
        assertTrue("Rechecks task should exists", taskQueue.hasDeferredTasks());

        // The gate allows recoveries again: the next recheck notices, dispatches everything, and reports the blocked duration.
        gateDecision.set(RecoveryGate.Decision.RUN);
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertThat(started.get(), equalTo(count));
        assertThat(unblockedCount.get(), equalTo(1));
        assertThat(reportedBlockedMillis.get(), equalTo(taskQueue.getCurrentTimeMillis() - blockedSince));
        assertFalse("No more scheduled tasks", taskQueue.hasAnyTasks());
    }

    /// The gating escape hatch: dynamically disabling the recovery gates must release recoveries held by a gate that never
    /// unblocks by itself, via the next periodic recheck.
    public void testDisablingGatesReleasesBlockedRecoveries() {
        final var taskQueue = new DeterministicTaskQueue();

        final var unblockedCount = new AtomicInteger();
        final var reportedBlockedMillis = new AtomicLong(-1);
        final RecoverySchedulingListener listener = new RecoverySchedulingListener() {
            @Override
            public void onRecoveriesUnblocked(long blockedTimeMillis) {
                unblockedCount.incrementAndGet();
                reportedBlockedMillis.set(blockedTimeMillis);
            }
        };
        final var clusterSettings = clusterSettingsWithGatesEnabled();
        // This gate never unblocks by itself: only disabling the gates can release the held recoveries.
        final RecoveryGate gate = () -> RecoveryGate.Decision.block("stuck", "never unblocks");
        final var service = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            newClusterService(Integer.MAX_VALUE), // plenty of slots, so only the gate can hold recoveries back
            listener,
            new RecoveryGateMonitor(() -> List.of(gate), taskQueue.getThreadPool(), clusterSettings)
        );
        service.start();

        final long blockedSince = taskQueue.getCurrentTimeMillis();
        final var started = new AtomicInteger();
        final int count = between(1, 100);
        for (int i = 0; i < count; i++) {
            service.enqueue(
                ProjectId.DEFAULT,
                RecoveryListener.NOOP,
                newRecoveryState(),
                newIndexMetadata(),
                UUIDs.randomBase64UUID(),
                stats,
                l -> {
                    started.incrementAndGet();
                    l.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                }
            );
        }
        taskQueue.runAllRunnableTasks();
        // Stay blocked across a few periodic rechecks.
        for (int i = between(0, 3); i > 0; i--) {
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }
        assertThat(started.get(), equalTo(0));
        assertThat(unblockedCount.get(), equalTo(0));

        // The next periodic recheck notices the gates are disabled: it dispatches every held recovery, reports the blocked
        // duration and stops rescheduling.
        clusterSettings.applySettings(Settings.builder().put(ENABLE_RECOVERY_GATES_SETTING.getKey(), false).build());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(started.get(), equalTo(count));
        assertThat(unblockedCount.get(), equalTo(1));
        assertThat(reportedBlockedMillis.get(), equalTo(taskQueue.getCurrentTimeMillis() - blockedSince));
        assertFalse("No more scheduled tasks", taskQueue.hasAnyTasks());
    }

    /// Hammers the service from multiple real threads while the gate flaps, to catch races between dispatch, the monitor's
    /// evaluations, and the resume callback: a missed wake-up leaves recoveries queued (the latch below never opens) and a deadlock
    /// hangs the test. Unlike the deterministic tests above, this uses a real thread pool.
    public void testConcurrentEnqueuesWithFlappingGateEventuallyDispatchEverything() throws Exception {
        final var gateDecision = new AtomicReference<>(RecoveryGate.Decision.RUN);
        final RecoveryGate gate = gateDecision::get;
        final var service = new ThrottlingRecoveryService(
            threadPool,
            DefaultProjectResolver.INSTANCE,
            newClusterService(randomBoolean() ? Integer.MAX_VALUE : between(1, 5)),
            RecoverySchedulingListener.NOOP,
            new RecoveryGateMonitor(() -> List.of(gate), threadPool, clusterSettingsWithGatesEnabled())
        );
        service.start();

        final int enqueueThreads = between(4, 10);
        final int recoveriesPerThread = between(20, 100);
        final int totalRecoveries = enqueueThreads * recoveriesPerThread;
        // Test randomness is bound to the main thread, so pre-compute everything random the worker threads need.
        final List<RecoveryState> recoveryStates = new ArrayList<>(totalRecoveries);
        for (int i = 0; i < totalRecoveries; i++) {
            recoveryStates.add(newRecoveryState());
        }
        final Random flapperRandom = new Random(randomLong());

        final var allCompleted = new CountDownLatch(totalRecoveries);
        final var enqueued = new AtomicInteger();
        startInParallel(enqueueThreads + 1, threadIndex -> {
            if (threadIndex == 0) {
                // Flap the gate while the other threads enqueue, then settle on RUN.
                while (enqueued.get() < totalRecoveries) {
                    gateDecision.set(
                        flapperRandom.nextBoolean() ? RecoveryGate.Decision.RUN : RecoveryGate.Decision.block("flapper", "concurrency test")
                    );
                    Thread.yield();
                }
                gateDecision.set(RecoveryGate.Decision.RUN);
            } else {
                for (int i = 0; i < recoveriesPerThread; i++) {
                    final RecoveryState recoveryState = recoveryStates.get((threadIndex - 1) * recoveriesPerThread + i);
                    service.enqueue(
                        ProjectId.DEFAULT,
                        RecoveryListener.NOOP,
                        recoveryState,
                        newIndexMetadata(),
                        UUIDs.randomBase64UUID(),
                        stats,
                        l -> {
                            l.onRecoveryDone(null, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY);
                            allCompleted.countDown();
                        }
                    );
                    enqueued.incrementAndGet();
                }
            }
        });

        // Whatever interleaving happened, once the gate settles on RUN every recovery must dispatch and complete.
        safeAwait(allCompleted);
        assertThat(service.currentQueueSize(), equalTo(0));
    }

    private static ClusterService newClusterService(int maxConcurrentRecoveries) {
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), maxConcurrentRecoveries)
            .build();
        return newClusterService(settings);
    }

    private static ClusterService newClusterService(Settings settings) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.localNode()).thenReturn(localNode);
        return clusterService;
    }

    private static ThrottlingRecoveryService newStartedService(
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        ClusterService clusterService
    ) {
        final var service = new ThrottlingRecoveryService(
            threadPool,
            projectResolver,
            clusterService,
            RecoverySchedulingListener.NOOP,
            monitorWithNoGates(threadPool)
        );
        service.start();
        return service;
    }

    /// A [RecoveryGateMonitor] with no gates: the decision never transitions, so the change listener never fires.
    private static RecoveryGateMonitor monitorWithNoGates(ThreadPool threadPool) {
        return new RecoveryGateMonitor(() -> List.of(), threadPool, ClusterSettings.createBuiltInClusterSettings());
    }

    private static ClusterSettings clusterSettingsWithGatesEnabled() {
        return new ClusterSettings(
            Settings.builder().put(ENABLE_RECOVERY_GATES_SETTING.getKey(), true).build(),
            Set.of(ENABLE_RECOVERY_GATES_SETTING)
        );
    }

    private static RecoveryState newRecoveryState() {
        return newRecoveryState(
            randomFrom(RecoverySource.Type.values()),
            new ShardId(randomIndexName(), IndexMetadata.INDEX_UUID_NA_VALUE, 1)
        );
    }

    private static RecoveryState newRecoveryState(ShardRouting.RecoveryPriority recoveryPriority) {
        return newRecoveryState(new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), randomIntBetween(0, 99)), recoveryPriority);
    }

    private static RecoveryState newUnassignedRecoveryState() {
        return newRecoveryState(
            randomFrom(
                ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY,
                ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED,
                ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
            )
        );
    }

    private static RecoveryState newRelocationRecoveryState() {
        return newRecoveryState(
            randomFrom(
                ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO,
                ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NOT_PREFERRED,
                ShardRouting.RecoveryPriority.RELOCATE_REBALANCING
            )
        );
    }

    private static RecoveryState newRecoveryState(ShardId shardId, ShardRouting.RecoveryPriority recoveryPriority) {
        String relocatingNodeId = switch (recoveryPriority) {
            case UNASSIGNED_NEW_PRIMARY, UNASSIGNED_UNEXPECTED, UNASSIGNED_EXPECTED -> null;
            case RELOCATION_CAN_REMAIN_NO, RELOCATION_CAN_REMAIN_NOT_PREFERRED, RELOCATE_REBALANCING -> "other-node";
            case UNKNOWN -> throw new IllegalArgumentException("cannot create recovery state with unknown recovery priority");
        };
        ShardRouting routing = TestShardRouting.shardRoutingBuilder(shardId, "node", randomBoolean(), ShardRoutingState.INITIALIZING)
            .withRecoveryPriority(recoveryPriority)
            .withRelocatingNodeId(relocatingNodeId)
            .build();
        return new RecoveryState(routing, targetNode, sourceNode);
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
        return new RecoveryState(routing, targetNode, sourceNode);
    }

    private static IndexMetadata newIndexMetadata() {
        return indexMetadataBuilder(randomIndexName()).build();
    }

    private static IndexMetadata.Builder indexMetadataBuilder(String index) {
        return IndexMetadata.builder(index).settings(ESTestCase.settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1);
    }

    private static RecoveryListener onRecoveryDoneListener(Runnable onRecoveryDone) {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                onRecoveryDone.run();
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, FailureStrategy failureStrategy) {
                fail(e, "unexpected recovery failure");
            }

            @Override
            public void onRecoveryAborted() {
                fail("unexpected recovery abort");
            }
        };
    }

    private static RecoveryListener noopRecoveryListener() {
        return onRecoveryDoneListener(() -> {});
    }
}
