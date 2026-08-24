/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.DelayRecoveryException;
import org.elasticsearch.indices.recovery.RecoverySchedulingListener;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.recovery.StatelessPrimaryRelocationSourceService.ThrottledPrimaryRelocations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/// Unit tests for the [ThrottledPrimaryRelocations] recovery throttle inside [StatelessPrimaryRelocationSourceService].
public class ThrottledPrimaryRelocationsTests extends ESTestCase {

    private static final StartRelocationResponse EMPTY_START_RELOCATION_RESPONSE = new StartRelocationResponse(
        new RelocationSourceMetrics(0, 0, 0, 0)
    );

    public void testStartsImmediatelyWhenSlotAvailable() {
        final var taskQueue = new DeterministicTaskQueue();

        final var throttle = new ThrottledPrimaryRelocations(
            taskQueue::scheduleNow,
            (task, request, shard, listener) -> listener.onResponse(EMPTY_START_RELOCATION_RESPONSE)
        );
        throttle.registerRecoverySchedulingListeners(new CompositeRecoverySchedulingListener());
        throttle.updateMaxConcurrentOutgoingRelocations(Integer.MAX_VALUE);

        final var shardId = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shard = mockShard(shardId);
        final var result = new AtomicReference<StartRelocationResponse>();

        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId),
            createTask(),
            shard,
            ActionListener.wrap(result::set, e -> fail("unexpected relocation failure: " + e))
        );

        // relocation started synchronously and dispatched to executor
        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(shard.recoveryStats().currentAsSource(), equalTo(1));

        taskQueue.runAllRunnableTasks();

        assertNotNull(result.get());
        assertThat(throttle.activeRelocationCount(), equalTo(0));
        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertThat(shard.recoveryStats().currentAsSource(), equalTo(0));
        assertTrue(shard.recoveryStats().noCurrentRecoveries());
    }

    public void testQueuesWhenSlotFull() {
        final var taskQueue = new DeterministicTaskQueue();
        final var completed = new AtomicInteger();
        final var throttle = new ThrottledPrimaryRelocations(
            taskQueue::scheduleNow,
            (task, request, shard, listener) -> completed.incrementAndGet()
        );
        throttle.registerRecoverySchedulingListeners(new CompositeRecoverySchedulingListener());
        throttle.updateMaxConcurrentOutgoingRelocations(1);

        final var shardId1 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shardId2 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shard1 = mockShard(shardId1);
        final var shard2 = mockShard(shardId2);

        // first relocation takes the slot.
        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId1),
            createTask(),
            shard1,
            ActionListener.noop()
        );

        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertThat(shard1.recoveryStats().currentAsSource(), equalTo(1));
        assertThat(shard1.recoveryStats().currentAsSourceQueued(), equalTo(0));

        // second relocation gets queued.
        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId2),
            createTask(),
            shard2,
            ActionListener.noop()
        );
        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(1));
        assertThat(shard2.recoveryStats().currentAsSourceQueued(), equalTo(1));
        assertThat(shard2.recoveryStats().currentAsSource(), equalTo(0));

        taskQueue.runAllRunnableTasks();
        assertThat(completed.get(), equalTo(1));
        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(1));
    }

    public void testQueuedRelocationStartsOnCompletion() {
        final var taskQueue = new DeterministicTaskQueue();
        final var capturedListener = new AtomicReference<ActionListener<StartRelocationResponse>>();
        final var throttle = new ThrottledPrimaryRelocations(
            taskQueue::scheduleNow,
            (task, request, shard, listener) -> capturedListener.set(listener)
        );
        throttle.registerRecoverySchedulingListeners(new CompositeRecoverySchedulingListener());
        throttle.updateMaxConcurrentOutgoingRelocations(1);

        final var shardId1 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shardId2 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shard1 = mockShard(shardId1);
        final var shard2 = mockShard(shardId2);

        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId1),
            createTask(),
            shard1,
            ActionListener.noop()
        );
        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId2),
            createTask(),
            shard2,
            ActionListener.noop()
        );

        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(1));
        assertThat(shard1.recoveryStats().currentAsSource(), equalTo(1));
        assertThat(shard2.recoveryStats().currentAsSourceQueued(), equalTo(1));

        // shard1 relocation runs, shard2 stays queued
        taskQueue.runAllRunnableTasks();
        assertNotNull(capturedListener.get());

        // Complete shard1 relocation, shard2 is dequeued and its work dispatched to the executor.
        capturedListener.get().onResponse(EMPTY_START_RELOCATION_RESPONSE);

        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertTrue(shard1.recoveryStats().noCurrentRecoveries());
        assertThat(shard2.recoveryStats().currentAsSourceQueued(), equalTo(0));
        assertThat(shard2.recoveryStats().currentAsSource(), equalTo(1));

        capturedListener.set(null);

        // shard2 relocation runs
        taskQueue.runAllRunnableTasks();
        assertNotNull(capturedListener.get());

        capturedListener.get().onResponse(EMPTY_START_RELOCATION_RESPONSE);

        assertThat(throttle.activeRelocationCount(), equalTo(0));
        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertTrue(shard2.recoveryStats().noCurrentRecoveries());
    }

    public void testQueuedRelocationStartsOnFailure() {
        final var taskQueue = new DeterministicTaskQueue();
        final var capturedListener = new AtomicReference<ActionListener<StartRelocationResponse>>();
        final var throttle = new ThrottledPrimaryRelocations(
            taskQueue::scheduleNow,
            (task, request, shard, listener) -> capturedListener.set(listener)
        );
        throttle.registerRecoverySchedulingListeners(new CompositeRecoverySchedulingListener());
        throttle.updateMaxConcurrentOutgoingRelocations(1);

        final var shardId1 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shardId2 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shard1 = mockShard(shardId1);
        final var shard2 = mockShard(shardId2);

        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId1),
            createTask(),
            shard1,
            ActionListener.wrap(ignored -> fail("expected failure"), ignored -> {})
        );
        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId2),
            createTask(),
            shard2,
            ActionListener.noop()
        );

        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(1));

        // shard1 relocation runs, shard2 stays queued
        taskQueue.runAllRunnableTasks();
        assertNotNull(capturedListener.get());

        // Fail the first relocation.
        capturedListener.get().onFailure(new RuntimeException("simulated failure"));
        assertTrue(shard1.recoveryStats().noCurrentRecoveries());

        // The second should have been dequeued and dispatched to the executor.
        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertThat(shard2.recoveryStats().currentAsSource(), equalTo(1));

        capturedListener.set(null);
        taskQueue.runAllRunnableTasks();
        assertNotNull(capturedListener.get());
    }

    public void testMultipleSlotsAllowParallelRelocations() {
        final var taskQueue = new DeterministicTaskQueue();
        final var listeners = new CompositeRecoverySchedulingListener();
        final var completed = new AtomicInteger();
        final var throttle = new ThrottledPrimaryRelocations(
            taskQueue::scheduleNow,
            (task, request, shard, listener) -> completed.incrementAndGet()
        );
        throttle.registerRecoverySchedulingListeners(listeners);
        throttle.updateMaxConcurrentOutgoingRelocations(3);

        for (int i = 0; i < 3; i++) {
            final var shardId = new ShardId(randomIndexName(), randomUUID(), 0);
            final var shard = mockShard(shardId);
            throttle.enqueueRelocation(
                createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId),
                createTask(),
                shard,
                ActionListener.noop()
            );
        }

        assertThat(throttle.activeRelocationCount(), equalTo(3));
        assertThat(throttle.queuedRelocationCount(), equalTo(0));

        taskQueue.runAllRunnableTasks();
        assertThat(completed.get(), equalTo(3));
    }

    public void testLimitIncreaseDrainsQueue() {
        final var taskQueue = new DeterministicTaskQueue();
        final var completed = new AtomicInteger();
        final var throttle = new ThrottledPrimaryRelocations(
            taskQueue::scheduleNow,
            (task, request, shard, listener) -> completed.incrementAndGet()
        );
        throttle.registerRecoverySchedulingListeners(new CompositeRecoverySchedulingListener());
        throttle.updateMaxConcurrentOutgoingRelocations(1);

        for (int i = 0; i < 3; i++) {
            final var shardId = new ShardId(randomIndexName(), randomUUID(), 0);
            throttle.enqueueRelocation(
                createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId),
                createTask(),
                mockShard(shardId),
                ActionListener.noop()
            );
        }

        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(2));

        // Raising the limit forks the drain onto the executor; state is unchanged until it runs.
        throttle.updateMaxConcurrentOutgoingRelocations(3);

        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(throttle.queuedRelocationCount(), equalTo(2));
        assertThat(completed.get(), equalTo(0));

        taskQueue.runAllRunnableTasks();

        assertThat(throttle.activeRelocationCount(), equalTo(3));
        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertThat(completed.get(), equalTo(3));
    }

    public void testCancelPendingRelocationsWithTargetNode() {
        final var taskQueue = new DeterministicTaskQueue();
        final var listeners = new CompositeRecoverySchedulingListener();
        final var discarded = new AtomicInteger();
        listeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onQueuedPeerRecoveryDiscardedOnSource() {
                discarded.incrementAndGet();
            }
        });
        final var throttle = new ThrottledPrimaryRelocations(taskQueue::scheduleNow, (task, request, shard, listener) -> {});
        throttle.registerRecoverySchedulingListeners(listeners);
        throttle.updateMaxConcurrentOutgoingRelocations(1);

        final var shardId1 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shardId2 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shard1 = mockShard(shardId1);
        final var shard2 = mockShard(shardId2);
        final var targetNode2 = DiscoveryNodeUtils.create(randomIdentifier());

        // Fill the slot with the first relocation.
        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId1),
            createTask(),
            shard1,
            ActionListener.noop()
        );

        // Queue a relocation targeting targetNode2.
        final var failures = new ArrayList<Exception>();
        throttle.enqueueRelocation(
            createStartRelocationRequest(targetNode2, shardId2),
            createTask(),
            shard2,
            ActionListener.wrap(ignored -> fail("expected failure"), failures::add)
        );

        // Queue another, targeting another node.
        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId1),
            createTask(),
            shard1,
            ActionListener.<StartRelocationResponse>noop()
                .delegateResponse((ignored, e) -> fail("relocation with unrelated target node should not have been cancelled: " + e))
        );

        assertThat(throttle.queuedRelocationCount(), equalTo(2));

        // targetNode2 departs
        throttle.cancelPendingRelocationsWithTargetNode(targetNode2);

        assertThat(throttle.queuedRelocationCount(), equalTo(1));
        assertThat(failures, hasSize(1));
        assertThat(failures.getFirst(), instanceOf(DelayRecoveryException.class));
        assertThat(discarded.get(), equalTo(1));
        assertThat(shard2.recoveryStats().currentAsSourceQueued(), equalTo(0));
        assertTrue(shard2.recoveryStats().noCurrentRecoveries());

        // The active first relocation is also unaffected.
        assertThat(throttle.activeRelocationCount(), equalTo(1));
        assertThat(shard1.recoveryStats().currentAsSource(), equalTo(1));
    }

    public void testCancelAllPendingRelocations() {
        final var taskQueue = new DeterministicTaskQueue();
        final var listeners = new CompositeRecoverySchedulingListener();
        final var discarded = new AtomicInteger();
        listeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onQueuedPeerRecoveryDiscardedOnSource() {
                discarded.incrementAndGet();
            }
        });
        final var throttle = new ThrottledPrimaryRelocations(taskQueue::scheduleNow, (task, request, shard, listener) -> {});
        throttle.registerRecoverySchedulingListeners(listeners);
        throttle.updateMaxConcurrentOutgoingRelocations(1);

        final var shardId1 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shardId2 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shard2 = mockShard(shardId2);
        final var failures = new ArrayList<Exception>();

        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId1),
            createTask(),
            mockShard(shardId1),
            ActionListener.noop()
        );
        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId2),
            createTask(),
            shard2,
            ActionListener.wrap(ignored -> fail("expected failure"), failures::add)
        );

        assertThat(throttle.queuedRelocationCount(), equalTo(1));

        throttle.cancelAllPendingRelocations();

        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertThat(failures, hasSize(1));
        assertThat(failures.get(0), instanceOf(DelayRecoveryException.class));
        assertThat(failures.get(0).getMessage(), containsString("source node is closing"));
        assertThat(discarded.get(), equalTo(1));
        assertTrue(shard2.recoveryStats().noCurrentRecoveries());
        // Active relocation is unaffected.
        assertThat(throttle.activeRelocationCount(), equalTo(1));
    }

    public void testCancelOnShardClosed() {
        final var taskQueue = new DeterministicTaskQueue();
        final var throttle = new ThrottledPrimaryRelocations(taskQueue::scheduleNow, (task, request, shard, listener) -> {});
        throttle.registerRecoverySchedulingListeners(new CompositeRecoverySchedulingListener());
        throttle.updateMaxConcurrentOutgoingRelocations(1);

        final var shardId1 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shardId2 = new ShardId(randomIndexName(), randomUUID(), 0);
        final var shard2 = mockShard(shardId2);
        final var failures = new ArrayList<Exception>();

        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId1),
            createTask(),
            mockShard(shardId1),
            ActionListener.noop()
        );
        throttle.enqueueRelocation(
            createStartRelocationRequest(DiscoveryNodeUtils.create(randomIdentifier()), shardId2),
            createTask(),
            shard2,
            ActionListener.wrap(ignored -> fail("expected failure"), failures::add)
        );

        assertThat(throttle.queuedRelocationCount(), equalTo(1));

        throttle.cancelPendingRelocationsForShard(shard2);

        assertThat(throttle.queuedRelocationCount(), equalTo(0));
        assertThat(failures, hasSize(1));
        final var failure = failures.getFirst();
        assertThat(failure, instanceOf(DelayRecoveryException.class));
        assertThat(failure.getMessage(), containsString("index shard closed"));
        assertTrue(shard2.recoveryStats().noCurrentRecoveries());
        assertThat(throttle.activeRelocationCount(), equalTo(1));
    }

    private static StatelessPrimaryRelocationAction.Request createStartRelocationRequest(DiscoveryNode targetNode, ShardId shardId) {
        return new StatelessPrimaryRelocationAction.Request(randomLong(), shardId, targetNode, randomIdentifier(), 0);
    }

    /// Creates a mock [IndexShard] with a real [RecoveryStats] to leverage counter assertions.
    private static IndexShard mockShard(ShardId shardId) {
        final var shard = mock(IndexShard.class);
        when(shard.recoveryStats()).thenReturn(new RecoveryStats());
        when(shard.shardId()).thenReturn(shardId);
        return shard;
    }

    private static Task createTask() {
        return new Task(randomNonNegativeLong(), "test", "primary-relocation", "", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }
}
