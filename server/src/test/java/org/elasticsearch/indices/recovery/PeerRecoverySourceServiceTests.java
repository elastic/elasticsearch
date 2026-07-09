/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.Actions.START_RECOVERY;
import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeerRecoverySourceServiceTests extends IndexShardTestCase {

    public void testQueuedWhenAtConcurrencyLimit() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var task = newRecoveryTask();

        try (
            var service = newPeerRecoverySourceService(2);
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))
        ) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));
            assertThat(primary1.recoveryStats().currentAsSource(), equalTo(1));
            assertThat(primary2.recoveryStats().currentAsSource(), equalTo(1));

            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(primary3.recoveryStats().currentAsSourceQueued(), equalTo(1));

            closeShards(primary1, primary2, primary3);
        }
    }

    public void testQueueProcessesNextQueuedWhenSlotFreed() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);

        final var task = newRecoveryTask();
        // onRecoveryCompleted fires after sourceRecoveryCompleted()
        final var completedListener = new CountDownLatch(2);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                completedListener.countDown();
            }
        });

        try (var service = newPeerRecoverySourceService(1, schedulingListeners); var block1 = blockShardRecovery(primary1)) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            // The recovery will fail immediately because the fake target allocation ID is not in the shard's routing table
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary2),
                task,
                primary2,
                ActionListener.wrap(
                    r -> fail("unexpected success"),
                    exception -> assertThat(exception, instanceOf(DelayRecoveryException.class))
                )
            );
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(1));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));
            assertThat(primary1.recoveryStats().currentAsSource(), equalTo(1));
            assertThat(primary2.recoveryStats().currentAsSourceQueued(), equalTo(1));

            // Releasing primary1's block lets its recovery fail, which frees a slot and starts primary2.
            block1.close();
            safeAwait(completedListener);

            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));
            assertThat(primary2.recoveryStats().currentAsSourceQueued(), equalTo(0));
            assertTrue(primary1.recoveryStats().noCurrentRecoveries());
            assertTrue(primary2.recoveryStats().noCurrentRecoveries());
            closeShards(primary1, primary2);
        }
    }

    public void testSchedulingListenersCalls() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final Queue<String> callOrder = new ConcurrentLinkedQueue<>();
        final var allRecoveriesStarted = new CountDownLatch(2);
        final var task = newRecoveryTask();
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryQueued(RecoverySource.Type type, RecoveryRole role) {
                callOrder.add("queued");
            }

            @Override
            public void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {
                callOrder.add("dequeued");
                allRecoveriesStarted.countDown();
            }
        });

        try (var service = newPeerRecoverySourceService(1, schedulingListeners); var block = blockShardRecovery(primary1)) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            assertThat(new ArrayList<>(callOrder), equalTo(List.of("queued", "dequeued")));
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(1));

            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertThat(new ArrayList<>(callOrder), equalTo(List.of("queued", "dequeued", "queued")));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));
            block.close();
            safeAwait(allRecoveriesStarted);
            closeShards(primary1, primary2);
        }
        assertThat(new ArrayList<>(callOrder), equalTo(List.of("queued", "dequeued", "queued", "dequeued")));
    }

    public void testQueueFifoOrdering() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final IndexShard primary5 = newStartedShard(true);
        final var task = newRecoveryTask();

        final var allRecoveriesCompleted = new CountDownLatch(4);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                allRecoveriesCompleted.countDown();
            }
        });
        final Queue<Integer> callOrder = new ConcurrentLinkedQueue<>();

        try (
            var service = newPeerRecoverySourceService(2, schedulingListeners);
            var block1 = blockShardRecovery(primary1);
            var ignored = blockShardRecovery(primary2)
        ) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary3),
                task,
                primary3,
                ActionListener.wrap(r -> fail("unexpected success"), e -> callOrder.add(3))
            );
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary4),
                task,
                primary4,
                ActionListener.wrap(r -> fail("unexpected success"), e -> callOrder.add(4))
            );
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary5),
                task,
                primary5,
                ActionListener.wrap(r -> fail("unexpected success"), e -> callOrder.add(5))
            );
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(3));
            assertThat(primary3.recoveryStats().currentAsSourceQueued(), equalTo(1));
            assertThat(primary4.recoveryStats().currentAsSourceQueued(), equalTo(1));
            assertThat(primary5.recoveryStats().currentAsSourceQueued(), equalTo(1));

            block1.close();
            safeAwait(allRecoveriesCompleted);
            assertThat(new ArrayList<>(callOrder), equalTo(List.of(3, 4, 5)));
            assertTrue(primary3.recoveryStats().noCurrentRecoveries());
            assertTrue(primary4.recoveryStats().noCurrentRecoveries());
            assertTrue(primary5.recoveryStats().noCurrentRecoveries());
            closeShards(primary1, primary2, primary3, primary4, primary5);
        }
    }

    public void testSameShardFillsMultipleSlots() throws Exception {
        final IndexShard primary = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var task = newRecoveryTask();

        // Two handlers for the same shard each consume one slot.
        try (var service = newPeerRecoverySourceService(2); var ignored = blockShardRecovery(primary)) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary), task, primary, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary), task, primary, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(primary.recoveryStats().currentAsSource(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));

            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));
            assertThat(primary2.recoveryStats().currentAsSourceQueued(), equalTo(1));

            closeShards(primary, primary2);
        }
    }

    public void testQueuedRecoveryCancelledOnShardClose() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var task = newRecoveryTask();

        try (var service = newPeerRecoverySourceService(1); var ignored = blockShardRecovery(primary1)) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());

            // Queue two recoveries for different shards.
            final AtomicReference<Exception> primary2Response = new AtomicReference<>();
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary2),
                task,
                primary2,
                ActionListener.wrap(r -> fail("unexpected success"), primary2Response::set)
            );
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(1));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(2));
            assertThat(primary3.recoveryStats().currentAsSourceQueued(), equalTo(1));

            service.beforeIndexShardClosed(primary2.shardId(), primary2, Settings.EMPTY);
            assertThat(primary2.recoveryStats().currentAsSourceQueued(), equalTo(0));
            assertThat(primary2.recoveryStats().currentAsSource(), equalTo(0));
            assertNotNull(primary2Response.get());
            assertThat(primary2Response.get(), instanceOf(DelayRecoveryException.class));
            assertThat(primary2Response.get().getMessage(), containsString("index shard closed"));

            assertThat(primary3.recoveryStats().currentAsSourceQueued(), equalTo(1));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            closeShards(primary1, primary2, primary3);
        }
    }

    public void testQueuedRecoveryCancelledOnNodeLeft() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var task = newRecoveryTask();

        try (var service = newPeerRecoverySourceService(1); var ignored = blockShardRecovery(primary1)) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());

            // Queue a recovery targeting a specific node.
            final DiscoveryNode departedNode = getFakeDiscoNode("departing");
            final var requestToDepartingNode = new StartRecoveryRequest(
                primary2.shardId(),
                randomAlphaOfLength(10),
                getFakeDiscoNode("source"),
                departedNode,
                0L,
                Store.MetadataSnapshot.EMPTY,
                randomBoolean(),
                randomLong(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                true
            );
            final AtomicReference<Exception> primary2Response = new AtomicReference<>();
            service.ongoingRecoveries.enqueueRecovery(
                requestToDepartingNode,
                task,
                primary2,
                ActionListener.wrap(r -> fail("unexpected success"), primary2Response::set)
            );
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(1));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            // Simulate node departure, the pending entry should fail.
            service.ongoingRecoveries.cancelOnNodeLeft(departedNode);
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));
            assertThat(primary2.recoveryStats().currentAsSourceQueued(), equalTo(0));
            assertNotNull(primary2Response.get());
            assertThat(primary2Response.get(), instanceOf(DelayRecoveryException.class));
            assertThat(primary2Response.get().getMessage(), containsString("target node left"));

            closeShards(primary1, primary2);
        }
    }

    public void testCancelAllPendingRecoveries() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var task = newRecoveryTask();

        try (
            var service = newPeerRecoverySourceService(2);
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))
        ) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

            final AtomicReference<Exception> primary3Response = new AtomicReference<>();
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary3),
                task,
                primary3,
                ActionListener.wrap(r -> fail("unexpected success"), primary3Response::set)
            );
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            service.ongoingRecoveries.cancelAllPendingRecoveries();
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));
            assertThat(primary3.recoveryStats().currentAsSourceQueued(), equalTo(0));
            assertNotNull(primary3Response.get());
            assertThat(primary3Response.get().getMessage(), containsString("node is closing"));

            closeShards(primary1, primary2, primary3);
        }
    }

    public void testDuplicateRejected() throws Exception {
        final IndexShard primary = newStartedShard(true);
        final var task = newRecoveryTask();
        final var request = newStartRecoveryRequest(primary, randomAlphaOfLength(10));
        final var firstComplete = new CountDownLatch(1);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        // onRecoveryCompleted fires after remove() completes inside the synchronized block, so awaiting it
        // guarantees the handler is gone from activeRecoveries.
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                firstComplete.countDown();
            }
        });

        try (var service = newPeerRecoverySourceService(Integer.MAX_VALUE, schedulingListeners)) {
            service.start();

            // Block so the first recovery stays active long enough to test duplicate rejection.
            try (var ignored = blockShardRecovery(primary)) {
                service.ongoingRecoveries.enqueueRecovery(request, task, primary, ActionListener.noop());

                // Same target allocation ID should be rejected while the recovery is active.
                final var duplicate = new StartRecoveryRequest(
                    primary.shardId(),
                    request.targetAllocationId(),
                    getFakeDiscoNode("source"),
                    getFakeDiscoNode("target-dup"),
                    0L,
                    Store.MetadataSnapshot.EMPTY,
                    randomBoolean(),
                    randomLong(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    true
                );
                final var exception = expectThrows(
                    DelayRecoveryException.class,
                    () -> service.ongoingRecoveries.enqueueRecovery(duplicate, task, primary, ActionListener.noop())
                );
                assertThat(exception.getMessage(), containsString("recovery with same target already registered"));
            }

            // Wait for onRecoveryComplete to finish before attempting re-add.
            safeAwait(firstComplete);

            // Re-adding with the same allocation ID succeeds after the previous attempt completes.
            final var secondComplete = new CountDownLatch(1);
            schedulingListeners.addListener(new RecoverySchedulingListener() {
                @Override
                public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                    secondComplete.countDown();
                }
            });
            service.ongoingRecoveries.enqueueRecovery(request, task, primary, ActionListener.noop());
            safeAwait(secondComplete);

            closeShards(primary);
        }
    }

    public void testQueuedDuplicateRejected() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var task = newRecoveryTask();

        try (var service = newPeerRecoverySourceService(1); var ignored = Releasables.wrap(blockShardRecovery(primary1))) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());

            final var queuedRequest = newStartRecoveryRequest(primary2);
            service.ongoingRecoveries.enqueueRecovery(queuedRequest, task, primary2, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(1));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            // Request for the same target allocation ID should be rejected.
            final var duplicateRequest = new StartRecoveryRequest(
                primary2.shardId(),
                queuedRequest.targetAllocationId(),
                getFakeDiscoNode("source"),
                getFakeDiscoNode("target-dup"),
                0L,
                Store.MetadataSnapshot.EMPTY,
                randomBoolean(),
                randomLong(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                true
            );
            final var exception = expectThrows(
                DelayRecoveryException.class,
                () -> service.ongoingRecoveries.enqueueRecovery(duplicateRequest, task, primary2, ActionListener.noop())
            );
            assertThat(exception.getMessage(), containsString("recovery with same target already registered"));

            closeShards(primary1, primary2);
        }
    }

    public void testActiveHandlerDuplicateRejected() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final var task = newRecoveryTask();

        try (
            var service = newPeerRecoverySourceService(Integer.MAX_VALUE);
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))
        ) {
            service.start();
            final var activeRequest = newStartRecoveryRequest(primary1);
            service.ongoingRecoveries.enqueueRecovery(activeRequest, task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

            final var duplicateOfActive = new StartRecoveryRequest(
                primary3.shardId(),
                activeRequest.targetAllocationId(),
                getFakeDiscoNode("source"),
                getFakeDiscoNode("target-dup"),
                0L,
                Store.MetadataSnapshot.EMPTY,
                randomBoolean(),
                randomLong(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                true
            );
            final var exception = expectThrows(
                DelayRecoveryException.class,
                () -> service.ongoingRecoveries.enqueueRecovery(duplicateOfActive, task, primary3, ActionListener.noop())
            );
            assertThat(exception.getMessage(), containsString("recovery with same target already registered"));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));

            closeShards(primary1, primary2, primary3);
        }
    }

    public void testReestablishActiveRecovery() throws Exception {
        final IndexShard primary = newStartedShard(true);
        final var task = newRecoveryTask();
        final var request = newStartRecoveryRequest(primary);

        try (var service = newPeerRecoverySourceService(Integer.MAX_VALUE); var ignored = blockShardRecovery(primary)) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(request, task, primary, ActionListener.noop());

            // Reestablish with the correct recovery ID and allocation ID succeeds.
            final var reestablishRequest = new ReestablishRecoveryRequest(
                request.recoveryId(),
                request.shardId(),
                request.targetAllocationId()
            );
            service.ongoingRecoveries.reestablishRecovery(reestablishRequest, primary, ActionListener.noop());

            // Wrong recovery ID throws ResourceNotFoundException.
            final var wrongIdRequest = new ReestablishRecoveryRequest(
                request.recoveryId() + 1,
                request.shardId(),
                request.targetAllocationId()
            );
            expectThrows(
                ResourceNotFoundException.class,
                () -> service.ongoingRecoveries.reestablishRecovery(wrongIdRequest, primary, ActionListener.noop())
            );

            closeShards(primary);
        }
    }

    public void testReestablishPendingRecovery() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var task = newRecoveryTask();

        try (var service = newPeerRecoverySourceService(1); var ignored = blockShardRecovery(primary1)) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());

            // Capture the original listener to verify it is never called prematurely.
            final var request2 = newStartRecoveryRequest(primary2);
            final var oldListenerResponse = new AtomicReference<Exception>();
            service.ongoingRecoveries.enqueueRecovery(
                request2,
                task,
                primary2,
                ActionListener.wrap(r -> fail("unexpected success"), oldListenerResponse::set)
            );
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));
            assertThat(primary2.recoveryStats().currentAsSourceQueued(), equalTo(1));

            // Reestablish the pending recovery with a fresh listener.
            final var newListenerResponse = new AtomicReference<Exception>();
            final var reestablishRequest = new ReestablishRecoveryRequest(
                request2.recoveryId(),
                request2.shardId(),
                request2.targetAllocationId()
            );
            service.ongoingRecoveries.reestablishRecovery(
                reestablishRequest,
                primary2,
                ActionListener.wrap(r -> fail("unexpected success"), newListenerResponse::set)
            );

            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));
            assertThat(primary2.recoveryStats().currentAsSourceQueued(), equalTo(1));

            assertNull(oldListenerResponse.get());
            assertNull(newListenerResponse.get());

            // Both the old and new listener are notified on completion (here cancellation).
            service.ongoingRecoveries.cancelAllPendingRecoveries();
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));
            assertThat(primary2.recoveryStats().currentAsSourceQueued(), equalTo(0));
            assertNotNull("old listener must have been called on cancel", oldListenerResponse.get());
            assertThat(oldListenerResponse.get(), instanceOf(DelayRecoveryException.class));
            assertThat(oldListenerResponse.get().getMessage(), containsString("node is closing"));
            assertThat(newListenerResponse.get(), instanceOf(DelayRecoveryException.class));
            assertThat(newListenerResponse.get().getMessage(), containsString("node is closing"));

            // Reestablishing when no matching entry exists throws PeerRecoveryNotFound.
            expectThrows(
                PeerRecoveryNotFound.class,
                () -> service.ongoingRecoveries.reestablishRecovery(reestablishRequest, primary2, ActionListener.noop())
            );

            closeShards(primary1, primary2);
        }
    }

    /// Tests when a shard has at least one active recovery AND a separate queued recovery (different allocation ID)
    public void testReestablishSameShardActiveAndQueuedRecovery() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var task = newRecoveryTask();

        try (
            var service = newPeerRecoverySourceService(2);
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))
        ) {
            service.start();
            // Slot 1: primary1 with allocation ID A (active)
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            // Slot 2: primary2 fills the remaining slot
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());

            // Queue a second recovery for primary1 with a different allocation ID.
            final var queuedRequest = newStartRecoveryRequest(primary1);
            final var oldListenerResponse = new AtomicReference<Exception>();
            service.ongoingRecoveries.enqueueRecovery(
                queuedRequest,
                task,
                primary1,
                ActionListener.wrap(r -> fail("unexpected success"), oldListenerResponse::set)
            );
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            final var reestablishRequest = new ReestablishRecoveryRequest(
                queuedRequest.recoveryId(),
                queuedRequest.shardId(),
                queuedRequest.targetAllocationId()
            );
            final var newListenerResponse = new AtomicReference<Exception>();
            service.ongoingRecoveries.reestablishRecovery(
                reestablishRequest,
                primary1,
                ActionListener.wrap(r -> fail("unexpected success"), newListenerResponse::set)
            );
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            // Both the old and new listener are notified on completion (here cancellation).
            service.ongoingRecoveries.cancelAllPendingRecoveries();
            assertNotNull("old listener must be called on cancel", oldListenerResponse.get());
            assertThat(oldListenerResponse.get(), instanceOf(DelayRecoveryException.class));
            assertNotNull("new listener must be called on cancel", newListenerResponse.get());
            assertThat(newListenerResponse.get(), instanceOf(DelayRecoveryException.class));

            closeShards(primary1, primary2);
        }
    }

    /// Regression test for the race where an active recovery completes during service shutdown and would cause
    /// a new pending recovery to be started after the lifecycle already moved to `State.STOPPED`.
    /// The queue is now drained before the lifecycle state changes.
    public void testCompletingRecoveryWhileStopping() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var task = newRecoveryTask();
        final var block = blockShardRecovery(primary1);

        try (var service = newPeerRecoverySourceService(1)) {
            service.start();

            // Fill slot
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());

            // Queue another recovery
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            // Stop the service and complete listener. Lifecycle assertions in the production code must hold.
            // The pending recovery should never start after the service moved to `State.STOPPED`.
            runInParallel(service::stop, block::close);
            closeShards(primary1, primary2);
        }
    }

    public void testDynamicLimitDecreaseQueuesNewRequests() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3);
        final var clusterSettings = serviceWithSettings.v2();
        final var task = newRecoveryTask();

        try (var service = serviceWithSettings.v1(); var ignored = blockShardRecovery(primary1)) {
            service.start();

            // Decrease the limit before any recoveries start
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1).build()
            );

            // First request starts immediately under the new lower limit.
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(1));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));

            // Second request queues because the new limit (1) is already reached.
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            closeShards(primary1, primary2);
        }
    }

    public void testDynamicLimitDecreaseDoesNotAffectActiveRecoveries() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3);
        final var clusterSettings = serviceWithSettings.v2();
        final var task = newRecoveryTask();

        try (
            var service = serviceWithSettings.v1();
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2), blockShardRecovery(primary3))
        ) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(3));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));

            // Decreasing the limit below the current active count must not cancel or disturb the in-flight recoveries
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1).build()
            );

            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(3));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));

            // New requests queue because active count (3) exceeds the new limit (1)
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            closeShards(primary1, primary2, primary3, primary4);
        }
    }

    public void testDynamicLimitDecreaseDoesNotNotifySchedulingListeners() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3, schedulingListeners);
        final var clusterSettings = serviceWithSettings.v2();
        final var task = newRecoveryTask();

        try (
            var service = serviceWithSettings.v1();
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2), blockShardRecovery(primary3))
        ) {
            service.start();
            // Fill all 3 active slots, then queue primary4
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(3));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            final var dequeuedCount = new AtomicInteger();
            schedulingListeners.addListener(new RecoverySchedulingListener() {
                @Override
                public void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {
                    dequeuedCount.incrementAndGet();
                }
            });

            // Decreasing the limit must not dequeue primary4 even though it is pending
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1).build()
            );

            assertThat(dequeuedCount.get(), equalTo(0));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(1));

            closeShards(primary1, primary2, primary3, primary4);
        }
    }

    public void testDynamicLimitIncreaseWithEmptyQueueDoesNotNotifySchedulingListeners() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2, schedulingListeners);
        final var clusterSettings = serviceWithSettings.v2();
        final var task = newRecoveryTask();

        try (
            var service = serviceWithSettings.v1();
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))
        ) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));

            final var dequeuedCount = new AtomicInteger();
            schedulingListeners.addListener(new RecoverySchedulingListener() {
                @Override
                public void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {
                    dequeuedCount.incrementAndGet();
                }
            });

            // Queue is empty, so raising the limit dequeues nothing and must not fire onRecoveryDequeuedAndStarted
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build()
            );
            assertThat(dequeuedCount.get(), equalTo(0));

            closeShards(primary1, primary2);
        }
    }

    public void testDynamicLimitIncreaseAllowsDirectStart() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2);
        final var clusterSettings = serviceWithSettings.v2();
        final var task = newRecoveryTask();

        try (
            var service = serviceWithSettings.v1();
            var ignored = Releasables.wrap(
                blockShardRecovery(primary1),
                blockShardRecovery(primary2),
                blockShardRecovery(primary3),
                blockShardRecovery(primary4)
            )
        ) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));

            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build()
            );

            // Both new requests must start immediately because active count (2) is below the new higher limit (4)
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(4));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));

            closeShards(primary1, primary2, primary3, primary4);
        }
    }

    public void testDynamicLimitIncreaseDrainsFullQueue() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final IndexShard primary5 = newStartedShard(true);
        // onRecoveryCompleted fires after remove() decrements activeRecoveryHandlerCount, so awaiting
        // all 3 guarantees the active count is stable before we assert.
        final var recoveriesCompleted = new CountDownLatch(3);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                recoveriesCompleted.countDown();
            }
        });
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2, schedulingListeners);
        final var clusterSettings = serviceWithSettings.v2();
        final var task = newRecoveryTask();

        try (
            var service = serviceWithSettings.v1();
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))
        ) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary5), task, primary5, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(3));

            // Raising the limit high enough to fit everything drains the entire queue at once
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 5).build()
            );

            // Wait for all 3 queued recoveries to complete.
            safeAwait(recoveriesCompleted);
            // primary1 and primary2 are blocked waiting for their shard's primary operation permits, so
            // recoverToTarget for those two hangs and onRecoveryComplete never fires. They remain active.
            // For the other 3 dequeued requests, recoverToTarget runs and completes immediately.
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));
            assertTrue(primary3.recoveryStats().noCurrentRecoveries());
            assertTrue(primary4.recoveryStats().noCurrentRecoveries());
            assertTrue(primary5.recoveryStats().noCurrentRecoveries());

            closeShards(primary1, primary2, primary3, primary4, primary5);
        }
    }

    /// When the limit increase opens fewer new slots than items in the queue, the initial drain fills
    /// the available slots immediately; remaining items are pulled out via the cascade as recoveries complete.
    public void testDynamicLimitIncreaseDrainsViaInitialSlotsThenCascade() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final IndexShard primary5 = newStartedShard(true);
        // onRecoveryCompleted fires after remove() decrements activeRecoveryHandlerCount, so awaiting
        // all 3 guarantees the active count is stable before we assert.
        final var recoveriesCompleted = new CountDownLatch(3);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                recoveriesCompleted.countDown();
            }
        });
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2, schedulingListeners);
        final var clusterSettings = serviceWithSettings.v2();

        final var task = newRecoveryTask();

        try (
            var service = serviceWithSettings.v1();
            var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))
        ) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            // Queue 3 items. The limit increase opens only 2 initial slots, so the 3rd drains via cascade.
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary5), task, primary5, ActionListener.noop());
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(3));
            assertThat(primary3.recoveryStats().currentAsSourceQueued(), equalTo(1));
            assertThat(primary4.recoveryStats().currentAsSourceQueued(), equalTo(1));
            assertThat(primary5.recoveryStats().currentAsSourceQueued(), equalTo(1));

            // Raising to 4 opens 2 initial slots; the remaining queued recovery drains via cascade
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build()
            );

            // Wait for all 3 queued recoveries to complete.
            safeAwait(recoveriesCompleted);
            // primary1 and primary2 are blocked waiting for their shard's primary operation permits, so
            // recoverToTarget for those two hangs and onRecoveryComplete never fires. They remain active.
            // For the other 3 dequeued requests, recoverToTarget runs and completes immediately.
            assertThat(service.ongoingRecoveries.activeRecoveryCount(), equalTo(2));
            assertThat(service.ongoingRecoveries.queuedRecoveryCount(), equalTo(0));
            assertTrue(primary3.recoveryStats().noCurrentRecoveries());
            assertTrue(primary4.recoveryStats().noCurrentRecoveries());
            assertTrue(primary5.recoveryStats().noCurrentRecoveries());

            closeShards(primary1, primary2, primary3, primary4, primary5);
        }
    }

    // See: https://github.com/elastic/elasticsearch-team/issues/4353
    public void testStartRecoveriesUpToLimitHandlesSynchronousFailures() throws Exception {
        final var task = newRecoveryTask();
        final var runningShard = newStartedShard(true);
        final var enqueuedShard = newStartedShard(true);
        final int queuedRecoveryCount = 1000;
        final var recoveriesCompleted = new CountDownLatch(queuedRecoveryCount + 1);

        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        schedulingListeners.addListener(new RecoverySchedulingListener() {
            @Override
            public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
                recoveriesCompleted.countDown();
            }
        });

        try (var threadPool = new TestThreadPool("testStartRecoveriesUpToLimitHandlesSynchronousFailures")) {
            final var transportService = mock(TransportService.class);
            when(transportService.getThreadPool()).thenReturn(threadPool);

            try (
                var service = newPeerRecoverySourceService(1, schedulingListeners, transportService);
                var block = blockShardRecovery(runningShard)
            ) {
                service.start();

                service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(runningShard), task, runningShard, ActionListener.noop());
                assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());

                for (int i = 0; i < queuedRecoveryCount; i++) {
                    service.ongoingRecoveries.enqueueRecovery(
                        newStartRecoveryRequest(enqueuedShard),
                        task,
                        enqueuedShard,
                        ActionListener.noop()
                    );
                }

                assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
                assertEquals(queuedRecoveryCount, service.ongoingRecoveries.queuedRecoveryCount());

                // Close queued shard so recoveries fail synchronously in recoverToTarget
                closeShards(enqueuedShard);

                // Trigger cascading failures
                block.close();

                safeAwait(recoveriesCompleted);
                assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
                closeShards(runningShard);
            }
        }
    }

    /// Blocks all new primary operations on `shard` (blocking `recoverToTarget`).
    /// The returned [Releasable] must be closed to unblock operations.
    private static Releasable blockShardRecovery(IndexShard shard) {
        return safeAwait(listener -> shard.acquireAllPrimaryOperationsPermits(listener, TimeValue.MAX_VALUE));
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(int limit) {
        return newPeerRecoverySourceService(
            limit,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            MockUtils.setupTransportServiceWithThreadpoolExecutor()
        );
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(int limit, CompositeRecoverySchedulingListener schedulingListeners) {
        final var settings = Settings.builder()
            .put(NodeRoles.dataNode())
            .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), limit)
            .build();
        return newPeerRecoverySourceService(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            schedulingListeners,
            MockUtils.setupTransportServiceWithThreadpoolExecutor()
        );
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(
        int limit,
        CompositeRecoverySchedulingListener schedulingListeners,
        TransportService transportService
    ) {
        final var settings = Settings.builder()
            .put(NodeRoles.dataNode())
            .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), limit)
            .build();
        return newPeerRecoverySourceService(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            schedulingListeners,
            transportService
        );
    }

    private Tuple<PeerRecoverySourceService, ClusterSettings> newPeerRecoverySourceServiceWithDynamicLimit(int limit) {
        return newPeerRecoverySourceServiceWithDynamicLimit(limit, new CompositeRecoverySchedulingListener());
    }

    private Tuple<PeerRecoverySourceService, ClusterSettings> newPeerRecoverySourceServiceWithDynamicLimit(
        int limit,
        CompositeRecoverySchedulingListener schedulingListeners
    ) {
        final var registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.add(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING);
        final var settings = Settings.builder()
            .put(NodeRoles.dataNode())
            .put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), limit)
            .build();
        final var clusterSettings = new ClusterSettings(settings, registeredSettings);
        return Tuple.tuple(
            newPeerRecoverySourceService(
                settings,
                clusterSettings,
                schedulingListeners,
                MockUtils.setupTransportServiceWithThreadpoolExecutor()
            ),
            clusterSettings
        );
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(
        int limit,
        Set<Setting<?>> registeredSettings,
        TransportService transportService
    ) {
        final var settings = Settings.builder()
            .put(NodeRoles.dataNode())
            .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), limit)
            .build();
        return newPeerRecoverySourceService(
            settings,
            new ClusterSettings(settings, registeredSettings),
            new CompositeRecoverySchedulingListener(),
            transportService
        );
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(
        Settings settings,
        ClusterSettings clusterSettings,
        CompositeRecoverySchedulingListener schedulingListeners,
        TransportService transportService
    ) {
        final var indicesService = mock(IndicesService.class);
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(indicesService.clusterService()).thenReturn(clusterService);
        return new PeerRecoverySourceService(
            transportService,
            indicesService,
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(RecoveryPlannerService.class),
            schedulingListeners
        );
    }

    private StartRecoveryRequest newStartRecoveryRequest(IndexShard shard) {
        return newStartRecoveryRequest(shard, randomAlphaOfLength(10));
    }

    private StartRecoveryRequest newStartRecoveryRequest(IndexShard shard, String allocationId) {
        return new StartRecoveryRequest(
            shard.shardId(),
            allocationId,
            getFakeDiscoNode("source"),
            getFakeDiscoNode("target-" + randomAlphaOfLength(5)),
            0L,
            Store.MetadataSnapshot.EMPTY,
            randomBoolean(),
            randomLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            true
        );
    }

    private Task newRecoveryTask() {
        return new Task(randomNonNegativeLong(), "test", START_RECOVERY, "", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }
}
