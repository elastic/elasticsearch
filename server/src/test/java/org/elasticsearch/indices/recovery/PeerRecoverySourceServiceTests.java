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
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.Actions.START_RECOVERY;
import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.containsString;
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
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(1, primary1.recoveryStats().currentAsSource());
            assertEquals(1, primary2.recoveryStats().currentAsSource());

            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(1, primary3.recoveryStats().currentAsSourceQueued());

            closeShards(primary1, primary2, primary3);
        }
    }

    public void testQueueProcessesNextQueuedWhenSlotFreed() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);

        final var task = newRecoveryTask();
        final var completedListener = new CountDownLatch(1);

        try (var service = newPeerRecoverySourceService(1); var block1 = blockShardRecovery(primary1);) {
            service.start();
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            // The recovery will fail immediately because the fake target allocation ID is not in the shard's routing table
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary2),
                task,
                primary2,
                ActionListener.wrap(r -> fail("unexpected success"), exception -> {
                    assertThat(exception, instanceOf(DelayRecoveryException.class));
                    completedListener.countDown();
                })
            );
            assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

            // Releasing primary1's block lets its recovery fail, which frees a slot and starts primary3.
            block1.close();
            safeAwait(completedListener);

            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(0, primary2.recoveryStats().currentAsSourceQueued());
            closeShards(primary1, primary2);
        }
    }

    public void testQueueFifoOrdering() throws Exception {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final IndexShard primary5 = newStartedShard(true);
        final var task = newRecoveryTask();
        final var allQueued = new CountDownLatch(1);
        final List<Integer> callOrder = new ArrayList<>();

        try (
            var service = newPeerRecoverySourceService(2);
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
                ActionListener.wrap(r -> fail("unexpected success"), e -> callOrder.add(1))
            );
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary4),
                task,
                primary4,
                ActionListener.wrap(r -> fail("unexpected success"), e -> callOrder.add(2))
            );
            service.ongoingRecoveries.enqueueRecovery(
                newStartRecoveryRequest(primary5),
                task,
                primary5,
                ActionListener.wrap(r -> fail("unexpected success"), e -> {
                    callOrder.add(3);
                    allQueued.countDown();
                })
            );
            assertEquals(3, service.ongoingRecoveries.queuedRecoveryCount());

            block1.close();
            safeAwait(allQueued);
            assertEquals(List.of(1, 2, 3), callOrder);

            closeShards(primary1, primary2, primary3, primary4, primary5);
        }
    }

    public void testSameShardFillsMultipleSlots() throws Exception {
        final IndexShard primary = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var service = newPeerRecoverySourceService(2);
        service.start();
        final var task = newRecoveryTask();

        // Two handlers for the same shard each consume one slot.
        try (var ignored = blockShardRecovery(primary)) {
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary), task, primary, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary), task, primary, ActionListener.noop());
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(2, primary.recoveryStats().currentAsSource());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(1, primary2.recoveryStats().currentAsSourceQueued());

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
            assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(2, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(1, primary3.recoveryStats().currentAsSourceQueued());

            service.beforeIndexShardClosed(primary2.shardId(), primary2, Settings.EMPTY);
            assertEquals(0, primary2.recoveryStats().currentAsSourceQueued());
            assertEquals(0, primary2.recoveryStats().currentAsSource());
            assertNotNull(primary2Response.get());
            assertThat(primary2Response.get(), instanceOf(DelayRecoveryException.class));
            assertThat(primary2Response.get().getMessage(), containsString("index shard closed"));

            assertEquals(1, primary3.recoveryStats().currentAsSourceQueued());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

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
            assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

            // Simulate node departure, the pending entry should fail.
            service.ongoingRecoveries.cancelOnNodeLeft(departedNode);
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(0, primary2.recoveryStats().currentAsSourceQueued());
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
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

            service.ongoingRecoveries.cancelAllPendingRecoveries();
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
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

        try (var service = newPeerRecoverySourceService()) {
            service.start();

            // Block so the first recovery stays active long enough to test duplicate rejection.
            try (var ignored = blockShardRecovery(primary)) {
                service.ongoingRecoveries.enqueueRecovery(
                    request,
                    task,
                    primary,
                    ActionListener.wrap(r -> firstComplete.countDown(), e -> firstComplete.countDown())
                );

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

            // Wait for the first recovery to complete naturally after the block is released.
            safeAwait(firstComplete);

            // Re-adding with the same allocation ID succeeds after the previous attempt completes.
            final var secondComplete = new CountDownLatch(1);
            service.ongoingRecoveries.enqueueRecovery(
                request,
                task,
                primary,
                ActionListener.wrap(r -> secondComplete.countDown(), e -> secondComplete.countDown())
            );
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
            assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

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
            var service = newPeerRecoverySourceService();
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
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

            closeShards(primary1, primary2, primary3);
        }
    }

    public void testReestablishActiveRecovery() throws Exception {
        final IndexShard primary = newStartedShard(true);
        final var task = newRecoveryTask();
        final var request = newStartRecoveryRequest(primary);

        try (var service = newPeerRecoverySourceService(); var ignored = blockShardRecovery(primary)) {
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
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(1, primary2.recoveryStats().currentAsSourceQueued());

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

            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(1, primary2.recoveryStats().currentAsSourceQueued());

            assertNull(oldListenerResponse.get());
            assertNull(newListenerResponse.get());

            // Both the old and new listener are notified on completion (here cancellation).
            service.ongoingRecoveries.cancelAllPendingRecoveries();
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(0, primary2.recoveryStats().currentAsSourceQueued());
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
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

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
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

            // Both the old and new listener are notified on completion (here cancellation).
            service.ongoingRecoveries.cancelAllPendingRecoveries();
            assertNotNull("old listener must be called on cancel", oldListenerResponse.get());
            assertThat(oldListenerResponse.get(), instanceOf(DelayRecoveryException.class));
            assertNotNull("new listener must be called on cancel", newListenerResponse.get());
            assertThat(newListenerResponse.get(), instanceOf(DelayRecoveryException.class));

            closeShards(primary1, primary2);
        }
    }

    public void testDynamicLimitDecreaseQueuesNewRequests() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        // Decrease the limit before any recoveries start
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1).build()
        );

        // First request starts immediately under the new lower limit.
        try (var ignored = blockShardRecovery(primary1)) {
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            assertEquals(1, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

            // Second request queues because the new limit (1) is already reached.
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

            closeShards(primary1, primary2);
        }
    }

    public void testDynamicLimitDecreaseDoesNotAffectActiveRecoveries() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        try (var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2), blockShardRecovery(primary3))) {
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            assertEquals(3, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

            // Decreasing the limit below the current active count must not cancel or disturb the in-flight recoveries
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1).build()
            );

            assertEquals(3, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

            // New requests queue because active count (3) exceeds the new limit (1)
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

            closeShards(primary1, primary2, primary3, primary4);
        }
    }

    public void testDynamicLimitDecreaseDoesNotNotifySchedulingListeners() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(3, schedulingListeners);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        try (var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2), blockShardRecovery(primary3))) {
            // Fill all 3 active slots, then queue primary4
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            assertEquals(3, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

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

            assertEquals(0, dequeuedCount.get());
            assertEquals(1, service.ongoingRecoveries.queuedRecoveryCount());

            closeShards(primary1, primary2, primary3, primary4);
        }
    }

    public void testDynamicLimitIncreaseWithEmptyQueueDoesNotNotifySchedulingListeners() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final var schedulingListeners = new CompositeRecoverySchedulingListener();
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2, schedulingListeners);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        try (var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))) {
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

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
            assertEquals(0, dequeuedCount.get());

            closeShards(primary1, primary2);
        }
    }

    public void testDynamicLimitIncreaseAllowsDirectStart() throws IOException {
        final IndexShard primary1 = newStartedShard(true);
        final IndexShard primary2 = newStartedShard(true);
        final IndexShard primary3 = newStartedShard(true);
        final IndexShard primary4 = newStartedShard(true);
        final var serviceWithSettings = newPeerRecoverySourceServiceWithDynamicLimit(2);
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        try (
            var ignored = Releasables.wrap(
                blockShardRecovery(primary1),
                blockShardRecovery(primary2),
                blockShardRecovery(primary3),
                blockShardRecovery(primary4)
            )
        ) {
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build()
            );

            // Both new requests must start immediately because active count (2) is below the new higher limit (4)
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            assertEquals(4, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());

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
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        try (var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))) {
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary5), task, primary5, ActionListener.noop());
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(3, service.ongoingRecoveries.queuedRecoveryCount());

            // Raising the limit high enough to fit everything drains the entire queue at once
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 5).build()
            );

            // Wait for all 3 queued recoveries to complete.
            safeAwait(recoveriesCompleted);
            // primary1 and primary2 are blocked waiting for their shard's primary operation permits, so
            // recoverToTarget for those two hangs and onRecoveryComplete never fires. They remain active.
            // For the other 3 dequeued requests, recoverToTarget runs and completes immediately.
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
            assertEquals(0, primary4.recoveryStats().currentAsSourceQueued());
            assertEquals(0, primary5.recoveryStats().currentAsSourceQueued());

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
        final var service = serviceWithSettings.v1();
        final var clusterSettings = serviceWithSettings.v2();
        service.start();
        final var task = newRecoveryTask();

        try (var ignored = Releasables.wrap(blockShardRecovery(primary1), blockShardRecovery(primary2))) {
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary1), task, primary1, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary2), task, primary2, ActionListener.noop());
            // Queue 3 items. The limit increase opens only 2 initial slots, so the 3rd drains via cascade.
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary3), task, primary3, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary4), task, primary4, ActionListener.noop());
            service.ongoingRecoveries.enqueueRecovery(newStartRecoveryRequest(primary5), task, primary5, ActionListener.noop());
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(3, service.ongoingRecoveries.queuedRecoveryCount());

            // Raising to 4 opens 2 initial slots; the remaining queued recovery drains via cascade
            clusterSettings.applySettings(
                Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 4).build()
            );

            // Wait for all 3 queued recoveries to complete.
            safeAwait(recoveriesCompleted);
            // primary1 and primary2 are blocked waiting for their shard's primary operation permits, so
            // recoverToTarget for those two hangs and onRecoveryComplete never fires. They remain active.
            // For the other 3 dequeued requests, recoverToTarget runs and completes immediately.
            assertEquals(2, service.ongoingRecoveries.activeRecoveryCount());
            assertEquals(0, service.ongoingRecoveries.queuedRecoveryCount());
            assertEquals(0, primary3.recoveryStats().currentAsSourceQueued());
            assertEquals(0, primary4.recoveryStats().currentAsSourceQueued());
            assertEquals(0, primary5.recoveryStats().currentAsSourceQueued());

            closeShards(primary1, primary2, primary3, primary4, primary5);
        }
    }

    /// Blocks all new primary operations on `shard` so that `recoverToTarget` does not fail
    /// synchronously. The returned [Releasable] must be closed to unblock operations.
    private static Releasable blockShardRecovery(IndexShard shard) {
        return safeAwait(listener -> shard.acquireAllPrimaryOperationsPermits(listener, TimeValue.MAX_VALUE));
    }

    private PeerRecoverySourceService newPeerRecoverySourceService() {
        return newPeerRecoverySourceService(Integer.MAX_VALUE);
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(int limit) {
        return newPeerRecoverySourceService(limit, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
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
        return Tuple.tuple(newPeerRecoverySourceService(settings, clusterSettings, schedulingListeners), clusterSettings);
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(int limit, Set<Setting<?>> registeredSettings) {
        final var settings = Settings.builder()
            .put(NodeRoles.dataNode())
            .put(PeerRecoverySourceService.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), limit)
            .build();
        return newPeerRecoverySourceService(
            settings,
            new ClusterSettings(settings, registeredSettings),
            new CompositeRecoverySchedulingListener()
        );
    }

    private PeerRecoverySourceService newPeerRecoverySourceService(
        Settings settings,
        ClusterSettings clusterSettings,
        CompositeRecoverySchedulingListener schedulingListeners
    ) {
        final var indicesService = mock(IndicesService.class);
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(indicesService.clusterService()).thenReturn(clusterService);
        final TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
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
